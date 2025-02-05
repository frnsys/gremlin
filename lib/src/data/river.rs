//! [`River`]s are data refinement pipelines meant to produce a collection
//! of some type `T` at the end.
//!
//! A `River` is composed of a [`Source`], which produces the initial population
//! of `T`, and then zero or more [`Tributary`]s, which fill in or modify fields.
//!
//! In fact, a `River` works with _partials_ of `T`, which is a mirror of `T` where
//! all fields are wrapped as `Option`s. Each step of the River is meant to fill
//! in some of these `Option`s, with the expectation that all fields are filled
//! by the end. Any instances with remaining `None` fields are considered _incomplete_
//! and can either be ignored or cause the entire pipeline to fail.
//!
//! `T` is also expected to implement [`Constrained`], which is used to check for any
//! _invalid_ instances, which can again either be ignored or cause the entire pipeline
//! to fail.
//!
//! To better understand what's happening in the pipeline there are outputs to describe the
//! input and output data. These are handled by two traits: [`Dataset`] and [`Row`].
//!
//! [`Dataset`] basically describes a collection of [`Row`]s, and a `Row` should describe the
//! fields of a struct in a table-style format; i.e. with column names and values (currently
//! fixed to `f32`). In its simplest form a `Dataset` is just a `Vec<Row>`, but they can
//! additionally implement reference values for each column and output grouped tables.
//!
//! [`Imperfect`] is meant to describe what can go wrong with a particular dataset, e.g.
//! miscoded values, and provides a way to log these problems. These are not intended
//! to be hard failures but rather warnings about potential data quality issues to help identify
//! where problematic values may be coming from. For hard failures instead look to [`Constrained`].

use itertools::Itertools;
use std::{backtrace::Backtrace, collections::BTreeMap, fmt::Debug, path::Path};
use thiserror::Error;

pub use super::report::{report_css, report_js, rows_html, Diff};
use super::{
    constrain::{Breach, Constrained},
    partial::{FromPartial, Partial},
    profile::VarProfile,
    report::RiverReport,
    DataProfile, Dataset, Row,
};

/// Define the river's strictness.
///
/// * _Invalid_ items are those that fail their constraints,
///   as defined by the [`Constrained`] implementation.
/// * _Incomplete_ items are those that haven't been fully hydrated,
///   i.e. the [`Partial`] has missing values.
#[derive(Debug, Default, Clone, Copy)]
pub enum Strictness {
    /// Produce an error if any invalid or incomplete items are found.
    #[default]
    Strict,

    /// Skip any invalid or incomplete items.
    IgnoreAll,

    /// Only ignore invalid items.
    IgnoreInvalid,

    /// Only ignore incomplete items.
    IgnoreIncomplete,
}
impl std::fmt::Display for Strictness {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", match self {
            Self::Strict => "Strict",
            Self::IgnoreAll => "IgnoreAll: Invalid or incomplete items are skipped.",
            Self::IgnoreInvalid => "IgnoreInvalid: Invalid items are skipped; incomplete items have missing values imputed.",
            Self::IgnoreIncomplete => "IgnoreIncomplete: Incomplete items are skipped; invalid items are kept.",
        })
    }
}

macro_rules! profile {
    ($dir:ident, $step:ident, $name:expr, $profiles:expr) => {
        $dir.as_ref().map(|path| {
            let prefix = format!("{:0>2}", $step);
            write_profiles_to_csv(path, prefix, $name, $profiles);
            $step += 1;
        });
    };
}

/// A `River` is a pipeline for producing a set of items of type `T`.
#[allow(private_bounds)]
#[derive(Debug)]
pub struct River<T: FromPartial + Constrained + Row>
where
    T::Partial: Row,
{
    /// A name used to identify this river.
    pub name: String,

    /// The [`Source`] produces the initial partial versions of `T`.
    pub source: Box<dyn Source<T>>,

    /// These define additional data sources that can fill in or overwrite
    /// additional fields in the partial versions of `T`.
    /// Note that the sequence here is important, as it's the order each
    /// [`Tributary`] is applied in, so later ones can override earlier ones.
    pub tributaries: Vec<Box<dyn Tributary<T>>>,

    /// Define the river's behavior when encountering incomplete or invalid items.
    pub strictness: Strictness,

    pub references: VarReferences,
}

type BySourceFacet<T> = BTreeMap<Option<String>, T>;
pub type BySource<T> = BTreeMap<String, T>;
pub type ByFacet<T> = BTreeMap<Option<String>, T>;
pub type ByField<T> = BTreeMap<String, T>;
pub type ByConstraint<T> = BTreeMap<String, T>;
pub type VarReferences = BySource<ByFacet<BySourceFacet<ByField<f32>>>>;
pub type FacetVarReferences = BySource<BySourceFacet<ByField<f32>>>;

#[derive(Debug, Clone)]
pub struct StepResult {
    pub num: usize,
    pub name: String,
    pub inputs: ByFacet<ByField<VarProfile>>,
    pub outputs: Diff<ByFacet<ByField<VarProfile>>>,
    pub invalid: ByField<ByConstraint<Diff<isize>>>,
    pub incomplete: Diff<isize>,
}
impl StepResult {
    pub fn new(
        num: usize,
        name: String,
        inputs: ByFacet<ByField<VarProfile>>,
        outputs: Diff<ByFacet<ByField<VarProfile>>>,
        invalid: Diff<Vec<Breach>>,
        incomplete: Diff<isize>,
    ) -> Self {
        let invalid = invalid.by_constraint();

        StepResult {
            num,
            name,
            inputs,
            outputs,
            incomplete,
            invalid,
        }
    }
}

impl<T: FromPartial + Constrained + Row + Debug> River<T>
where
    T::Partial: Row + Constrained,
{
    pub fn run(&self, log_dir: Option<&Path>) -> Result<(Vec<T>, RiverReport), HydrateError> {
        let mut inputs = vec![self.source.name()];
        inputs.extend(self.tributaries.iter().map(|t| t.name()));
        let mut report = RiverReport {
            name: self.name.clone(),
            strictness: self.strictness,
            tributaries: inputs,
            references: self.references.clone(),
            ..Default::default()
        };

        let mut step = 0;

        if let Some(dir) = &log_dir {
            if !dir.exists() {
                fs_err::create_dir_all(dir)?;
            }
        }

        // Generate initial items.
        let mut items = self.source.generate()?;

        let mut incomplete: Diff<isize> = Diff::default();
        incomplete.current = items
            .iter()
            .filter(|item| !item.missing_fields().is_empty())
            .count() as isize;
        let mut invalid: Diff<Vec<Breach>> =
            Diff::new(items.iter().flat_map(|item| item.validate()).collect());
        let mut var_profiles = Diff::new(items.var_profiles());

        report.play_by_play.push(StepResult::new(
            step,
            self.source.name(),
            self.source.inspect_vars(),
            var_profiles.clone(),
            invalid.clone(),
            incomplete.clone(),
        ));

        profile!(log_dir, step, Dataset::name(&items), items.profile());

        // Fill in (hydrate) items.
        for tributary in self.tributaries.iter() {
            step += 1;

            tributary.fill(&mut items)?;
            profile!(log_dir, step, Dataset::name(&items), items.profile());

            incomplete.update(
                items
                    .iter()
                    .filter(|item| !item.missing_fields().is_empty())
                    .count() as isize,
            );
            invalid.update(items.iter().flat_map(|item| item.validate()).collect());

            var_profiles.update(items.var_profiles());
            report.play_by_play.push(StepResult::new(
                step,
                tributary.name(),
                tributary.inspect_vars(),
                var_profiles.clone(),
                invalid.clone(),
                incomplete.clone(),
            ));
        }

        let items = items.into_iter().map(|partial| T::from(partial));

        // Check for incomplete items.
        let ignore_incomplete = matches!(
            self.strictness,
            Strictness::IgnoreAll | Strictness::IgnoreIncomplete
        );
        let (items, incomplete): (Vec<_>, Vec<_>) = items.partition_result();

        if !incomplete.is_empty() {
            let mut error_counts: BTreeMap<String, isize> = BTreeMap::default();
            for err in incomplete.iter() {
                if let HydrateError::EmptyFields(_, fields) = err {
                    for field in fields {
                        let count = error_counts.entry(field.to_string()).or_default();
                        *count += 1;
                    }
                }
            }

            if ignore_incomplete {
                tracing::warn!("{} incomplete items, ignoring.", incomplete.len());
                tracing::warn!("Missing fields: {:#?}", error_counts);
            } else {
                tracing::error!("{} incomplete items.", incomplete.len());
                tracing::error!("Missing fields: {:#?}", error_counts);
                return Err(HydrateError::Many(incomplete));
            }
            report.n_incomplete = incomplete.len();
        }

        // Check for invalid items.
        let ignore_invalid = matches!(
            self.strictness,
            Strictness::IgnoreAll | Strictness::IgnoreInvalid
        );
        let items = items.into_iter().map(|item| {
            let errs = item.validate();
            println!("{:?}", item);
            if errs.is_empty() {
                Ok(item)
            } else {
                Err(HydrateError::InvalidValues(errs))
            }
        });
        let (items, invalid): (Vec<_>, Vec<_>) = items.partition_result();

        if !invalid.is_empty() {
            let mut error_counts: BTreeMap<String, isize> = BTreeMap::default();
            for err in invalid.iter() {
                if let HydrateError::InvalidValues(values) = err {
                    for value in values {
                        let count = error_counts.entry(value.field.to_string()).or_default();
                        *count += 1;
                    }
                }
            }

            if ignore_invalid {
                tracing::warn!("{} invalid items, ignoring.", invalid.len());
            } else {
                tracing::error!("{} invalid items.", invalid.len());
                return Err(HydrateError::Many(invalid));
            }
            report.n_invalid = invalid.len();
        }

        profile!(log_dir, step, Dataset::name(&items), items.profile());

        Ok((items, report))
    }
}

fn write_profiles_to_csv(
    dir: &Path,
    prefix: String,
    name: String,
    profiles: BTreeMap<Option<String>, DataProfile>,
) {
    let fname = format!("{prefix}.{}.csvs", name);
    let fpath = dir.join(fname);
    let file = fs_err::OpenOptions::new()
        .write(true)
        .truncate(true)
        .create(true)
        .open(fpath)
        .unwrap();
    let mut wtr = csv::Writer::from_writer(file);
    for (facet, profile) in profiles.iter() {
        let records = profile.as_csv();
        if records.is_empty() {
            continue;
        }

        let n_cols = records[0].len();
        let facet = facet.as_ref().map_or("All", |f| f.as_str());
        let mut csv_divider = vec!["#".to_string(); n_cols];
        csv_divider[0] = format!("#>{}", facet);
        wtr.write_record(csv_divider).unwrap();
        for row in records {
            wtr.write_record(row).unwrap();
        }
    }
    wtr.flush().unwrap();
}

/// Any step in the data refinement pipeline,
/// e.g. a [`Source`] or a [`Tributary`], needs to
/// implement this trait.
///
/// However you do not need to implement this trait
/// directly; instead you can implement [`Dataset`].
pub trait DataStep {
    fn name(&self) -> String {
        std::any::type_name::<Self>().to_string()
    }

    fn inspect_vars(&self) -> ByFacet<ByField<VarProfile>> {
        Default::default()
    }
}
impl<T: Dataset> DataStep for T {
    fn name(&self) -> String {
        Dataset::name(self)
    }

    fn inspect_vars(&self) -> ByFacet<ByField<VarProfile>> {
        Dataset::var_profiles(self)
    }
}

/// A `Tributary` fills in fields in the population of partial `T`s.
/// See [`River`].
pub trait Tributary<T: FromPartial>: Debug + DataStep {
    fn fill(&self, items: &mut [T::Partial]) -> Result<(), HydrateError>;
}

/// A `Source` produces the initial population of partial `T`s.
/// See [`River`].
pub trait Source<T: FromPartial>: Debug + DataStep {
    fn generate(&self) -> Result<Vec<T::Partial>, HydrateError>;
}

/// Dummy source, if a placeholder is needed.
impl<T: FromPartial> Source<T> for () {
    fn generate(&self) -> Result<Vec<<T as FromPartial>::Partial>, HydrateError> {
        Ok(vec![])
    }
}
impl DataStep for () {
    fn name(&self) -> String {
        "(never)".to_string()
    }
}
impl<T: FromPartial + Constrained + Row> Default for River<T>
where
    T::Partial: Row,
{
    fn default() -> Self {
        Self {
            name: "(river-stub)".into(),
            source: Box::new(()),
            tributaries: vec![],
            strictness: Strictness::IgnoreAll,
            references: Default::default(),
        }
    }
}

#[derive(Debug, Error)]
pub enum HydrateError {
    #[error("The following fields for {0} were empty: {1:?}.\n  You may need to add a tributary to fill the field(s).")]
    EmptyFields(&'static str, Vec<&'static str>),

    #[error("The following field for {name} is required to fill other fields: {field}")]
    MissingExpectedField {
        name: &'static str,
        field: &'static str,
        backtrace: Backtrace,
    },

    #[error("An item had invalid values: {0:?}")]
    InvalidValues(Vec<Breach>),

    #[error("There were multiple errors: {0:?}")]
    Many(Vec<HydrateError>),

    #[error("IO error: {0:?}")]
    IO(#[from] std::io::Error),
}

#[cfg(test)]
mod tests {
    use gremlin_macros::{Constrained, Partial, Row};

    use crate::data::AsRowValue;

    use super::*;

    #[test]
    fn test_river() {
        #[derive(Debug, Default, Partial, Constrained, Row)]
        #[partial(row, constrained)]
        struct Target {
            #[row]
            #[constraint(GreaterThan = 0.)]
            field_a: f32,

            #[row]
            field_b: f32,

            #[row]
            field_c: f32,
        }

        #[derive(Debug, Clone, Row)]
        struct XRow {
            #[row]
            field_a: f32,

            #[row]
            field_b: f32,
        }
        impl Source<Target> for Vec<XRow> {
            fn generate(&self) -> Result<Vec<PartialTarget>, HydrateError> {
                let items = self.iter().map(|row| PartialTarget {
                    field_a: Some(row.field_a),
                    field_b: Some(row.field_b),
                    field_c: None,
                });
                Ok(items.collect())
            }
        }

        #[derive(Debug, Clone, Row)]
        struct YRow {
            #[row]
            field_c: f32,
        }
        impl Tributary<Target> for Vec<YRow> {
            fn fill(&self, items: &mut [PartialTarget]) -> Result<(), HydrateError> {
                for (i, item) in items.iter_mut().enumerate() {
                    item.field_c = Some(self[i].field_c);
                }
                Ok(())
            }
        }

        let mut x_set = vec![
            XRow {
                field_a: 2.,
                field_b: 4.,
            },
            XRow {
                field_a: -2.,
                field_b: 8.,
            },
        ];

        let y_set = vec![YRow { field_c: 10. }, YRow { field_c: 20. }];

        // Fail strict as `field_c` is not filled.
        let river = River {
            source: Box::new(x_set.clone()),
            tributaries: vec![],
            strictness: Strictness::Strict,
            ..Default::default()
        };
        let res = river.run(None);
        assert!(res.is_err());

        // Succeeds, but no results as `field_c` is not filled.
        let river = River {
            source: Box::new(x_set.clone()),
            tributaries: vec![],
            strictness: Strictness::IgnoreAll,
            ..Default::default()
        };
        let (res, _) = river.run(None).unwrap();
        assert!(res.is_empty());

        // Fails, as we have an invalid `field_a` value.
        let river = River {
            source: Box::new(x_set.clone()),
            tributaries: vec![Box::new(y_set.clone())],
            strictness: Strictness::Strict,
            ..Default::default()
        };
        let res = river.run(None);
        assert!(res.is_err());

        // Succeeds, but only results in one item, as the other has an invalid `field_a` value.
        let river = River {
            source: Box::new(x_set.clone()),
            tributaries: vec![Box::new(y_set.clone())],
            strictness: Strictness::IgnoreAll,
            ..Default::default()
        };
        let (res, _) = river.run(None).unwrap();
        assert_eq!(res.len(), 1);

        // Set to valid value.
        x_set[1].field_a = 5.;
        let river = River {
            source: Box::new(x_set),
            tributaries: vec![Box::new(y_set)],
            strictness: Strictness::IgnoreAll,
            ..Default::default()
        };
        let (res, _) = river.run(None).unwrap();
        assert_eq!(res.len(), 2);
    }

    #[test]
    fn test_derive_row() {
        #[derive(Row)]
        struct Foo {
            #[row]
            field_a: f32,
            #[row]
            field_b: SomeUnit,
            field_c: String,
            #[row]
            field_d: SomeNestedType,
        }

        #[derive(Row)]
        struct SomeNestedType {
            #[row]
            subfield_a: f32,
            #[row]
            subfield_b: f32,
        }

        #[derive(Clone, Copy)]
        struct SomeUnit;
        impl AsRowValue for SomeUnit {
            fn as_f32(&self) -> f32 {
                1.0
            }
        }

        let foo = Foo {
            field_a: 42.0,
            field_b: SomeUnit,
            field_c: "hello".to_string(),
            field_d: SomeNestedType {
                subfield_a: 3.14,
                subfield_b: 2.71,
            },
        };

        assert_eq!(
            Foo::columns(),
            vec![
                "field_a",
                "field_b",
                "field_d.subfield_a",
                "field_d.subfield_b"
            ]
        );
        assert_eq!(foo.values(), vec![42., 1., 3.14, 2.71]);
    }
}
