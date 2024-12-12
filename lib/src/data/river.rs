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
//! input data, output data, and custom warnings. These are handled by three traits: [`Dataset`],
//! [`Row`], and [`Imperfect`].
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

use anyhow::Error as AnyhowError;
use fs_err::File;
use iocraft::{element, ElementExt};
use itertools::Itertools;
use std::{collections::BTreeMap, fmt::Debug, io::Write, path::Path};
use thiserror::Error;

use crate::data::display::CountTable;

use super::{
    constrain::{Constrained, InvalidValue},
    partial::FromPartial,
    DataProfile, Dataset, Imperfect, Row,
};

/// Define the river's strictness.
///
/// * _Invalid_ items are those that fail their constraints,
///   as defined by the [`Constrained`] implementation.
/// * _Incomplete_ items are those that haven't been fully hydrated,
///   i.e. the [`Partial`] has missing values.
#[derive(Debug, Default)]
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
    /// The [`Source`] produces the initial partial versions of `T`.
    pub source: Box<dyn Source<T>>,

    /// These define additional data sources that can fill in or overwrite
    /// additional fields in the partial versions of `T`.
    /// Note that the sequence here is important, as it's the order each
    /// [`Tributary`] is applied in, so later ones can override earlier ones.
    pub tributaries: Vec<Box<dyn Tributary<T>>>,

    /// Define the river's behavior when encountering incomplete or invalid items.
    pub strictness: Strictness,
}
impl<T: FromPartial + Constrained + Row> River<T>
where
    T::Partial: Row,
{
    pub fn run(&self, log_dir: Option<&Path>) -> Result<Vec<T>, HydrateError> {
        let mut step = 0;

        let mut buffer = Vec::new();

        if let Some(dir) = &log_dir {
            if !dir.exists() {
                fs_err::create_dir_all(dir)?;
            }
        }

        // Generate initial items.
        profile!(
            log_dir,
            step,
            self.source.name(),
            self.source.inspect_data()
        );
        let mut items = self.source.generate()?;
        writeln!(buffer, "{}", self.source.inspect_logs())?;

        profile!(log_dir, step, Dataset::name(&items), items.profile());

        // Fill in (hydrate) items.
        for tributary in &self.tributaries {
            profile!(log_dir, step, tributary.name(), tributary.inspect_data());
            tributary.fill(&mut items)?;
            profile!(log_dir, step, Dataset::name(&items), items.profile());
            writeln!(buffer, "{}", tributary.inspect_logs())?;
            // writeln!(buffer, "{}", T::collect_logs())?; // TODO
        }

        let total = items.len();
        let items = items.into_iter().map(|partial| T::from(partial));

        // Check for incomplete items.
        let ignore_incomplete = matches!(
            self.strictness,
            Strictness::IgnoreAll | Strictness::IgnoreIncomplete
        );
        let (items, incomplete): (Vec<_>, Vec<_>) = items.partition_result();

        if !incomplete.is_empty() {
            let mut error_counts: BTreeMap<String, usize> = BTreeMap::default();
            for err in incomplete.iter() {
                if let HydrateError::EmptyFields(_, fields) = err {
                    for field in fields {
                        let count = error_counts.entry(field.to_string()).or_default();
                        *count += 1;
                    }
                }
            }
            let incomplete_table =
                element!(CountTable(name: "Incomplete".to_string(), total: Some(total), counts: error_counts))
                    .to_string();
            writeln!(buffer, "{}", incomplete_table)?;

            if ignore_incomplete {
                tracing::warn!("{} incomplete items, ignoring.", incomplete.len());
            } else {
                tracing::error!("{} incomplete items.", incomplete.len());
                return Err(HydrateError::Many(incomplete));
            }
        }

        // Check for invalid items.
        let ignore_invalid = matches!(
            self.strictness,
            Strictness::IgnoreAll | Strictness::IgnoreInvalid
        );
        let items = items.into_iter().map(|item| {
            let errs = item.validate();
            if errs.is_empty() {
                Ok(item)
            } else {
                Err(HydrateError::InvalidValues(errs))
            }
        });
        let (items, invalid): (Vec<_>, Vec<_>) = items.partition_result();

        if !invalid.is_empty() {
            let mut error_counts: BTreeMap<String, usize> = BTreeMap::default();
            for err in invalid.iter() {
                if let HydrateError::InvalidValues(values) = err {
                    for value in values {
                        let count = error_counts.entry(value.field.to_string()).or_default();
                        *count += 1;
                    }
                }
            }
            let error_table =
                element!(CountTable(name: "Invalid".to_string(), total: Some(total), counts: error_counts)).to_string();
            writeln!(buffer, "{}", error_table)?;

            if ignore_invalid {
                tracing::warn!("{} invalid items, ignoring.", invalid.len());
            } else {
                tracing::error!("{} invalid items.", invalid.len());
                return Err(HydrateError::Many(invalid));
            }
        }

        profile!(log_dir, step, Dataset::name(&items), items.profile());

        // if let Some(path) = log_path {
        //     let mut file = File::create(path)?;
        //     file.write_all(&buffer)?;
        // }

        Ok(items)
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
/// directly; instead you should implement [`Dataset`]
/// and [`Imperfect`]
trait DataStep {
    fn name(&self) -> String;
    fn inspect_data(&self) -> BTreeMap<Option<String>, DataProfile>;
    fn inspect_logs(&self) -> String;
}
impl<T: Dataset + Imperfect> DataStep for T
where
    for<'a> &'static str: From<&'a <T as Imperfect>::Warning>,
{
    fn name(&self) -> String {
        Dataset::name(self)
    }

    fn inspect_data(&self) -> BTreeMap<Option<String>, DataProfile> {
        Dataset::profile(self)
    }

    fn inspect_logs(&self) -> String {
        let counted: BTreeMap<String, usize> = Self::collect_logs()
            .into_iter()
            .map(|(warn, warnings)| (warn.to_string(), warnings.len()))
            .collect();

        if counted.is_empty() {
            String::new()
        } else {
            let name = std::any::type_name::<T>().split("::").last().unwrap();
            element!(CountTable(name, counts: counted)).to_string()
        }
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

#[derive(Debug, Error)]
pub enum HydrateError {
    #[error("The following fields for {0} were empty: {1:?}.\n  You may need to add a tributary to fill the field(s).")]
    EmptyFields(&'static str, Vec<&'static str>),

    #[error("The following field for {0} is required to fill other fields: {1}")]
    MissingExpectedField(&'static str, &'static str),

    #[error("An item had invalid values: {0:?}")]
    InvalidValues(Vec<InvalidValue>),

    #[error("There were multiple errors: {0:?}")]
    Many(Vec<HydrateError>),

    #[error("IO error: {0:?}")]
    IO(#[from] std::io::Error),

    #[error(transparent)]
    Other(#[from] AnyhowError),
}

#[cfg(test)]
mod tests {
    use gremlin_macros::{Partial, Row};

    use crate::data::AsRowValue;

    use super::*;

    #[test]
    fn test_river() {
        #[derive(Default, Partial, Row)]
        #[partial(row)]
        struct Target {
            #[row]
            field_a: f32,

            #[row]
            field_b: f32,

            #[row]
            field_c: f32,
        }
        impl Constrained for Target {
            fn id(&self) -> String {
                String::new()
            }

            fn validate(&self) -> Vec<InvalidValue> {
                if self.field_a < 0.0 {
                    return vec![InvalidValue {
                        field: "field_a".into(),
                        value: self.field_a.to_string(),
                        constraint: "Must be gte 0.".into(),
                    }];
                }
                vec![]
            }
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
        };
        let res = river.run(None);
        assert!(res.is_err());

        // Succeeds, but no results as `field_c` is not filled.
        let river = River {
            source: Box::new(x_set.clone()),
            tributaries: vec![],
            strictness: Strictness::IgnoreAll,
        };
        let res = river.run(None).unwrap();
        assert!(res.is_empty());

        // Fails, as we have an invalid `field_a` value.
        let river = River {
            source: Box::new(x_set.clone()),
            tributaries: vec![Box::new(y_set.clone())],
            strictness: Strictness::Strict,
        };
        let res = river.run(None);
        assert!(res.is_err());

        // Succeeds, but only results in one item, as the other has an invalid `field_a` value.
        let river = River {
            source: Box::new(x_set.clone()),
            tributaries: vec![Box::new(y_set.clone())],
            strictness: Strictness::IgnoreAll,
        };
        let res = river.run(None).unwrap();
        assert_eq!(res.len(), 1);

        // Set to valid value.
        x_set[1].field_a = 5.;
        let river = River {
            source: Box::new(x_set),
            tributaries: vec![Box::new(y_set)],
            strictness: Strictness::IgnoreAll,
        };
        let res = river.run(None).unwrap();
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
