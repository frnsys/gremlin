use anyhow::Error as AnyhowError;
use iocraft::prelude::*;
use itertools::Itertools;
use std::{boxed::Box as RBox, collections::BTreeMap, fmt::Debug};
use thiserror::Error;

use crate::data::profile::Count;

use super::{
    partial::FromPartial,
    profile::{profile, VarProfile},
};

pub trait Row {
    /// Describe the columns for this type of row.
    fn columns() -> &'static [&'static str];

    /// The values for this row.
    /// They should align with the columns from `columns`.
    fn values(&self) -> Vec<f32>;
}

pub trait Dataset {
    type Row: Row;

    fn rows(&self) -> &[Self::Row];

    /// Optionally return reference values for the rows' fields.
    /// The expected structure is `{ source name: { field name: reference value } }`.
    fn refs(&self) -> BTreeMap<&'static str, BTreeMap<&'static str, f32>> {
        BTreeMap::default()
    }

    /// Optionally return the rows grouped in some way,
    /// for displaying separate tables for each group.
    fn grouped(&self) -> Option<BTreeMap<String, Vec<&Self::Row>>> {
        None
    }

    /// Print table describing the rows in this dataset.
    fn inspect(&self) {
        let mut col_values: BTreeMap<&str, Vec<f32>> = BTreeMap::default();
        let cols = Self::Row::columns();
        let rows = self.rows();
        let total = rows.len();
        for row in rows {
            for (col, val) in cols.iter().zip(row.values().iter()) {
                let vals = col_values.entry(col).or_default();
                vals.push(*val);
            }
        }
        let profiles: BTreeMap<&str, VarProfile> = col_values
            .into_iter()
            .map(|(col, vals)| {
                let profile = profile(vals.into_iter());
                (col, profile)
            })
            .collect();

        let refs = self.refs();

        let name = std::any::type_name::<Self::Row>()
            .split("::")
            .last()
            .unwrap();
        element!(VarTable(name, total, refs, vars: profiles)).print();
    }
}

/// A `Vec<Row>` is enough to constitute a `Dataset`.
impl<R: Row> Dataset for Vec<R> {
    type Row = R;
    fn rows(&self) -> &[R] {
        &self
    }
}

/// Erase the associated `Row` type in the `Dataset` trait.
trait DatasetErased {
    fn inspect(&self);
}
impl<R: Row, T: Dataset<Row = R>> DatasetErased for T {
    fn inspect(&self) {
        Dataset::inspect(self)
    }
}

/// Describe & validate any constraints on fields for this type.
pub trait Constrained {
    fn validate(&self) -> Result<(), Vec<InvalidValue>>;
}

/// Describe an invalid value.
#[derive(Debug)]
pub struct InvalidValue {
    /// The field that is invalid.
    field: String,

    /// The provided value.
    value: String,

    /// Describe the violated constraint.
    constraint: String,
}

/// Define the river's strictness.
///
/// * _Invalid_ items are those that fail their constraints,
///   as defined by the [`Constrained`] implementation.
/// * _Incomplete_ items are those that haven't been fully hydrated,
///   i.e. the [`Partial`] has missing values.
#[derive(Default)]
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

/// A `River` is a pipeline for producing a set of items of type `T`.
pub struct River<T: FromPartial + Constrained + Row>
where
    T::Partial: Row,
{
    /// The [`Source`] produces the initial partial versions of `T`.
    pub source: RBox<dyn Source<T>>,

    /// These define additional data sources that can fill in or overwrite
    /// additional fields in the partial versions of `T`.
    /// Note that the sequence here is important, as it's the order each
    /// [`Tributary`] is applied in, so later ones can override earlier ones.
    pub tributaries: Vec<RBox<dyn Tributary<T>>>,

    /// Define the river's behavior when encountering incomplete or invalid items.
    pub strictness: Strictness,
}
impl<T: FromPartial + Constrained + Row> River<T>
where
    T::Partial: Row,
{
    pub fn run(&self, verbose: bool) -> Result<Vec<T>, HydrateError> {
        // Generate initial items.
        if verbose {
            self.source.inspect();
        }
        let mut items = self.source.generate()?;
        if verbose {
            Dataset::inspect(&items);
        }

        // Fill in (hydrate) items.
        for tributary in &self.tributaries {
            if verbose {
                self.source.inspect();
            }
            tributary.fill(&mut items)?;
            if verbose {
                Dataset::inspect(&items);
            }
        }

        let total = items.len();
        let items = items.into_iter().map(|partial| T::from(partial));

        // Check for incomplete items.
        let ignore_incomplete = matches!(
            self.strictness,
            Strictness::IgnoreAll | Strictness::IgnoreIncomplete
        );
        let (items, incomplete): (Vec<_>, Vec<_>) = items.partition_result();

        if !incomplete.is_empty() && verbose {
            let mut error_counts: BTreeMap<String, usize> = BTreeMap::default();
            for err in incomplete.iter() {
                if let HydrateError::EmptyFields(_, fields) = err {
                    for field in fields {
                        let count = error_counts.entry(field.to_string()).or_default();
                        *count += 1;
                    }
                }
            }
            element!(ErrorTable(name: "Incomplete".to_string(), total, error_counts)).print();
        }

        if ignore_incomplete {
            tracing::warn!("{} incomplete items, ignoring.", incomplete.len());
        } else {
            tracing::error!("{} incomplete items.", incomplete.len());
            return Err(HydrateError::Many(incomplete));
        }

        // Check for invalid items.
        let ignore_invalid = matches!(
            self.strictness,
            Strictness::IgnoreAll | Strictness::IgnoreInvalid
        );
        let items = items.into_iter().map(|item| match item.validate() {
            Ok(_) => Ok(item),
            Err(errs) => Err(HydrateError::InvalidValues(errs)),
        });
        let (items, invalid): (Vec<_>, Vec<_>) = items.partition_result();

        if !invalid.is_empty() && verbose {
            let mut error_counts: BTreeMap<String, usize> = BTreeMap::default();
            for err in invalid.iter() {
                if let HydrateError::InvalidValues(values) = err {
                    for value in values {
                        let count = error_counts.entry(value.field.to_string()).or_default();
                        *count += 1;
                    }
                }
            }
            element!(ErrorTable(name: "Invalid".to_string(), total, error_counts)).print();
        }
        if ignore_invalid {
            tracing::warn!("{} invalid items, ignoring.", invalid.len());
        } else {
            tracing::error!("{} invalid items.", invalid.len());
            return Err(HydrateError::Many(invalid));
        }

        if verbose {
            Dataset::inspect(&items);
        }

        Ok(items)
    }
}

/// A `Source` produces the initial population of partial `T`s.
/// See [`River`].
pub trait Source<T>: Debug + DatasetErased
where
    T: FromPartial,
{
    fn generate(&self) -> Result<Vec<T::Partial>, HydrateError>;
}

/// A `Tributary` fills in fields in the population of partial `T`s.
/// See [`River`].
pub trait Tributary<T>: Debug + DatasetErased
where
    T: FromPartial,
{
    fn fill(&self, items: &mut [T::Partial]) -> Result<(), HydrateError>;
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

    #[error(transparent)]
    Other(#[from] AnyhowError),
}

#[derive(Default, Props)]
struct ErrorTableProps {
    name: String,
    total: usize,
    error_counts: BTreeMap<String, usize>,
}

#[component]
fn ErrorTable<'a>(props: &ErrorTableProps) -> impl Into<AnyElement<'a>> {
    element! {
        Box(
            flex_direction: FlexDirection::Column,
            margin_top: 1,
            margin_bottom: 1,
            border_style: BorderStyle::Single,
            border_color: Color::Black,
        ) {
            Box(
                flex_direction: FlexDirection::Row,
                width: Size::Auto,
                justify_content: JustifyContent::SpaceBetween,
                column_gap: Gap::Length(2),
                margin_bottom: 1,
                margin_left: 1,
                margin_right: 1,
            ) {
                Text(content: &props.name, weight: Weight::Bold)
                Text(content: format!("{} rows", props.total))
            }
            Box(
                flex_direction: FlexDirection::Row,
                width: Size::Auto,
            ) {
                Box(flex_direction: FlexDirection::Column, align_items: AlignItems::End, padding_left: 1, padding_right: 1) {
                    Text(content: "Field", weight: Weight::Bold)
                    #(props.error_counts.keys().map(|err| element! {
                        Text(content: err)
                    }))
                }

                Box(flex_direction: FlexDirection::Column, padding_right: 1, align_items: AlignItems::End) {
                    Text(content: "Count", weight: Weight::Bold)
                    #(props.error_counts.values().map(|count| element! {
                        Text(content: count.to_string())
                    }))
                }

                Box(flex_direction: FlexDirection::Column, padding_right: 1, align_items: AlignItems::End) {
                    Text(content: "Percent", weight: Weight::Bold)
                    #(props.error_counts.values().map(|count| element! {
                        Text(content: Count::new(*count, props.total).display_percent())
                    }))
                }
            }
        }
    }
}

#[derive(Default, Props)]
struct VarTableProps {
    name: &'static str,
    total: usize,
    vars: BTreeMap<&'static str, VarProfile>,
    refs: BTreeMap<&'static str, BTreeMap<&'static str, f32>>,
}

#[component]
fn VarTable<'a>(props: &VarTableProps) -> impl Into<AnyElement<'a>> {
    element! {
        Box(
            flex_direction: FlexDirection::Column,
            margin_top: 1,
            margin_bottom: 1,
            border_style: BorderStyle::Single,
            border_color: Color::Black,
        ) {
            Box(
                flex_direction: FlexDirection::Row,
                width: Size::Auto,
                justify_content: JustifyContent::SpaceBetween,
                column_gap: Gap::Length(2),
                margin_bottom: 1,
                margin_left: 1,
                margin_right: 1,
            ) {
                Text(content: props.name, weight: Weight::Bold)
                Text(content: format!("{} rows", props.total))
            }
            Box(
                flex_direction: FlexDirection::Row,
                width: Size::Auto,
            ) {
                Box(flex_direction: FlexDirection::Column, align_items: AlignItems::End, padding_left: 1, padding_right: 1) {
                    Text(content: "Var", weight: Weight::Bold)
                    Text(content: "Missing", weight: Weight::Bold)
                    Text(content: "Distinct", weight: Weight::Bold)
                    Text(content: "Duplicate", weight: Weight::Bold)
                    Text(content: "Zero", weight: Weight::Bold)
                    Text(content: "Negative", weight: Weight::Bold)
                    Text(content: "Infinite", weight: Weight::Bold)
                    Text(content: "Outliers", weight: Weight::Bold)
                    Text(content: "Mean", weight: Weight::Bold)
                    Text(content: "Median", weight: Weight::Bold)
                    Text(content: "Std Dev", weight: Weight::Bold)
                    Text(content: "Min", weight: Weight::Bold)
                    Text(content: "Max", weight: Weight::Bold)
                    Text(content: "Range", weight: Weight::Bold)
                    Text(content: "Dist", weight: Weight::Bold)
                    Text(content: "------", color: Color::Black)
                    #(props.refs.keys().map(|source| element! {
                        Text(content: *source)
                    }))
                }

                #(props.vars.iter().map(|(var, prof)| element! {
                    Box(flex_direction: FlexDirection::Column, padding_right: 1, align_items: AlignItems::End) {
                        Text(content: *var, decoration: TextDecoration::Underline)
                        Text(content: format!("{}", prof.missing.count), color: if prof.missing.all() {
                            Color::Red
                        } else {
                            Color::Reset
                        })
                        Text(content: format!("{}", prof.distinct.count))
                        Text(content: format!("{}", prof.duplicate.count))
                        Text(content: format!("{}", prof.zero.count))
                        Text(content: format!("{}", prof.negative.count))
                        Text(content: format!("{}", prof.infinite.count))
                        Text(content: format!("{}", prof.summary.outliers.count))
                        Text(content: format!("{:.3}", prof.summary.mean))
                        Text(content: format!("{:.3}", prof.summary.median))
                        Text(content: format!("{:.3}", prof.summary.std_dev))
                        Text(content: format!("{:.3}", prof.summary.min))
                        Text(content: format!("{:.3}", prof.summary.max))
                        Text(content: format!("{:.3}", prof.summary.range))
                        Text(content: &prof.summary.histogram, color: Color::DarkGrey)
                        Text(content: "------", color: Color::Black)
                        #(props.refs.values().map(|refs| {
                            refs.get(var).map_or_else(|| {
                                element! {
                                    Text(content: "-----", color: Color::Black)
                                }
                            }, |val| {
                                element! {
                                    Text(content: format!("{:.3}", val))
                                }
                            })
                        }))
                    }
                }))
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use gremlin_macros::Partial;

    use super::*;

    #[test]
    fn test_river() {
        #[derive(Default, Partial)]
        struct Target {
            field_a: f32,
            field_b: f32,
            field_c: f32,
        }
        impl Constrained for Target {
            fn validate(&self) -> Result<(), Vec<InvalidValue>> {
                if self.field_a < 0.0 {
                    return Err(vec![InvalidValue {
                        field: "field_a".into(),
                        value: self.field_a.to_string(),
                        constraint: "Must be gte 0.".into(),
                    }]);
                }
                Ok(())
            }
        }
        // TODO derive Row
        impl Row for Target {
            fn values(&self) -> Vec<f32> {
                vec![self.field_a, self.field_b, self.field_c]
            }
            fn columns() -> &'static [&'static str] {
                &["field_a", "field_b", "field_c"]
            }
        }
        impl Row for PartialTarget {
            fn values(&self) -> Vec<f32> {
                vec![
                    self.field_a.unwrap_or(f32::NAN),
                    self.field_b.unwrap_or(f32::NAN),
                    self.field_c.unwrap_or(f32::NAN),
                ]
            }
            fn columns() -> &'static [&'static str] {
                &["field_a", "field_b", "field_c"]
            }
        }

        #[derive(Debug, Clone)]
        struct XRow {
            field_a: f32,
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
        impl Row for XRow {
            fn values(&self) -> Vec<f32> {
                vec![self.field_a, self.field_b]
            }
            fn columns() -> &'static [&'static str] {
                &["field_a", "field_b"]
            }
        }

        #[derive(Debug, Clone)]
        struct YRow {
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
        impl Row for YRow {
            fn values(&self) -> Vec<f32> {
                vec![self.field_c]
            }
            fn columns() -> &'static [&'static str] {
                &["field_c"]
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
            source: RBox::new(x_set.clone()),
            tributaries: vec![],
            strictness: Strictness::Strict,
        };
        let res = river.run(true);
        assert!(res.is_err());

        // Succeeds, but no results as `field_c` is not filled.
        let river = River {
            source: RBox::new(x_set.clone()),
            tributaries: vec![],
            strictness: Strictness::IgnoreAll,
        };
        let res = river.run(true).unwrap();
        assert!(res.is_empty());

        // Fails, as we have an invalid `field_a` value.
        let river = River {
            source: RBox::new(x_set.clone()),
            tributaries: vec![RBox::new(y_set.clone())],
            strictness: Strictness::Strict,
        };
        let res = river.run(true);
        assert!(res.is_err());

        // Succeeds, but only results in one item, as the other has an invalid `field_a` value.
        let river = River {
            source: RBox::new(x_set.clone()),
            tributaries: vec![RBox::new(y_set.clone())],
            strictness: Strictness::IgnoreAll,
        };
        let res = river.run(true).unwrap();
        assert_eq!(res.len(), 1);

        // Set to valid value.
        x_set[1].field_a = 5.;
        let river = River {
            source: RBox::new(x_set),
            tributaries: vec![RBox::new(y_set)],
            strictness: Strictness::IgnoreAll,
        };
        let res = river.run(true).unwrap();
        assert_eq!(res.len(), 2);
    }
}
