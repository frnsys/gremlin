use anyhow::Error as AnyhowError;
use fs_err::File;
use iocraft::prelude::*;
use itertools::Itertools;
use std::{boxed::Box as RBox, collections::BTreeMap, fmt::Debug, io::Write, path::Path};
use thiserror::Error;

use crate::{core::Unit, data::profile::Count};

use super::{
    partial::FromPartial,
    profile::{profile, VarProfile},
};

pub trait Row {
    /// Describe the columns for this type of row.
    fn columns() -> Vec<String>;

    /// The values for this row.
    /// They should align with the columns from `columns`.
    fn values(&self) -> Vec<f32>;
}

pub trait AsRowValue {
    fn as_f32(&self) -> f32;
}

pub struct EmptyRow;
impl Row for EmptyRow {
    fn columns() -> Vec<String> {
        vec![]
    }

    fn values(&self) -> Vec<f32> {
        vec![]
    }
}
#[macro_export]
macro_rules! non_dataset {
    ($t:ty) => {
        impl Dataset for $t {
            // TODO make distinction in gremlin b/w dataset tributaries
            // and compute/transform tributaries, which don't need to
            // implement dataset
            type Row = EmptyRow;
            fn rows(&self) -> impl Iterator<Item = &Self::Row> {
                std::iter::empty()
            }
            fn inspect(&self) -> String {
                String::new()
            }
        }
    };
}

pub trait Dataset {
    type Row: Row;

    fn rows(&self) -> impl Iterator<Item = &Self::Row>;

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
    fn inspect(&self) -> String {
        let mut col_values: BTreeMap<String, Vec<f32>> = BTreeMap::default();
        let cols = Self::Row::columns();
        let rows: Vec<_> = self.rows().collect();
        let total = rows.len();
        for row in rows {
            for (col, val) in cols.iter().zip(row.values().iter()) {
                let vals = col_values.entry(col.clone()).or_default();
                vals.push(*val);
            }
        }
        let profiles: BTreeMap<String, VarProfile> = col_values
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
        element!(VarTable(name, total, refs, vars: profiles)).to_string()
    }
}

/// A `Vec<Row>` is enough to constitute a `Dataset`.
impl<R: Row> Dataset for Vec<R> {
    type Row = R;
    fn rows(&self) -> impl Iterator<Item = &R> {
        self.iter()
    }
}

/// Erase the associated `Row` type in the `Dataset` trait.
trait DatasetErased {
    fn inspect(&self) -> String;
}
impl<R: Row, T: Dataset<Row = R>> DatasetErased for T {
    fn inspect(&self) -> String {
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

/// A `River` is a pipeline for producing a set of items of type `T`.
#[derive(Debug)]
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
    pub fn run(&self, log_path: Option<&Path>) -> Result<Vec<T>, HydrateError> {
        let mut buffer = Vec::new();

        // Generate initial items.
        writeln!(buffer, "{}", self.source.inspect());
        let mut items = self.source.generate()?;
        writeln!(buffer, "{}", Dataset::inspect(&items));

        // Fill in (hydrate) items.
        for tributary in &self.tributaries {
            writeln!(buffer, "{}", tributary.inspect());
            tributary.fill(&mut items)?;
            writeln!(buffer, "{}", Dataset::inspect(&items));
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
                element!(ErrorTable(name: "Incomplete".to_string(), total, error_counts))
                    .to_string();
            writeln!(buffer, "{}", incomplete_table);

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
        let items = items.into_iter().map(|item| match item.validate() {
            Ok(_) => Ok(item),
            Err(errs) => Err(HydrateError::InvalidValues(errs)),
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
                element!(ErrorTable(name: "Invalid".to_string(), total, error_counts)).to_string();
            writeln!(buffer, "{}", error_table);

            if ignore_invalid {
                tracing::warn!("{} invalid items, ignoring.", invalid.len());
            } else {
                tracing::error!("{} invalid items.", invalid.len());
                return Err(HydrateError::Many(invalid));
            }
        }

        writeln!(buffer, "{}", Dataset::inspect(&items));

        if let Some(path) = log_path {
            let mut file = File::create(path)?;
            file.write_all(&buffer)?;
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

    #[error("IO error: {0:?}")]
    IO(#[from] std::io::Error),

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
    vars: BTreeMap<String, VarProfile>,
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
                        Text(content: var, decoration: TextDecoration::Underline)
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
                            refs.get(var.as_str()).map_or_else(|| {
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

impl<T> Row for T
where
    T: AsRowValue + Copy,
{
    fn columns() -> Vec<String> {
        vec!["".into()]
    }

    fn values(&self) -> Vec<f32> {
        vec![self.as_f32()]
    }
}

// Can't do this as it would conflict
// with the *possibility* of `From<f32>`
// being implemented in the future for `Option<f32>`.
// See <https://github.com/rust-lang/rfcs/issues/2758>
// impl<T> AsRowValue for T
// where
//     T: Copy,
//     f32: From<T>,
// {
//     fn as_f32(&self) -> f32 {
//         f32::from(*self)
//     }
// }
impl AsRowValue for f32 {
    fn as_f32(&self) -> f32 {
        *self
    }
}
impl AsRowValue for Option<f32> {
    fn as_f32(&self) -> f32 {
        self.unwrap_or(f32::NAN)
    }
}
impl AsRowValue for Option<u16> {
    fn as_f32(&self) -> f32 {
        self.map_or(f32::NAN, |val| val as f32)
    }
}
impl<U: Unit> AsRowValue for U {
    fn as_f32(&self) -> f32 {
        self.value()
    }
}

/// Macro for conveniently defining a [`Constraint`].
// #[macro_export]
// macro_rules! constraint {
//     (>$val: expr) => {
//         $crate::draw::Constraint::LowerBound($val)
//     };
//     (>=$val: expr) => {
//         $crate::draw::Constraint::LowerBoundInclusive($val)
//     };
//     (<$val: expr) => {
//         $crate::draw::Constraint::UpperBound($val)
//     };
//     (<=$val: expr) => {
//         $crate::draw::Constraint::UpperBoundInclusive($val)
//     };
//     ($val_a: expr => $val_b: expr) => {
//         $crate::draw::Constraint::RangeInclusive($val_a, $val_b)
//     }; // ($val_a: expr => $val_b: expr) => {
//        //     $crate::draw::Constraint::Range($val_a, $val_b)
//        // };
// }

#[cfg(test)]
mod tests {
    use gremlin_macros::{Partial, Row};

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
            source: RBox::new(x_set.clone()),
            tributaries: vec![],
            strictness: Strictness::Strict,
        };
        let res = river.run(None);
        assert!(res.is_err());

        // Succeeds, but no results as `field_c` is not filled.
        let river = River {
            source: RBox::new(x_set.clone()),
            tributaries: vec![],
            strictness: Strictness::IgnoreAll,
        };
        let res = river.run(None).unwrap();
        assert!(res.is_empty());

        // Fails, as we have an invalid `field_a` value.
        let river = River {
            source: RBox::new(x_set.clone()),
            tributaries: vec![RBox::new(y_set.clone())],
            strictness: Strictness::Strict,
        };
        let res = river.run(None);
        assert!(res.is_err());

        // Succeeds, but only results in one item, as the other has an invalid `field_a` value.
        let river = River {
            source: RBox::new(x_set.clone()),
            tributaries: vec![RBox::new(y_set.clone())],
            strictness: Strictness::IgnoreAll,
        };
        let res = river.run(None).unwrap();
        assert_eq!(res.len(), 1);

        // Set to valid value.
        x_set[1].field_a = 5.;
        let river = River {
            source: RBox::new(x_set),
            tributaries: vec![RBox::new(y_set)],
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
