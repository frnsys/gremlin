use std::collections::BTreeMap;

use iocraft::{element, ElementExt};

use crate::{core::Unit, data::display::FacetedVarTables};

use super::{
    profile::{profile, VarProfile},
    Facet, RefForFacet,
};

pub trait Row {
    /// Describe the columns for this type of row.
    fn columns() -> Vec<String>;

    /// The values for this row.
    /// They should align with the columns from `columns`.
    fn values(&self) -> Vec<f32>;
}

impl Row for () {
    fn columns() -> Vec<String> {
        vec![]
    }

    fn values(&self) -> Vec<f32> {
        vec![]
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

pub trait AsRowValue {
    fn as_f32(&self) -> f32;
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
impl<U: Unit> AsRowValue for Option<U> {
    fn as_f32(&self) -> f32 {
        self.as_ref().map_or(f32::NAN, |val| val.value())
    }
}

/// A `Vec<Row>` is enough to constitute a `Dataset`.
impl<R: Row> Dataset for Vec<R> {
    type Row = R;
    type Facet = String;
    fn rows(&self) -> impl Iterator<Item = &R> {
        self.iter()
    }
}

#[macro_export]
macro_rules! non_dataset {
    ($t:ty) => {
        impl Dataset for $t {
            // TODO make distinction in gremlin b/w dataset tributaries
            // and compute/transform tributaries, which don't need to
            // implement dataset
            type Row = ();
            type Facet = String;
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
    type Facet: Facet + 'static;

    fn rows(&self) -> impl Iterator<Item = &Self::Row>;

    /// Optionally return the rows grouped in some way,
    /// for displaying separate tables for each group.
    fn faceted(&self) -> BTreeMap<Self::Facet, Vec<&Self::Row>> {
        BTreeMap::default()
    }

    /// Optionally return references to compare this dataset to.
    fn refs() -> Vec<Box<dyn RefForFacet<Self::Facet>>> {
        vec![]
    }

    /// Print table describing the rows in this dataset.
    fn inspect(&self) -> String {
        let cols = Self::Row::columns();
        let mut facet_profiles = BTreeMap::default();

        let refs = Self::refs();
        let rows: Vec<_> = self.rows().collect();
        facet_profiles.insert(
            None,
            DataProfile {
                total: rows.len(),
                refs: refs
                    .iter()
                    // TODO get rid of clones
                    .map(|r| (r.source().to_string(), r.aggregate().clone()))
                    .collect(),
                profiles: build_var_profile(rows.into_iter(), &cols),
            },
        );

        for (facet, rows) in self.faceted() {
            facet_profiles.insert(
                Some(facet.to_string()),
                DataProfile {
                    total: rows.len(),
                    refs: refs
                        .iter()
                        // TODO get rid of clones
                        .flat_map(|r| {
                            r.for_facet(&facet)
                                .into_iter()
                                .map(|(matched_facet, refs)| {
                                    (format!("{}:{}", r.source(), matched_facet), refs.clone())
                                })
                        })
                        .collect(),
                    profiles: build_var_profile(rows.into_iter(), &cols),
                },
            );
        }

        let name = std::any::type_name::<Self::Row>()
            .split("::")
            .last()
            .unwrap();
        element!(FacetedVarTables(name, profiles: facet_profiles)).to_string()
    }
}

#[derive(Default, Clone, Debug)]
pub struct DataProfile {
    /// Total number of records.
    pub total: usize,

    /// Profiles for each variable.
    pub profiles: BTreeMap<String, VarProfile>,

    pub refs: BTreeMap<String, BTreeMap<String, f32>>,
}

fn build_var_profile<'a, R: Row + 'a>(
    rows: impl Iterator<Item = &'a R>,
    columns: &[String],
) -> BTreeMap<String, VarProfile> {
    let mut col_values: BTreeMap<String, Vec<f32>> = BTreeMap::default();
    for row in rows {
        for (col, val) in columns.iter().zip(row.values().iter()) {
            let vals = col_values.entry(col.clone()).or_default();
            vals.push(*val);
        }
    }
    col_values
        .into_iter()
        .map(|(col, vals)| {
            let profile = profile(vals.into_iter());
            (col, profile)
        })
        .collect()
}
