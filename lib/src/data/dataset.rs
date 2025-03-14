use std::collections::BTreeMap;

use crate::core::{Percent, Unit};

use super::{
    profile::{profile, VarProfile},
    ByFacet, ByField, Facet, Rows,
};

pub trait Row {
    /// Describe the columns for this type of row.
    fn columns() -> Vec<String>;

    /// The values for this row.
    /// They should align with the columns from `columns`.
    fn values(&self) -> Vec<f32>;

    /// The facet for this row.
    fn facet(&self) -> String;
}

impl Row for () {
    fn columns() -> Vec<String> {
        vec![]
    }

    fn values(&self) -> Vec<f32> {
        vec![]
    }

    fn facet(&self) -> String {
        String::new()
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

    fn facet(&self) -> String {
        String::new()
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
impl AsRowValue for Percent {
    fn as_f32(&self) -> f32 {
        self.value()
    }
}

/// A `Vec<Row>` is enough to constitute a `Dataset`.
impl<R: Row> Dataset for Vec<R> {
    type Row = R;
    type Facet = String;

    fn name(&self) -> String {
        std::any::type_name::<Self::Row>()
            .split("::")
            .last()
            .unwrap()
            .to_string()
    }

    fn rows(&self) -> impl Iterator<Item = &R> {
        self.iter()
    }

    fn faceted(&self) -> BTreeMap<Self::Facet, Vec<&Self::Row>> {
        let hm = self.rows().group_by(|row| row.facet());
        hm.into_iter().collect()
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
            fn name(&self) -> String {
                std::any::type_name::<$t>()
                    .split("::")
                    .last()
                    .unwrap()
                    .to_string()
            }
            fn rows(&self) -> impl Iterator<Item = &Self::Row> {
                std::iter::empty()
            }
            fn profile(
                &self,
            ) -> std::collections::BTreeMap<Option<String>, $crate::data::DataProfile> {
                std::collections::BTreeMap::default()
            }
        }
    };
}

#[macro_export]
macro_rules! dataset {
    ($name:ident, $row:ty) => {
        dataset!($name, $row, String, |_row: &$row| { String::new() });
    };

    ($name:ident, $row:ty, $facet:ty, $facet_prop:expr) => {
        #[derive(Debug, gremlin::Dataset)]
        #[dataset(row = $row, facet = $facet)]
        pub struct $name {
            #[dataset(rows)]
            rows: Vec<$row>,
        }
        impl $name {
            pub fn load() -> Self {
                Self {
                    rows: <$row>::load_rows(),
                }
            }

            fn get_row_facet(&self, row: &$row) -> $facet {
                $facet_prop(row)
            }
        }
    };
}

pub trait Dataset {
    type Row: Row;
    type Facet: Facet + 'static;

    fn name(&self) -> String {
        std::any::type_name::<Self>()
            .split("::")
            .last()
            .unwrap()
            .to_string()
    }

    fn rows(&self) -> impl Iterator<Item = &Self::Row>;

    /// Optionally return the rows grouped in some way,
    /// for displaying separate tables for each group.
    fn faceted(&self) -> BTreeMap<Self::Facet, Vec<&Self::Row>> {
        BTreeMap::default()
    }

    /// Return profiles describing the rows in this dataset, by facet.
    fn profile(&self) -> BTreeMap<Option<String>, DataProfile> {
        let cols = Self::Row::columns();
        let mut facet_profiles = BTreeMap::default();

        let rows: Vec<_> = self.rows().collect();
        facet_profiles.insert(
            None,
            DataProfile {
                total: rows.len(),
                profiles: build_var_profile(rows, &cols),
            },
        );

        for (facet, rows) in self.faceted() {
            facet_profiles.insert(
                Some(facet.to_string()),
                DataProfile {
                    total: rows.len(),
                    profiles: build_var_profile(rows, &cols),
                },
            );
        }

        facet_profiles
    }

    fn var_profiles(&self) -> ByFacet<ByField<VarProfile>> {
        self.profile()
            .into_iter()
            .map(|(facet, profile)| (facet, profile.profiles))
            .collect()
    }
}

#[derive(Default, Clone, Debug)]
pub struct DataProfile {
    /// Total number of records.
    pub total: usize,

    /// Profiles for each variable.
    pub profiles: BTreeMap<String, VarProfile>,
}
impl DataProfile {
    pub fn as_csv(&self) -> Vec<Vec<String>> {
        let metrics = &[
            "",
            "Missing",
            "Distinct",
            "Duplicate",
            "Zero",
            "Negative",
            "Infinite",
            "Outliers",
            "Mean",
            "Median",
            "Std Dev",
            "Min",
            "Max",
            "Range",
            "Dist",
            "------",
        ];
        let mut rows = vec![];
        let mut headers = vec!["".to_string()];
        for (var, prof) in &self.profiles {
            headers.push(var.to_string());
            rows.push(vec![
                format!("{}", prof.missing.count),
                format!("{}", prof.distinct.count),
                format!("{}", prof.duplicate.count),
                format!("{}", prof.zero.count),
                format!("{}", prof.negative.count),
                format!("{}", prof.infinite.count),
                format!("{}", prof.summary.outliers.count),
                format!("{:.3}", prof.summary.mean),
                format!("{:.3}", prof.summary.median),
                format!("{:.3}", prof.summary.std_dev),
                format!("{:.3}", prof.summary.min),
                format!("{:.3}", prof.summary.max),
                format!("{:.3}", prof.summary.range),
                prof.summary.histogram.clone(),
                "------".to_string(),
            ]);
        }

        // TODO  references

        // Transpose the rows.
        let n_cols = metrics.len() - 1;
        let n_rows = rows.len();
        let mut rows: Vec<Vec<_>> = (0..n_cols)
            .map(|col| (0..n_rows).map(|row| rows[row][col].clone()).collect())
            .collect();

        for (i, row) in rows.iter_mut().enumerate() {
            row.insert(0, metrics[i + 1].to_string());
        }
        let mut records = vec![headers];
        records.extend(rows);

        records
    }
}

fn build_var_profile<'a, R: Row + 'a>(
    rows: Vec<&'a R>,
    columns: &[String],
) -> BTreeMap<String, VarProfile> {
    let n = rows.len();
    let mut col_values: BTreeMap<&str, Vec<f32>> = BTreeMap::default();
    for row in rows {
        for (col, val) in columns.iter().zip(row.values().iter()) {
            let vals = col_values
                .entry(col.as_str())
                .or_insert_with(|| Vec::with_capacity(n));
            vals.push(*val);
        }
    }
    col_values
        .into_iter()
        .map(|(col, vals)| {
            let profile = profile(vals.into_iter());
            (col.to_string(), profile)
        })
        .collect()
}
