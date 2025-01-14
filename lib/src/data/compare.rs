//! Utilities for comparing datasets.
//!
//! Implement the [`Comparable`] trait for a [`Row`],
//! with some type `V`, which should be an enum
//! representing the set of variables that can be compared.
//!
//! Then you can build a comparison table by passing in
//! a `HashMap<Key, Rows>`, where each [`Key`] is a `(String, String)`
//! representing the data source and facet.

use std::path::Path;

use super::profile::VarProfile;
use ahash::HashMap;
use serde::{Deserialize, Serialize};
use strum::IntoEnumIterator;

pub trait Comparable<V: IntoEnumIterator + std::fmt::Display> {
    fn var_value(&self, var: &V) -> Option<f32>;
}

// (Source, Facet)
pub type Key = (String, String);
pub type Rows<V> = Vec<Box<dyn Comparable<V>>>;

pub fn build_comparison_table<V: IntoEnumIterator + std::fmt::Display>(
    group: String,
    data: HashMap<Key, Rows<V>>,
) -> Vec<CompareRow> {
    let mut results = vec![];
    for ((source, facet), rows) in data {
        for var in V::iter() {
            let values = rows.iter().map(|v| v.var_value(&var).unwrap_or(f32::NAN));
            let profile = super::profile::profile(values);
            results.push(CompareRow {
                group: group.clone(),
                source: source.clone(),
                facet: facet.clone(),
                variable: var.to_string(),
                profile,
            });
        }
    }
    results
}

serde_with::with_prefix!(profile "profile.");

#[derive(Clone, Serialize, Deserialize, Debug, Default)]
pub struct CompareRow {
    pub group: String,
    pub source: String,
    pub facet: String,
    pub variable: String,

    #[serde(flatten, with = "profile")]
    pub profile: VarProfile,
}

pub fn write_comparisons_to_csv(dir: &Path, prefix: String, name: String, rows: Vec<CompareRow>) {
    let fname = format!("{prefix}.{}.csvs", name);
    let fpath = dir.join(fname);
    let file = fs_err::OpenOptions::new()
        .write(true)
        .truncate(true)
        .create(true)
        .open(fpath)
        .unwrap();
    let mut wtr = csv::Writer::from_writer(file);

    let mut by_facet: HashMap<String, Vec<CompareRow>> = HashMap::default();
    for row in rows {
        // TODO facet vs group
        // println!("{:?}", row.group);
        let group = by_facet.entry(row.group.clone()).or_default();
        group.push(row);
    }

    let cols = ["source", "mean", "count", "min", "max", "median", "hist"];
    let n_cols = cols.len();

    for (facet, rows) in by_facet {
        let mut by_var: HashMap<String, Vec<CompareRow>> = HashMap::default();
        for row in rows {
            let group = by_var.entry(row.variable.clone()).or_default();
            group.push(row);
        }

        for (var, rows) in by_var {
            if rows.is_empty() {
                continue;
            }

            wtr.write_record(cols).unwrap();

            for row in rows {
                let csv_row = [
                    row.source,
                    format!("{:.2}", row.profile.summary.mean),
                    row.profile.distinct.count.to_string(),
                    format!("{:.2}", row.profile.summary.min),
                    format!("{:.2}", row.profile.summary.max),
                    format!("{:.2}", row.profile.summary.median),
                    row.profile.summary.histogram,
                ];
                wtr.write_record(csv_row).unwrap();
            }

            let mut csv_divider = vec!["#".to_string(); n_cols];
            csv_divider[0] = format!("#>{}:{}", facet, var);
            wtr.write_record(csv_divider).unwrap();
        }
    }
    wtr.flush().unwrap();
}
