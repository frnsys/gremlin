//! File system helpers, including
//! serializing/deserializing from common data formats.

mod path;

use std::{
    fs::{File, OpenOptions},
    path::{Path, PathBuf},
};

use anyhow::Context;
use itertools::multizip;
use serde::{de::DeserializeOwned, Serialize};
use serde_json::{json, Value};

pub use path::*;
use thiserror::Error;

pub fn read_csv<T: DeserializeOwned, P: AsRef<Path>>(
    path: P,
) -> impl Iterator<Item = T> {
    let path = path.as_ref();
    let mut reader = csv::Reader::from_path(path)
        .with_context(|| path.display().to_string())
        .unwrap();
    let headers = reader.headers().unwrap().clone();
    let src = path.display().to_string();
    reader.into_records().map(move |rec| {
        let rec = rec.unwrap();
        rec.deserialize(Some(&headers))
            .with_context(|| format!("Source: {src}\n{rec:?}"))
            .unwrap()
    })
}

/// Read raw rows from a CSV, specifying the column names.
/// The column order doesn't need to match what's in the CSV;
/// they will automatically be rearranged to match.
pub fn read_rows<const N: usize>(
    path: &Path,
    columns: &[&str; N],
) -> anyhow::Result<Vec<[String; N]>> {
    let mut rdr = csv::Reader::from_path(path)?;
    let headers = rdr.headers().expect("Should have headers");
    let idxs: [usize; N] = columns.map(|col| {
        headers
            .iter()
            .position(|field| field == col)
            .unwrap_or_else(|| {
                panic!(r#"Couldn't find column "{}""#, col)
            })
    });
    let mut results = vec![];
    for result in rdr.records() {
        let record = result?;
        let row: [String; N] = idxs.map(|idx| record[idx].to_string());
        results.push(row);
    }
    Ok(results)
}

/// A hacky way to deserialize a type from a CSV where its
/// nested structs are flattened.
///
/// Per [this issue](https://github.com/BurntSushi/rust-csv/issues/239)
/// this is not something the `csv` crate will support.
///
/// The way this works is:
///
/// - We first serialize a default copy of the object to a JSON
///     object so we learn the flattened column names and types.
///     This is why `T` must implement both `Serialize` and `Default`.
/// - We read the CSV and check that the columns look correct.
/// - Then we parse each CSV record according to what we learned
///     from the JSON-serialized example.
///
/// For this to work you must flatten fields that are structs and
/// define a prefix:
///
/// ```
/// serde_with::with_prefix!(my_prefix "my_prefix.");
///
/// struct MyStruct {
///     #[serde(flatten, with = "my_prefix")]
///     nested: Nested,
/// }
/// ```
pub fn read_flat_csv<
    T: Serialize + DeserializeOwned + Default,
    P: AsRef<Path> + std::fmt::Debug,
>(
    path: P,
) -> Result<Vec<T>, CsvError> {
    tracing::debug!("Deserializing CSV: {:#?}", path);
    let ref_val = T::default();
    let json_val = serde_json::to_value(&ref_val)?;
    let json_obj = json_val
        .as_object()
        .expect("We gave an object so we get one back");
    let ref_cols: Vec<&String> = json_obj.keys().collect();
    let path = path.as_ref();

    let mut reader = csv::Reader::from_path(path).map_err(|_err| {
        CsvError::FailedToOpenFile(path.to_path_buf())
    })?;

    let cols: Vec<String> = reader
        .headers()
        .map_err(|_err| CsvError::FailedToReadHeaders)?
        .iter()
        .map(|col| col.to_string())
        .collect();

    // Some basic validation
    let missing: Vec<_> = ref_cols
        .iter()
        .filter(|col| !cols.contains(col))
        .map(|col| col.to_string())
        .collect();
    if !missing.is_empty() {
        return Err(CsvError::ColumnsMismatch(
            path.display().to_string(),
            missing,
        ));
    }

    let idxs: Vec<usize> = ref_cols
        .iter()
        .filter_map(|col| cols.iter().position(|c| c == *col))
        .collect();

    let mut data = vec![];
    for rec in reader.records() {
        let rec = rec.map_err(|_err| CsvError::FailedToReadRecord)?;
        let mut map = serde_json::Map::new();
        for (idx, typ) in multizip((idxs.iter(), json_obj.values())) {
            let val = &rec[*idx];
            let col = &cols[*idx];
            match typ {
                Value::Bool(_) => {
                    map.insert(
                        col.to_string(),
                        json!(val.parse::<bool>()?),
                    );
                }
                Value::Number(num) => {
                    if num.is_f64() {
                        map.insert(
                            col.to_string(),
                            json!(val.parse::<f32>()?),
                        );
                    } else if num.is_i64() {
                        map.insert(
                            col.to_string(),
                            json!(val.parse::<i32>()?),
                        );
                    } else if num.is_u64() {
                        map.insert(
                            col.to_string(),
                            json!(val.parse::<u32>()?),
                        );
                    }
                }
                Value::String(_) => {
                    map.insert(col.to_string(), json!(val));
                }
                _ => {
                    return Err(CsvError::UnhandledType(
                        col.clone(),
                        typ.clone(),
                    ));
                }
            }
        }
        let obj: T = serde_json::from_value(Value::Object(map))?;
        data.push(obj);
    }
    Ok(data)
}

/// A hacky way to serialize a type to a flattened CSV representation.
/// See [`read_flat_csv`] for more details.
pub fn write_flat_csv<
    T: Serialize + Default,
    P: AsRef<Path> + std::fmt::Debug,
>(
    path: P,
    items: &[T],
) -> Result<(), CsvError> {
    tracing::debug!("Serializing CSV: {:#?}", path);
    let mut w =
        csv::Writer::from_path(path.as_ref()).map_err(|_err| {
            CsvError::FailedToOpenFile(path.as_ref().to_path_buf())
        })?;

    let ref_val = T::default();
    let json_val = serde_json::to_value(&ref_val)?;
    let json_obj = json_val
        .as_object()
        .expect("We gave an object so we get one back");
    let ref_cols: Vec<&String> = json_obj.keys().collect();
    w.write_record(ref_cols)?;

    for item in items {
        let json_val = serde_json::to_value(item)?;
        let obj = json_val
            .as_object()
            .expect("We gave an object so we get one back");
        let vals: Vec<String> = obj
            .values()
            .map(|v| {
                // A JSON string value as a string
                // includes quotes, so `"hello"` becomes
                // `"\"hello\""`, which then cause redandant
                // quotes to be added when writing to a CSV.
                // So to avoid that we strip the quotes if present.
                let string = v.to_string();
                if string.starts_with('"') && string.ends_with('"') {
                    string[1..string.len() - 1].to_string()
                } else {
                    string
                }
            })
            .collect();
        w.write_record(&vals)?;
    }
    w.flush()?;
    Ok(())
}

pub fn write_csv<T: Serialize>(path: &Path, items: &[T]) {
    let mut w = csv::Writer::from_path(path).unwrap();
    for item in items {
        w.serialize(item).unwrap();
    }
    w.flush().unwrap();
}

pub fn write_yaml<D: Serialize>(d: &D, path: &Path) {
    let f = OpenOptions::new()
        .write(true)
        .create(true)
        .truncate(true)
        .open(path)
        .unwrap_or_else(|_| {
            panic!("Couldn't open file for writing: {path:#?}")
        });
    serde_yaml::to_writer(f, d).unwrap_or_else(|_| {
        panic!("Couldn't write yaml to file: {path:#?}")
    });
}

pub fn read_yaml<T: DeserializeOwned>(path: &Path) -> T {
    let file = File::open(path).unwrap_or_else(|_| {
        panic!("Couldn't open file for reading: {path:#?}")
    });
    serde_yaml::from_reader(file).unwrap_or_else(|_| {
        panic!("Couldn't read yaml from file: {path:#?}")
    })
}

#[derive(Debug, Error)]
pub enum CsvError {
    #[error("Failed to open the CSV file: {0}.")]
    FailedToOpenFile(PathBuf),

    #[error(
        "These expected columns were missing from the CSV {0:?}: {1:?}."
    )]
    ColumnsMismatch(String, Vec<String>),

    #[error("Encountered a column value/struct field of an unknown type: {0} -> {1:?}")]
    UnhandledType(String, Value),

    #[error("Failed to read a record of the CSV.")]
    FailedToReadRecord,

    #[error("Failed to read the CSV's headers.")]
    FailedToReadHeaders,

    #[error("Failed to de/serialize to the target type.")]
    FailedToDeSerialize(#[from] serde_json::Error),

    #[error("Failed to parse the string into a boolean: {0}.")]
    FailedToParseBool(#[from] std::str::ParseBoolError),

    #[error("Failed to parse the string into a float: {0}.")]
    FailedToParseFloat(#[from] std::num::ParseFloatError),

    #[error("Failed to parse the string into a int: {0}.")]
    FailedToParseInt(#[from] std::num::ParseIntError),

    #[error("I/O error: {0}.")]
    IO(#[from] std::io::Error),

    #[error("Other CSV error: {0}")]
    Other(#[from] csv::Error),
}
