//! File system helpers, including
//! serializing/deserializing from common data formats.

mod path;

use fs_err::{File, OpenOptions};
use std::path::{Path, PathBuf};

use itertools::{multizip, Itertools};
use serde::{de::DeserializeOwned, Serialize};
use serde_yaml::{Number, Value};

pub use path::*;
use thiserror::Error;

use crate::data::DataResult;

pub fn read_csv<T: DeserializeOwned, P: AsRef<Path>>(path: P) -> impl Iterator<Item = T> {
    let path = path.as_ref();
    let mut reader = csv::Reader::from_path(path)
        .inspect_err(|_err| eprintln!("Error reading CSV: {}", path.display().to_string()))
        .unwrap();
    let headers = reader.headers().unwrap().clone();
    let src = path.display().to_string();

    let display = path.display().to_string();
    reader.into_records().map(move |rec| {
        let rec = rec
            .inspect_err(|_err| eprintln!("Error reading CSV record: {}", display))
            .unwrap();
        rec.deserialize(Some(&headers))
            .inspect_err(|err| {
                match err.kind() {
                    csv::ErrorKind::Deserialize { err, .. } => {
                        if let Some(idx) = err.field() {
                            eprintln!("Field: {:?}", &headers[idx as usize]);
                        }
                    }
                    _ => {}
                }

                let rows = headers
                    .iter()
                    .zip(rec.iter())
                    .map(|(col, val)| format!("{}: {:?}", col, val))
                    .join("\n");
                eprintln!("Source: {src}\n{rows}")
            })
            .unwrap()
    })
}

pub fn read_csvs<T: DeserializeOwned, P: AsPaths>(paths: P) -> impl Iterator<Item = T> {
    paths.as_paths().into_iter().flat_map(|path| read_csv(path))
}

pub fn read_csv_str<T: DeserializeOwned>(s: &str) -> impl Iterator<Item = T> + '_ {
    let mut reader = csv::Reader::from_reader(s.as_bytes());
    let headers = reader.headers().unwrap().clone();
    reader.into_records().map(move |rec| {
        let rec = rec.unwrap();
        rec.deserialize(Some(&headers))
            .inspect_err(|_err| eprintln!("{rec:?}"))
            .unwrap()
    })
}

/// Read raw rows from a CSV, specifying the column names.
/// The column order doesn't need to match what's in the CSV;
/// they will automatically be rearranged to match.
pub fn read_rows<const N: usize>(path: &Path, columns: &[&str; N]) -> DataResult<Vec<[String; N]>> {
    let mut rdr = csv::Reader::from_path(path)?;
    let headers = rdr.headers().expect("Should have headers");
    let idxs: [usize; N] = columns.map(|col| {
        headers
            .iter()
            .position(|field| field == col)
            .unwrap_or_else(|| panic!(r#"Couldn't find column "{}""#, col))
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
/// - We first serialize a default copy of the object to a YAML
///     object so we learn the flattened column names and types.
///     This is why `T` must implement both `Serialize` and `Default`.
/// - We read the CSV and check that the columns look correct.
/// - Then we parse each CSV record according to what we learned
///     from the YAML-serialized example.
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
///
/// Note that YAML is preferred over JSON because it supports NaN and infinity values.
pub fn read_flat_csv<
    T: Serialize + DeserializeOwned + Default,
    P: AsRef<Path> + std::fmt::Debug,
>(
    path: P,
) -> Result<Vec<T>, CsvError> {
    tracing::debug!("Deserializing CSV: {:#?}", path);
    let path = path.as_ref();
    let reader = csv::Reader::from_path(path).map_err(|err| {
        tracing::error!("{:?}", err);
        CsvError::FailedToOpenFile(path.to_path_buf())
    })?;

    let source = path.display().to_string();
    _read_flat_csv(source, reader)
}

pub fn read_flat_csv_str<T: Serialize + DeserializeOwned + Default>(
    s: &str,
) -> Result<Vec<T>, CsvError> {
    let reader = csv::Reader::from_reader(s.as_bytes());
    _read_flat_csv("(raw string)".into(), reader)
}

fn _read_flat_csv<R: std::io::Read, T: Serialize + DeserializeOwned + Default>(
    source: String,
    mut reader: csv::Reader<R>,
) -> Result<Vec<T>, CsvError> {
    let ref_val = T::default();
    let yaml_val = serde_yaml::to_value(&ref_val)?;
    let yaml_obj = yaml_val
        .as_mapping()
        .expect("We gave an object so we get one back");
    let ref_cols: Vec<String> = yaml_obj
        .keys()
        .map(|val| val.as_str().expect("All keys are strings").to_string())
        .collect();

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
        return Err(CsvError::ColumnsMismatch(source, missing));
    }

    let idxs: Vec<usize> = ref_cols
        .iter()
        .filter_map(|col| cols.iter().position(|c| c == col))
        .collect();

    let mut data = vec![];
    for rec in reader.records() {
        let rec = rec.map_err(|_err| CsvError::FailedToReadRecord)?;
        let mut map = serde_yaml::Mapping::new();
        for (idx, typ) in multizip((idxs.iter(), yaml_obj.values())) {
            let val = &rec[*idx];
            let col = &cols[*idx];
            let key = Value::String(col.to_string());
            match typ {
                Value::Bool(_) => {
                    map.insert(key, Value::from(val.parse::<bool>()?));
                }
                Value::Number(num) => {
                    if num.is_f64() {
                        map.insert(
                            key,
                            if val == ".nan" {
                                // How yaml encodes nans
                                Value::Number(Number::from(f32::NAN))
                            } else {
                                Value::from(val.parse::<f32>().inspect_err(|_err| {
                                    tracing::error!("Failed to parse float: {val}");
                                })?)
                            },
                        );
                    } else if num.is_i64() {
                        map.insert(key, Value::Number(Number::from(val.parse::<i32>()?)));
                    } else if num.is_u64() {
                        map.insert(key, Value::Number(Number::from(val.parse::<u32>()?)));
                    }
                }
                Value::String(_) => {
                    map.insert(key, Value::String(val.into()));
                }
                _ => {
                    return Err(CsvError::UnhandledType(col.clone(), typ.clone()));
                }
            }
        }
        let obj: T = serde_yaml::from_value(Value::Mapping(map))?;
        data.push(obj);
    }
    Ok(data)
}

/// A hacky way to serialize a type to a flattened CSV representation.
/// See [`read_flat_csv`] for more details.
pub fn write_flat_csv<T: Serialize + Default, P: AsRef<Path> + std::fmt::Debug>(
    path: P,
    items: &[T],
) -> Result<(), CsvError> {
    tracing::debug!("Serializing CSV: {:#?}", path);
    let mut w = csv::Writer::from_path(path.as_ref())
        .map_err(|_err| CsvError::FailedToOpenFile(path.as_ref().to_path_buf()))?;

    let ref_val = T::default();
    let yaml_val = serde_yaml::to_value(&ref_val)?;
    let yaml_obj = yaml_val
        .as_mapping()
        .expect("We gave an object so we get one back");
    let ref_cols: Vec<String> = yaml_obj
        .keys()
        .map(|val| val.as_str().expect("All keys are strings").to_string())
        .collect();
    w.write_record(ref_cols)?;

    for item in items {
        let yaml_val = serde_yaml::to_value(item)?;
        let obj = yaml_val
            .as_mapping()
            .expect("We gave an object so we get one back");
        let vals: Vec<String> = obj
            .values()
            .map(|v| {
                // A JSON string value as a string
                // includes quotes, so `"hello"` becomes
                // `"\"hello\""`, which then cause redandant
                // quotes to be added when writing to a CSV.
                // So to avoid that we strip the quotes if present.
                let string = serde_yaml::to_string(v)
                    .expect("Is primitive and should serialize")
                    .trim()
                    .to_string();
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

pub fn write_csv<T: Serialize + std::fmt::Debug>(path: &Path, items: &[T]) {
    let mut w = csv::Writer::from_path(path).unwrap();
    for item in items {
        w.serialize(item)
            .inspect_err(|_err| eprintln!("{:?}", item))
            .unwrap();
    }
    w.flush().unwrap();
}

pub fn write_yaml<D: Serialize>(d: &D, path: &Path) {
    let f = OpenOptions::new()
        .write(true)
        .create(true)
        .truncate(true)
        .open(path)
        .unwrap_or_else(|_| panic!("Couldn't open file for writing: {path:#?}"));
    serde_yaml::to_writer(f, d)
        .unwrap_or_else(|_| panic!("Couldn't write yaml to file: {path:#?}"));
}

pub fn read_yaml<T: DeserializeOwned>(path: &Path) -> T {
    let file =
        File::open(path).unwrap_or_else(|_| panic!("Couldn't open file for reading: {path:#?}"));
    serde_yaml::from_reader(file)
        .unwrap_or_else(|_| panic!("Couldn't read yaml from file: {path:#?}"))
}

#[derive(Debug, Error)]
pub enum CsvError {
    #[error("Failed to open the CSV file: {0}.")]
    FailedToOpenFile(PathBuf),

    #[error("These expected columns were missing from the CSV {0:?}: {1:?}.")]
    ColumnsMismatch(String, Vec<String>),

    #[error("Encountered a column value/struct field of an unknown type: {0} -> {1:?}")]
    UnhandledType(String, Value),

    #[error("Failed to read a record of the CSV.")]
    FailedToReadRecord,

    #[error("Failed to read the CSV's headers.")]
    FailedToReadHeaders,

    #[error("Failed to de/serialize to the target type.")]
    FailedToDeSerialize(#[from] serde_yaml::Error),

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

#[cfg(test)]
mod tests {
    use serde::Deserialize;

    use super::*;

    serde_with::with_prefix!(inner "inner.");

    #[test]
    fn test_flat_csv_roundtrip() {
        #[derive(Debug, Default, Serialize, Deserialize)]
        struct Inner {
            x: f32,
            y: String,
            z: usize,
            w: bool,
        }
        impl PartialEq for Inner {
            fn eq(&self, other: &Self) -> bool {
                // NOTE: NaNs normally not considered equal.
                (self.x == other.x || self.x.is_nan() && other.x.is_nan())
                    && self.y == other.y
                    && self.z == other.z
                    && self.w == other.w
            }
        }

        #[derive(Debug, Default, Serialize, Deserialize)]
        struct Outer {
            a: f32,
            b: String,
            c: usize,
            d: bool,

            #[serde(flatten, with = "inner")]
            inner: Inner,
        }
        impl PartialEq for Outer {
            fn eq(&self, other: &Self) -> bool {
                // NOTE: NaNs normally not considered equal.
                (self.a == other.a || self.a.is_nan() && other.a.is_nan())
                    && self.b == other.b
                    && self.c == other.c
                    && self.d == other.d
                    && self.inner == other.inner
            }
        }

        let items = vec![
            Outer {
                a: 12.,
                b: "First".to_string(),
                c: 25,
                d: true,
                inner: Inner {
                    x: f32::NAN,
                    y: "First-Inner".to_string(),
                    z: 48,
                    w: true,
                },
            },
            Outer {
                a: f32::NAN,
                b: "Second".to_string(),
                c: 6,
                d: false,
                inner: Inner {
                    x: 19.,
                    y: "Second-Inner".to_string(),
                    z: 7,
                    w: true,
                },
            },
        ];

        write_flat_csv("/tmp/flat_csv_test.csv", &items).unwrap();

        let items_read: Vec<Outer> = read_flat_csv("/tmp/flat_csv_test.csv").unwrap();
        assert_eq!(items, items_read);
    }
}
