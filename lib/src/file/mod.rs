mod path;

use std::{
    fs::{File, OpenOptions},
    path::Path,
};

use anyhow::Context;
use serde::{de::DeserializeOwned, Serialize};

pub use path::*;

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

pub fn write_csv<T: Serialize>(items: &[T], path: &Path) {
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
        .expect(&format!(
            "Couldn't open file for writing: {path:#?}"
        ));
    serde_yaml::to_writer(f, d).expect(&format!(
        "Couldn't write yaml to file: {path:#?}"
    ));
}

pub fn read_yaml<T: DeserializeOwned>(path: &Path) -> T {
    let file = File::open(path).expect(&format!(
        "Couldn't open file for reading: {path:#?}"
    ));
    serde_yaml::from_reader(file).expect(&format!(
        "Couldn't read yaml from file: {path:#?}"
    ))
}
