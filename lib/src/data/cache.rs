//! For caching intermediate outputs.

use std::{
    io::{BufReader, BufWriter},
    path::{Path, PathBuf},
};

use fs_err::File;
use rmp_serde::{Deserializer, Serializer};
use serde::{de::DeserializeOwned, Deserialize, Serialize};
use tracing::info;

use crate::data::DataError;

use super::error::DataResult;

/// Use this to process/generate a dataset
/// or load from cached results if available.
///
/// Cached results are serialized as MessagePack
/// and saved to the `cache` folder.
///
/// For example, the following checks if `cache/solar_capacity_factors.msgpack` exists:
///
/// - if it does, the data is loaded from there
/// - otherwise `flows::solar_capacity_factors` is run and the data will be saved to that file.
///
/// To force a re-computation, e.g. when something about the processing changes,
/// you only need to delete `cache/solar_capacity_factors.msgpack`.
///
/// ```
/// cached_or("solar_capacity_factors", flows::solar_capacity_factors)?;
/// ```
pub fn cached_or<
    T: DeserializeOwned + Serialize,
    P: AsRef<Path>,
    E: std::error::Error + Send + Sync + 'static,
>(
    dir: P,
    name: &str,
    f: impl Fn() -> Result<T, E>,
) -> DataResult<T> {
    let path = cached_path(&dir, name)?;
    if path.exists() {
        info!("Cached file found: {:#?}", path);
        cached(&dir, name)
    } else {
        info!("Cached file not found, generating: {:#?}", path);
        let val = f().map_err(|err| DataError::CacheFunction(Box::new(err)))?;
        recache(&dir, name, val)
    }
}

/// Force a overwrite of cached data.
pub fn recache<T: DeserializeOwned + Serialize, P: AsRef<Path>>(
    dir: &P,
    name: &str,
    data: T,
) -> DataResult<T> {
    let path = cached_path(dir, name)?;
    let file = File::create(path)?;
    let mut writer = BufWriter::new(file);
    data.serialize(&mut Serializer::new(&mut writer))?;
    Ok(data)
}

/// Load cached data. Will return `Err` if the cached file doesn't exist.
pub fn cached<T: DeserializeOwned, P: AsRef<Path>>(dir: &P, name: &str) -> DataResult<T> {
    let path = cached_path(dir, name)?;
    let file = File::open(path)?;
    let reader = BufReader::new(file);
    let mut de = Deserializer::new(reader);
    let val = Deserialize::deserialize(&mut de)?;
    Ok(val)
}

fn cached_path<P: AsRef<Path>>(dir: &P, name: &str) -> DataResult<PathBuf> {
    let dir: &Path = dir.as_ref();
    if !dir.exists() {
        fs_err::create_dir_all(dir)?;
    }

    let mut path = dir.join(name);
    path.set_extension("msgpack");
    Ok(path)
}
