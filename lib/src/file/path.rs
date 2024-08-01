use std::path::{Path, PathBuf};

use glob::glob;
use tracing::warn;

/// Convenience trait for types that can
/// be turned into a list of paths.
pub trait AsPaths {
    fn as_paths(&self) -> Vec<PathBuf>;
}

/// A `&str` can be interpreted as a
/// glob pattern to give a list of paths.
impl AsPaths for &str {
    fn as_paths(&self) -> Vec<PathBuf> {
        let paths = glob(self)
            .expect("Failed to read glob pattern")
            .collect::<Result<Vec<PathBuf>, _>>()
            .unwrap();
        if paths.is_empty() {
            warn!("No matching files found for {:?}", &self);
        }

        paths
    }
}

impl AsPaths for &Path {
    fn as_paths(&self) -> Vec<PathBuf> {
        self.to_str().unwrap().as_paths()
    }
}
