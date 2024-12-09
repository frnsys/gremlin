use std::{
    fmt::{Debug, Display},
    sync::{Arc, Mutex},
};

use ahash::HashMap;
use thiserror::Error;

#[macro_export]
macro_rules! imperfect {
    ($t:ty, $err:ty) => {
        impl Imperfect for $t {
            type Warning = $err;
            fn logger() -> &'static DataLogger<Self::Warning> {
                static LOGGER: std::sync::LazyLock<DataLogger<$err>> =
                    std::sync::LazyLock::new(|| DataLogger::new(stringify!($t)));
                &LOGGER
            }
        }
    };
}

#[derive(Debug, Error)]
pub enum Infallible {}
impl From<&Infallible> for &'static str {
    fn from(_other: &Infallible) -> &'static str {
        "infallible"
    }
}

#[macro_export]
macro_rules! perfect {
    ($t:ty) => {
        $crate::imperfect!($t, Infallible);
    };
}

pub trait Imperfect {
    type Warning: Display + Debug + 'static;

    fn logger() -> &'static DataLogger<Self::Warning>;

    fn warn(msg: Self::Warning) {
        Self::logger().warn(msg);
    }

    fn collect_logs() -> HashMap<&'static str, Vec<Self::Warning>>
    where
        for<'a> &'a Self::Warning: Into<&'static str>,
    {
        Self::logger().collect()
    }
}

pub struct DataLogger<T: Display + Debug> {
    pub source: &'static str,
    pub logs: Arc<Mutex<Vec<T>>>,
}
impl<T: Display + Debug> DataLogger<T> {
    pub fn new(source: &'static str) -> Self {
        Self {
            source,
            logs: Arc::new(Mutex::new(vec![])),
        }
    }

    pub fn warn(&self, msg: T) {
        let mut logs = self.logs.lock().unwrap();
        logs.push(msg);
    }

    pub fn collect(&self) -> HashMap<&'static str, Vec<T>>
    where
        for<'a> &'a T: Into<&'static str>,
    {
        let mut groups: HashMap<&'static str, Vec<T>> = HashMap::default();
        for log in self.logs.lock().unwrap().drain(..) {
            let variant_name: &'static str = (&log).into();
            let group = groups.entry(variant_name).or_default();
            group.push(log);
        }
        groups
    }
}
