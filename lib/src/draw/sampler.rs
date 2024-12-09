//! Probablistic machine learning models.
//! See <https://www.lace.dev/> for more details.

//! A [`Sampler`] is essentially a learned conditional
//! joint probability distribution (using the [`lace`](https://www.lace.dev/) crate).
//!
//! The distribution itself is learned using [`PmlModel`](model::PmlModel).
//!
//! A type `T` that is to be sampled using a [`Sampler`] needs to implement
//! the [`Sampleable`] trait, which defines how the raw sampled values
//! are to be interpreted as an instance of `T` and what the expected
//! given/fixed values the sampling will be conditioned on (as a type `G`
//! that implements the [`Given`] trait).
//!
//! The easiest way to implement the traits is to use the [`sampleable`]
//! macro. See the source of [`Generator`](../impulse_model/production/dispatch/struct.Generator.html) for an example.

use std::{marker::PhantomData, path::Path};

use super::transform::*;
use ahash::{HashMap, HashSet};
pub use lace::{self, Datum};
use lace::{metadata::Error as LaceError, Category, Engine, Oracle, OracleT};
use rand::rngs::ThreadRng;
use serde::{Deserialize, Serialize};
use thiserror::Error;
use time::{Date, OffsetDateTime, Time};
use tracing::warn;

/// Defines the maximum number of attempts
/// in producing valid samples (i.e. those
/// where all variables satisfy their constraints)
/// before falling back to a more naive (i.e. non-joint-conditional)
/// sampling process. This scales based on the number of samples requested,
/// e.g. if `n` samples are asked for, the number of attempts will be
/// `MAX_TRIES_PER_SAMPLE * n`.
const MAX_TRIES_PER_SAMPLE: usize = 100;

#[derive(Error, Debug)]
pub enum SamplerError {
    #[error("Failed to load engine: {0}")]
    FailedToLoadEngine(#[from] LaceError),

    #[error("Failed to read columns: {0}")]
    FailedToLoadColumns(#[from] std::io::Error),

    #[error("Failed to parse columns: {0}")]
    FailedToParseColumns(#[from] serde_yaml::Error),

    #[error("Failed to draw samples: {0}")]
    FailedToSimulate(#[from] lace::error::SimulateError),

    #[error("Column data for {0} was expected to be float, but it wasn't")]
    IncorrectColumnType(String),

    #[error("Missing expected column: {0}")]
    MissingColumn(String),

    #[error("Provided value is out of expected range: {0:?}: {1}")]
    OutOfRange(Constraint, f64),
}

/// A constraint on a sampled variables values.
#[derive(Debug, Copy, Clone, Serialize, Deserialize)]
pub enum Constraint {
    LowerBound(f32),
    UpperBound(f32),
    LowerBoundInclusive(f32),
    UpperBoundInclusive(f32),
    Range(f32, f32),
    RangeInclusive(f32, f32),
}
impl Constraint {
    pub fn is_valid(&self, value: f32) -> bool {
        match self {
            Self::LowerBound(c) => value > *c,
            Self::LowerBoundInclusive(c) => value >= *c,
            Self::UpperBound(c) => value < *c,
            Self::UpperBoundInclusive(c) => value <= *c,
            Self::Range(l, h) => value > *l && value < *h,
            Self::RangeInclusive(l, h) => value >= *l && value <= *h,
        }
    }

    pub fn transform(&self, value: f64) -> Result<f64, SamplerError> {
        const EPSILON: f64 = 1e-10;
        let transformed = match self {
            Self::LowerBound(c) => log_lb(value, *c as f64),
            Self::LowerBoundInclusive(c) => log_lb(value, *c as f64 - EPSILON),
            Self::UpperBound(c) => log_ub(value, *c as f64),
            Self::UpperBoundInclusive(c) => log_ub(value, *c as f64 + EPSILON),
            Self::Range(l, h) => logit_gen(value, *l as f64, *h as f64),
            Self::RangeInclusive(l, h) => {
                logit_gen(value, *l as f64 - EPSILON, *h as f64 + EPSILON)
            }
        };
        if transformed.is_nan() {
            Err(SamplerError::OutOfRange(self.clone(), value))
        } else {
            Ok(transformed)
        }
    }

    pub fn inv_transform(&self, value: f64) -> f64 {
        match self {
            Self::LowerBound(c) => inv_log_lb(value, *c as f64),
            Self::LowerBoundInclusive(c) => inv_log_lb(value, *c as f64),
            Self::UpperBound(c) => inv_log_ub(value, *c as f64),
            Self::UpperBoundInclusive(c) => inv_log_ub(value, *c as f64),
            Self::Range(l, h) => inv_logit_gen(value, *l as f64, *h as f64),
            Self::RangeInclusive(l, h) => inv_logit_gen(value, *l as f64, *h as f64),
        }
    }
}

/// A column represents a sampleable variable
/// with an optional [`Constraint`].
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Column {
    pub name: String,
    pub constraint: Option<Constraint>,
}
impl Column {
    pub fn new(name: String, constraint: Option<Constraint>) -> Column {
        Column { name, constraint }
    }
}
impl From<String> for Column {
    fn from(value: String) -> Self {
        Column {
            name: value,
            constraint: None,
        }
    }
}

/// This lets use the custom [`Column`] type for indexing with lace.
impl lace::ColumnIndex for Column {
    fn col_ix(&self, codebook: &lace::prelude::Codebook) -> Result<usize, lace::error::IndexError> {
        self.name.col_ix(codebook)
    }

    fn col_str(&self) -> Option<&str> {
        self.name.col_str()
    }

    fn col_usize(&self) -> Option<usize> {
        self.name.col_usize()
    }
}

/// A sampler running off a model trained using [`PmlModel`](model::PmlModel).
#[derive(Debug, Clone)]
pub struct Sampler<T: Sampleable = ()> {
    oracle: Oracle,
    columns: Vec<Column>,
    marker: PhantomData<T>,

    // Cache a mapping of column name to constraints.
    constraints: HashMap<String, Constraint>,
}
impl<T: Sampleable> Sampler<T> {
    pub fn new(engine: Engine, columns: Vec<Column>) -> Sampler<T> {
        let oracle = Oracle::from_engine(engine);

        let constraints: HashMap<String, Constraint> = columns
            .iter()
            .filter_map(|col| {
                col.constraint
                    .map(|constraint| (col.name.to_string(), constraint))
            })
            .collect();

        Sampler {
            oracle,
            columns,
            constraints,
            marker: PhantomData,
        }
    }

    /// Create a sampler by loading a pre-learned [`lace::Engine`].
    /// The model folder usually ends with `.lace`.
    pub fn load(path: &Path) -> Result<Sampler<T>, SamplerError> {
        let engine = Engine::load(path)?;

        let column_path = path.join("columns.yml");
        let reader = fs_err::File::open(column_path)?;
        let columns: Vec<Column> = serde_yaml::from_reader(reader)?;
        // let columns: Vec<String> = oracle
        //     .codebook
        //     .col_metadata
        //     .iter()
        //     .map(|m| m.name.clone())
        //     .collect();
        Ok(Self::new(engine, columns))
    }

    /// Sample across all columns using the given values.
    pub fn sample(
        &self,
        n_samples: usize,
        given: Option<&T::Given>,
    ) -> Result<Vec<T>, SamplerError> {
        let in_data = given.map_or(vec![], |given| given.to_data());
        let given_cols: Vec<_> = in_data.iter().map(|g| g.0).collect();
        let columns: Vec<_> = self
            .columns
            .iter()
            .map(|c| c.name.as_str())
            .filter(|name| !given_cols.contains(name))
            .collect();
        let samples = self.sample_columns(n_samples, &in_data, &columns)?;
        let samples = samples
            .into_iter()
            .map(|data| T::from_data(given, data))
            .collect::<Result<_, _>>()?;
        Ok(samples)
    }

    /// Produce a joint sample for `columns`, with the `given` values.
    /// Samples are returned as rows of float values.
    ///
    /// This will only return samples that satisfy any column constraints (see [`Column`]).
    /// However this may in practice be impossible, depending on the data and model fit.
    /// After [`MAX_TRIES_PER_SAMPLE`] for each sample the sampling will fallback to a less
    /// restricted version (i.e. unconditional sampling, but still respecting column constraints),
    /// which will probably lead to degraded-quality samples.
    ///
    /// NOTE: This assumes that all columns are of type `f64` (coerced to `f32`).
    ///
    /// # Improvements
    ///
    /// Lace also supports `u8` column types (for categorical data), so we can add that in later.
    pub fn sample_columns<'a>(
        &'a self,
        n_samples: usize,
        given: &[(&str, Datum)],
        columns: &[&'a str],
    ) -> Result<Vec<HashMap<&'a str, f32>>, SamplerError> {
        let mut rng = rand::thread_rng();
        let given = lace::Given::Conditions(given.to_vec());
        let max_tries = MAX_TRIES_PER_SAMPLE * n_samples;

        // Sample with constraints
        let mut samples = Vec::with_capacity(n_samples);
        let mut n_attempts = 0;
        let mut failed_columns = HashSet::default();
        let mut fallback_sampling = false;
        while samples.len() < n_samples {
            let results = self
                .oracle
                .simulate(columns, &given, n_samples, None, &mut rng)?;
            let valid = results
                .into_iter()
                .filter_map(|sample| {
                    sample
                        .into_iter()
                        .zip(columns)
                        .map(|(val, col)| {
                            let (maybe_val, failed) = self.extract_and_check_value(
                                val,
                                col,
                                fallback_sampling,
                                &mut rng,
                            )?;
                            if failed {
                                failed_columns.insert(col);
                            }
                            Ok(maybe_val.map(|val| (*col, val)))
                        })
                        .collect::<Result<Option<HashMap<_, _>>, SamplerError>>()
                        .transpose()
                })
                .collect::<Result<Vec<HashMap<&str, f32>>, SamplerError>>()?;
            samples.extend(valid);

            n_attempts += 1;
            if n_attempts == max_tries {
                warn!("Sampling still not finished after {max_tries} attempts. There may be an unsatisfiable constraint or the model may need more fitting time.");
                warn!("Problematic columns: {:?}", failed_columns);
                warn!("From now on we'll sample unconditionally for the failed columns.");
                fallback_sampling = true;
            }
        }
        Ok(samples)
    }

    fn extract_and_check_value(
        &self,
        val: Datum,
        col: &str,
        use_fallback: bool,
        rng: &mut ThreadRng,
    ) -> Result<(Option<f32>, bool), SamplerError> {
        // Note: The only other option here is `to_u8_opt()`
        // for categorical data.
        let val = val
            .to_f64_opt()
            .ok_or_else(|| SamplerError::IncorrectColumnType(col.to_string()))?;
        if let Some(constraint) = self.constraints.get(col) {
            let val = constraint.inv_transform(val) as f32;
            let ok = constraint.is_valid(val);
            if ok {
                Ok((Some(val), false))
            } else {
                // PERF this can be more efficient
                if use_fallback {
                    let results = self.oracle.simulate(
                        &[col],
                        &lace::Given::<&str>::Nothing,
                        1,
                        None,
                        rng,
                    )?;
                    let val = results[0][0]
                        .to_f64_opt()
                        .ok_or_else(|| SamplerError::IncorrectColumnType(col.to_string()))?
                        as f32;
                    if constraint.is_valid(val) {
                        return Ok((Some(val), true));
                    }
                }
                Ok((None, true))
            }
        } else {
            Ok((Some(val as f32), false))
        }
    }
}

/// Implement this so a type can be sampled using a [`Sampler`].
pub trait Sampleable: Sized {
    type Given: Given;
    fn from_data(
        given: Option<&Self::Given>,
        data: HashMap<&str, f32>,
    ) -> Result<Self, SamplerError>;
}

/// Dummy implementation for samplers that
/// just need to return column data.
impl Sampleable for () {
    type Given = ();
    fn from_data(
        _given: Option<&Self::Given>,
        _data: HashMap<&str, f32>,
    ) -> Result<Self, SamplerError> {
        Ok(())
    }
}
impl Given for () {
    fn to_data(&self) -> Vec<(&str, Datum)> {
        vec![]
    }
}

/// Implement this trait for a set of data that
/// a sample is conditioned on.
pub trait Given {
    fn to_data(&self) -> Vec<(&str, Datum)>;
}

/// A convenience trait for turning types
/// into [`Datum`]s for sampling.
pub trait AsDatum {
    fn as_datum(&self) -> Datum;
}
impl AsDatum for bool {
    fn as_datum(&self) -> Datum {
        Datum::Binary(*self)
    }
}
impl AsDatum for f32 {
    fn as_datum(&self) -> Datum {
        Datum::Continuous(*self as f64)
    }
}
impl AsDatum for String {
    fn as_datum(&self) -> Datum {
        (&self).as_datum()
    }
}
impl AsDatum for &String {
    fn as_datum(&self) -> Datum {
        Datum::Categorical(Category::String(self.to_string()))
    }
}
impl AsDatum for Date {
    fn as_datum(&self) -> Datum {
        let dt = OffsetDateTime::new_utc(*self, Time::MIDNIGHT);
        let ts = dt.unix_timestamp() as f64;
        Datum::Continuous(ts)
    }
}

/// Shortcut for implementing the [`Sampler`] trait for a type.
#[macro_export]
macro_rules! sampleable {
    ( $given:ident { $($g_field:ident: $g_type:ty => $g_key:literal),* $(,)? }, $t:ident { $($field:ident$(.$prop:ident)* => $key:literal),* $(,)? }, $finish:expr) => {
        use $crate::draw::{lace, Sampleable, SamplerError, Given};

        pub struct $given {
            $(pub $g_field: $g_type,)*
        }
        impl Given for $given {
            fn to_data(&self) -> Vec<(&str, Datum)> {
                vec![
                    $(($g_key, self.$g_field.as_datum()),)*
                ]
            }
        }

        use $crate::draw::lace::Datum;
        impl Sampleable for $t {
            type Given = $given;

            fn from_data(given: Option<&Self::Given>, data: HashMap<&str, f32>) -> Result<Self, SamplerError> {
                macro_rules! get {
                    ( $k:literal ) => {
                        *data.get($k).ok_or_else(|| SamplerError::MissingColumn($k.to_string()))?
                    };
                }
                let mut item: $t = Default::default();
                $(
                    item.$field$(.$prop)* = get!($key).into();
                )*

                $finish(&mut item, given, data);
                Ok(item)
            }
        }
    }
}
