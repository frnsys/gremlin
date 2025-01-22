use thiserror::Error;

#[derive(Debug, Error)]
pub enum DataError {
    #[error("IO error: {0:?}")]
    IO(#[from] std::io::Error),

    #[error("CSV error: {0:?}")]
    Csv(#[from] csv::Error),

    #[error("Cache encode error: {0:?}")]
    Encode(#[from] rmp_serde::encode::Error),

    #[error("Cache decode error: {0:?}")]
    Decode(#[from] rmp_serde::decode::Error),

    #[error("Parse value error: {0:?}")]
    Parse(String),

    #[error("Parse float error: {0:?}")]
    ParseFloat(#[from] std::num::ParseFloatError),

    #[error("Polars error: {0:?}")]
    Polars(#[from] polars::prelude::PolarsError),

    #[error("Infallible error: {0:?}")]
    Infallible(#[from] std::convert::Infallible),

    #[error("Cache function error: {0:?}")]
    CacheFunction(Box<dyn std::error::Error + Send + Sync>),
}

pub type DataResult<T> = Result<T, DataError>;
