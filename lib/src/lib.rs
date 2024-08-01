//! A collection of utilities to make writing
//! a model more concerned with semantics and
//! come together intuitively, rather than getting
//! bogged down in too many implementation details.
//!
//! This includes features like:
//! - Data structures for working with timeseries.
//! - Units leveraging the type system so that the expected
//!     units are self-documented throughout the model and
//!     checked at compile-time.
//! - Helpers for processing raw data into consistent types.
//! - Helpers for automating some aspects of model documentation.
//! - Helpers for capturing model outputs.

pub mod core;
pub mod data;
pub mod docs;
pub mod file;
pub mod id;
pub mod plot;
pub mod record;
pub mod util;

pub use gremlin_macros::*;
