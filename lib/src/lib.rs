//! A collection of utilities to make writing
//! a model more concerned with semantics and
//! come together intuitively, rather than getting
//! bogged down in too many implementation details.
//!
//! This includes features like:
//! - Data structures for working with timeseries.
//! - Units leveraging the type system so that the expected
//!   units are self-documented throughout the model and
//!   checked at compile-time.
//! - Helpers for processing raw data into consistent types.
//! - Helpers for automating some aspects of model documentation.
//! - Helpers for capturing model outputs.

#![feature(error_generic_member_access)]

pub mod core;
pub mod data;
pub mod docs;
pub mod file;

pub mod bayes;
pub mod id;
pub mod logging;
pub mod record;
pub mod stats;
pub mod util;

#[cfg(feature = "plotting")]
pub mod plot;

#[cfg(feature = "forest")]
pub mod forest;

pub use gremlin_macros::*;
pub use paste;
pub use thousands::Separable;
