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
pub mod draw;
pub mod file;

pub mod id;
pub mod probe;
pub mod record;
pub mod util;

#[cfg(feature = "plotting")]
pub mod plot;

#[cfg(feature = "forest")]
pub mod forest;

pub use gremlin_macros::*;
pub use paste;

#[cfg(test)]
mod tests {
    use super::*;
    use gremlin_macros::Row;

    #[test]
    fn test_derive_partial() {
        use crate::data::{FromPartial, HydrateError, Row};
        #[derive(
            Debug, Default, Partial, PartialEq, Clone, serde::Serialize, serde::Deserialize, Row,
        )]
        struct TestInner {
            a: String,
        }

        #[derive(Default, Partial, Row)]
        struct Test {
            a: u32,

            #[row]
            b: f32,

            c: Vec<TestInner>,

            #[partial]
            d: Vec<TestInner>,
        }
    }
}
