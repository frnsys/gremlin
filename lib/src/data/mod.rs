//! Data processing helpers.

mod cache;
pub mod compare;
mod constrain;
mod datamap;
mod dataset;
mod display;
mod error;
mod facet;
pub mod geotime;
mod helpers;
mod imperfect;
mod iterator;
mod partial;
pub mod profile;
mod reference;
mod report;
mod river;
mod series;

// Behind a feature gate as the
// polars dependency can be quite heavy.
#[cfg(feature = "imputing")]
pub mod impute;

#[cfg(feature = "imputing")]
pub use impute::*;

pub use cache::*;
pub use constrain::*;
pub use datamap::*;
pub use dataset::*;
pub use error::*;
pub use facet::*;
pub use helpers::*;
pub use imperfect::*;
pub use iterator::*;
pub use partial::*;
pub use reference::*;
pub use river::*;
pub use series::*;

pub use time::{
    self, macros::format_description, serde::format_description as serde_description, Date,
    OffsetDateTime, PrimitiveDateTime,
};
