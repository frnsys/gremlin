//! Learn and draw samples from a distribution.

#[cfg(feature = "learning")]
mod model;
pub use model::*;

#[cfg(feature = "sampling")]
mod sampler;
pub use sampler::*;
