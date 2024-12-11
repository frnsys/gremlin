use std::fmt::Display;

pub trait Facet: Display + Ord {}
impl<T: Display + Ord> Facet for T {}

/// We use this rather than `Into<F>`
/// because one facet may map to multiple other facets.
pub trait AsFacet<F: Facet> {
    fn as_facet(&self) -> Vec<F>;
}

impl<F: Facet + Clone> AsFacet<F> for F {
    fn as_facet(&self) -> Vec<F> {
        vec![self.clone()]
    }
}
