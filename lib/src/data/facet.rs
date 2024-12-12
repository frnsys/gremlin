use std::fmt::Display;

pub trait Facet: Display + Ord {
    // This is just a more convenient way to
    // convert to a different facet using generics;
    // e.g. `f.to_facet::<OtherFacet>`.
    fn to_facet<F: Facet>(&self) -> Vec<F>
    where
        Self: AsFacet<F>;
}
impl<T: Display + Ord> Facet for T {
    fn to_facet<F: Facet>(&self) -> Vec<F>
    where
        Self: AsFacet<F>,
    {
        AsFacet::as_facet(self)
    }
}

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
