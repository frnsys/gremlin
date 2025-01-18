use std::collections::BTreeMap;

use super::{AsFacet, ByFacet, ByField, Facet};

#[derive(Debug)]
pub struct Reference<F: Facet> {
    /// A label indicating the source of this reference.
    pub source: String,

    pub aggregate: BTreeMap<String, f32>,
    pub by_facet: BTreeMap<F, BTreeMap<String, f32>>,
}
impl<F: Facet> Reference<F> {
    pub fn for_all(&self, var: &str) -> Option<&f32> {
        self.aggregate.get(var)
    }

    pub fn for_facet(&self, facet: impl AsFacet<F>, var: &str) -> Vec<&f32> {
        let facet = facet.as_facet();
        facet
            .iter()
            .filter_map(|facet| self.by_facet.get(facet).and_then(|vars| vars.get(var)))
            .collect()
    }

    pub fn by_facet(&self) -> BTreeMap<Option<F>, BTreeMap<String, f32>> {
        let mut by_facet: BTreeMap<Option<F>, BTreeMap<String, f32>> = BTreeMap::default();
        by_facet.insert(None, self.aggregate.clone());
        for (facet, vars) in &self.by_facet {
            by_facet.insert(Some(facet.clone()), vars.clone());
        }
        by_facet
    }

    pub fn to_other_facet<T: Facet>(&self) -> ByFacet<ByFacet<ByField<f32>>>
    where
        F: AsFacet<T>,
    {
        let mut by_facet: ByFacet<ByFacet<ByField<f32>>> = BTreeMap::default();
        for (source_facet, vars) in self.by_facet() {
            match source_facet {
                Some(source_facet) => {
                    let facet_name = source_facet.to_string();
                    let mapped_facets = by_facet.entry(Some(facet_name)).or_default();
                    for facet in source_facet.to_facet::<T>() {
                        mapped_facets.insert(Some(facet.to_string()), vars.clone());
                    }
                }
                None => {
                    let mapped_facets = by_facet.entry(None).or_default();
                    mapped_facets.insert(None, vars);
                }
            }
        }
        by_facet
    }
}

/// A trait that indicates a [`Reference`] can be used for a different [`Facet`]
/// than the one it originated with.
pub trait RefForFacet<F: Facet> {
    fn source(&self) -> &str;
    fn aggregate(&self) -> &BTreeMap<String, f32>;
    fn for_facet(&self, facet: &F) -> BTreeMap<String, &BTreeMap<String, f32>>;
}
impl<F1: Facet, F2: Facet> RefForFacet<F1> for Reference<F2>
where
    F1: AsFacet<F2>,
{
    fn source(&self) -> &str {
        &self.source
    }

    fn aggregate(&self) -> &BTreeMap<String, f32> {
        &self.aggregate
    }

    fn for_facet(&self, facet: &F1) -> BTreeMap<String, &BTreeMap<String, f32>> {
        let facet: Vec<F2> = facet.as_facet();
        facet
            .iter()
            .filter_map(|facet| {
                self.by_facet
                    .get(facet)
                    .map(|refs| (facet.to_string(), refs))
            })
            .collect()
    }
}
