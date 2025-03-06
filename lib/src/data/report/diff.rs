use std::collections::BTreeMap;

use crate::data::{profile::VarProfile, Breach, ByConstraint, ByFacet, ByField};

#[derive(Debug, Clone)]
pub struct Diff<T: Clone> {
    pub current: T,
    pub previous: Option<T>,
}
impl Default for Diff<isize> {
    fn default() -> Self {
        Self {
            current: 0,
            previous: Some(0),
        }
    }
}
impl<T: Clone> Diff<T> {
    pub fn new(current: T) -> Self {
        Self {
            current,
            previous: None,
        }
    }

    pub fn update(&mut self, current: T) {
        let previous = std::mem::replace(&mut self.current, current);
        self.previous = Some(previous);
    }
}
impl Diff<isize> {
    pub fn previous(&self) -> isize {
        self.previous.unwrap_or_default()
    }

    pub fn change(&self) -> isize {
        self.current - self.previous()
    }
}
impl std::fmt::Display for Diff<isize> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{} -> {}", self.previous(), self.current)
    }
}

impl Diff<&ByField<VarProfile>> {
    pub fn iter(&self) -> Vec<(&String, &VarProfile, Option<&VarProfile>)> {
        match self.previous {
            None => self
                .current
                .iter()
                .map(|(var, cur)| (var, cur, None))
                .collect(),
            Some(prev) => self
                .current
                .iter()
                .map(|(var, cur)| (var, cur, prev.get(var)))
                .collect(),
        }
    }
}

impl Diff<ByFacet<ByField<VarProfile>>> {
    pub fn count_changes(&self) -> ByFacet<ByField<usize>> {
        let mut counts = BTreeMap::default();
        if let Some(prev) = &self.previous {
            for (facet, vars) in &self.current {
                let mut changes = BTreeMap::default();
                if let Some(prev) = prev.get(facet) {
                    for (var, cur) in vars {
                        let count = prev.get(var).map_or(0, |prev| {
                            if has_diff(cur.summary.mean, prev.summary.mean) {
                                1
                            } else {
                                0
                            }
                        });
                        changes.insert(var.clone(), count);
                    }
                }
                counts.insert(facet.clone(), changes);
            }
        }
        counts
    }
}

/// Whether or not the provided difference
/// is a meaningful difference.
/// If it`s 0, that means no difference.
/// If both the difference and the current value are
/// NaN, that also means no meaningful difference.
fn has_diff(cur: f32, prev: f32) -> bool {
    let diff = cur - prev;
    !(diff == 0. || (diff.is_nan() && cur.is_nan()))
}

impl Diff<Vec<Breach>> {
    /// Group breaches by constraint and by field and then count them.
    pub fn by_constraint(&self) -> ByField<ByConstraint<Diff<isize>>> {
        let mut by_constraint: ByField<ByConstraint<Diff<isize>>> = BTreeMap::default();
        for breach in &self.current {
            let by_field = by_constraint.entry(breach.field.clone()).or_default();
            let count = by_field.entry(breach.constraint.clone()).or_default();
            count.current += 1;
        }
        if let Some(previous) = &self.previous {
            for breach in previous {
                let by_field = by_constraint.entry(breach.field.clone()).or_default();
                let count = by_field.entry(breach.constraint.clone()).or_default();
                if let Some(count) = &mut count.previous {
                    *count += 1;
                }
            }
        }
        by_constraint
    }
}
