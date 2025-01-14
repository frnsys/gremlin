use std::{cmp::Ord, collections::BTreeMap};

/// Percent difference of `b` vs `a`.
pub(super) fn per_diff(a: f32, b: f32) -> f32 {
    ((b - a) / a) * 100.0
}

pub(super) fn sum_inner<T: Ord, U: Ord>(
    map: &BTreeMap<T, BTreeMap<U, usize>>,
) -> BTreeMap<&T, usize> {
    map.iter()
        .map(|(key, inner)| (key, inner.values().sum()))
        .collect()
}

pub(super) fn transpose_sum_inner<T: Ord, U: Ord>(
    map: &BTreeMap<T, BTreeMap<U, usize>>,
) -> BTreeMap<&U, usize> {
    let mut transposed: BTreeMap<&U, BTreeMap<&T, usize>> = BTreeMap::default();
    for (o_key, inner) in map {
        for (i_key, value) in inner {
            let t_inner = transposed.entry(i_key).or_default();
            let current = t_inner.entry(o_key).or_default();
            *current += value;
        }
    }

    transposed
        .iter()
        .map(|(key, inner)| (*key, inner.values().sum()))
        .collect()
}

/// Transpose a `Map<A, Map<B, C>>`
/// to `Map<B, Map<A, C>>`.
pub(super) fn transpose<T: Ord + Clone, U: Ord, V>(
    map: BTreeMap<T, BTreeMap<U, V>>,
) -> BTreeMap<U, BTreeMap<T, V>> {
    let mut transposed = BTreeMap::default();
    for (outer, map) in map {
        for (inner, value) in map {
            let t_inner: &mut BTreeMap<T, V> = transposed.entry(inner).or_default();
            t_inner.insert(outer.clone(), value);
        }
    }
    transposed
}
