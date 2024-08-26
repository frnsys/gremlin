use ahash::HashMap;
use extend::ext;
use rayon::iter::{ParallelBridge, ParallelIterator};
use std::hash::Hash;

/// Trait providing a method to group an iterator's
/// items by some key function.
pub trait Rows: Iterator {
    /// Group rows according to the provided key function.
    /// Rows that return the same key will be grouped together.
    fn group_by<K: Hash + Eq>(
        self,
        key_fn: impl Fn(&Self::Item) -> K,
    ) -> HashMap<K, Vec<Self::Item>>
    where
        Self: Sized,
    {
        let mut map: HashMap<K, Vec<Self::Item>> = HashMap::default();
        for (key, row) in self
            .map(|row| {
                let key = key_fn(&row);
                (key, row)
            })
            .collect::<Vec<(K, Self::Item)>>()
        {
            map.entry(key).or_default().push(row);
        }
        map
    }
}
impl<I: Iterator> Rows for I {}

/// Provides methods for filtering, mapping,
/// and merging [`HashMap`]s.
#[ext(name=HashMapExt)]
pub impl<K: Hash + Eq, V> HashMap<K, V> {
    /// Filter out groups that fail to satisfy the provided predicate.
    fn filter(
        self,
        predicate: impl Fn(&(K, V)) -> bool,
    ) -> HashMap<K, V> {
        self.into_iter().filter(predicate).collect()
    }

    /// Apply a function to each group, returning a new `Groups`
    /// mapping the keys to the function's output.
    fn map<U>(self, f: impl Fn(&K, V) -> U) -> HashMap<K, U> {
        self.into_iter()
            .map(|(key, val)| {
                let mapped = f(&key, val);
                (key, mapped)
            })
            .collect()
    }

    /// Merge two groups together.
    /// This consumes the other group.
    ///
    /// The `merger` function takes a key,
    /// the mutable reference of the value from this group A,
    /// and an owned value from the other group B.
    ///
    /// Note that if key `K` is present in group B but not
    /// in group A, we just move the value from B into A.
    fn merge(
        mut self,
        other: HashMap<K, V>,
        merger: impl Fn(&K, &mut V, V),
    ) -> HashMap<K, V> {
        for (k, other_val) in other.into_iter() {
            // instead of default, check if present in
            // either this or other, and use other if needed
            if let Some(v) = self.get_mut(&k) {
                merger(&k, v, other_val);
            } else {
                self.insert(k, other_val);
            }
        }
        self
    }
}

/// Specific functionality for groups where the values are `Vec`s.
#[ext(name=HashMapVecExt)]
pub impl<K: Hash + Eq, V> HashMap<K, Vec<V>> {
    /// Apply a map function over each group's members.
    fn map_members<U: Default>(
        self,
        f: impl Fn(&K, V) -> U + Send + Sync,
    ) -> HashMap<K, Vec<U>> {
        self.map(|k, vs| vs.into_iter().map(|v| f(k, v)).collect())
    }
}

#[doc(hidden)]
pub trait Float {
    fn to_f32(&self) -> f32;
}
impl Float for f32 {
    fn to_f32(&self) -> f32 {
        *self
    }
}
impl Float for &f32 {
    fn to_f32(&self) -> f32 {
        **self
    }
}

/// Extension functions for `dyn Iterator<Item = f32>`.
pub trait IterExt<T: Float>: Iterator<Item = T> {
    fn mean(self) -> f32
    where
        Self: Sized,
    {
        let mut n = 0;
        let mut sum = 0.;
        for v in self {
            n += 1;
            sum += v.to_f32();
        }
        sum / n as f32
    }

    fn mean_or(self, default_fn: impl FnOnce() -> f32) -> f32
    where
        Self: Sized,
    {
        let mean = self.mean();
        if mean.is_nan() {
            default_fn()
        } else {
            mean
        }
    }

    /// Get the minimum element.
    fn min(self) -> f32
    where
        Self: Sized,
    {
        self.fold(f32::INFINITY, |a, b| a.min(b.to_f32()))
    }

    /// Get the maximum element.
    fn max(self) -> f32
    where
        Self: Sized,
    {
        self.fold(-f32::INFINITY, |a, b| a.max(b.to_f32()))
    }
}
impl<T, F: Float> IterExt<F> for T where T: Iterator<Item = F> {}

/// Methods for getting the min/max of a `Vec<f32>`.
#[ext]
pub impl Vec<f32> {
    /// Get the minimum element.
    fn min(&self) -> f32
    where
        Self: Sized,
    {
        self.iter().fold(f32::INFINITY, |a, b| a.min(*b))
    }

    /// Get the maximum element.
    fn max(&self) -> f32
    where
        Self: Sized,
    {
        self.iter().fold(-f32::INFINITY, |a, b| a.max(*b))
    }
}

/// Map-reduce on an iterator.
pub fn mapreduce<T: Send + Sync, U: Send + Sync, V: Send>(
    iter: impl Iterator<Item = T> + Send + ParallelBridge,
    map: impl Fn(T) -> U + Send + Sync,
    reduce: impl Fn(Vec<U>) -> V + Send + Sync,
) -> V {
    let to_reduce: Vec<U> =
        iter.par_bridge().map(|chunk| map(chunk)).collect();
    reduce(to_reduce)
}
