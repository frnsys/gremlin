use extend::ext;
use std::{collections::HashMap, hash::Hash};

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
        let mut map: HashMap<K, Vec<Self::Item>> =
            HashMap::default();
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
}

/// Specific functionality for groups where the values are `Vec`s.
#[ext(name=HashMapVecExt)]
pub impl<K: Hash + Eq, V> HashMap<K, Vec<V>> {
    /// Apply a map function over each group's members.
    fn map_members<U: Default>(
        self,
        f: impl Fn(&K, V) -> U + Send + Sync,
    ) -> HashMap<K, Vec<U>> {
        self.map(|k, vs| {
            vs.into_iter().map(|v| f(k, v)).collect()
        })
    }
}
