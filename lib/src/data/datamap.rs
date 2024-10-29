//! A map with default values.

use std::{
    collections::BTreeMap,
    ops::{Deref, DerefMut, Index, IndexMut},
};

use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct DataMap<K: Ord, T>
where
    T: Clone,
{
    map: BTreeMap<K, T>,
    default: T,
}

impl<K: Ord, T: Clone + Default> Default for DataMap<K, T> {
    fn default() -> Self {
        Self {
            map: BTreeMap::default(),
            default: T::default(),
        }
    }
}

impl<K: Ord, T: Clone> DataMap<K, T> {
    pub fn with_default(value: T) -> Self {
        Self {
            map: BTreeMap::default(),
            default: value,
        }
    }
}

impl<K: Ord, T: Clone> Deref for DataMap<K, T> {
    type Target = BTreeMap<K, T>;

    fn deref(&self) -> &Self::Target {
        &self.map
    }
}

impl<K: Ord, T: Clone> DerefMut for DataMap<K, T> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.map
    }
}

impl<K: Ord, T: Clone> Index<&K> for DataMap<K, T> {
    type Output = T;

    fn index(&self, index: &K) -> &Self::Output {
        self.get(index).unwrap_or(&self.default)
    }
}
impl<K: Ord, T: Clone> Index<K> for DataMap<K, T> {
    type Output = T;

    fn index(&self, index: K) -> &Self::Output {
        self.get(&index).unwrap_or(&self.default)
    }
}

impl<K: Ord, T: Clone> IndexMut<K> for DataMap<K, T> {
    fn index_mut(&mut self, index: K) -> &mut Self::Output {
        self.map
            .entry(index)
            .or_insert_with(|| self.default.clone())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_datamap() {
        #[derive(PartialEq, Eq, PartialOrd, Ord)]
        enum MyKey {
            A,
            B,
            C(MySubKey),
        }

        #[derive(PartialEq, Eq, PartialOrd, Ord)]
        enum MySubKey {
            X,
            Y,
            Z,
        }

        let mut map = DataMap::with_default(0.0);
        map[MyKey::A] = 1.0;

        assert_eq!(map[MyKey::A], 1.0);
        assert_eq!(map[MyKey::B], 0.0);
        assert_eq!(map[MyKey::C(MySubKey::Z)], 0.0);
    }
}
