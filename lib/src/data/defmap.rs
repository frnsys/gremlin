//! A map with default values.

use std::{
    collections::BTreeMap,
    ops::{Deref, DerefMut, Index, IndexMut},
};

pub struct DefMap<K, T>
where
    T: Clone,
{
    map: BTreeMap<K, T>,
    default: T,
}

impl<K, T: Clone> DefMap<K, T> {
    pub fn with_default(value: T) -> Self {
        Self {
            map: BTreeMap::default(),
            default: value,
        }
    }
}

impl<K, T: Clone> Deref for DefMap<K, T> {
    type Target = BTreeMap<K, T>;

    fn deref(&self) -> &Self::Target {
        &self.map
    }
}

impl<K, T: Clone> DerefMut for DefMap<K, T> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.map
    }
}

impl<K: Ord, T: Clone> Index<&K> for DefMap<K, T> {
    type Output = T;

    fn index(&self, index: &K) -> &Self::Output {
        self.get(index).unwrap_or(&self.default)
    }
}
impl<K: Ord, T: Clone> Index<K> for DefMap<K, T> {
    type Output = T;

    fn index(&self, index: K) -> &Self::Output {
        self.get(&index).unwrap_or(&self.default)
    }
}

impl<K: Ord, T: Clone> IndexMut<K> for DefMap<K, T> {
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
    fn test_defmap() {
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

        let mut map = DefMap::with_default(0.0);
        map[MyKey::A] = 1.0;

        assert_eq!(map[MyKey::A], 1.0);
        assert_eq!(map[MyKey::B], 0.0);
        assert_eq!(map[MyKey::C(MySubKey::Z)], 0.0);
    }
}