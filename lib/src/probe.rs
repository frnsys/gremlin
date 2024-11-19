//! Some plumbing to help debugging.
//!
//! Register sentinel functions that, when triggered,
//! log a message and the item that triggered it.
//! Then use the `probe!` macro throughout your code to
//! run the sentinels.

use std::{
    any::{Any, TypeId},
    ops::Deref,
    sync::{Mutex, OnceLock},
};

use ahash::HashMap;

type Registry = Mutex<HashMap<TypeId, Box<dyn Any + Send>>>;

pub struct Probe<T: std::fmt::Debug + Send + 'static> {
    sentinel: Box<dyn Fn(&T) -> Option<String> + Send>,
}
impl<T: std::fmt::Debug + Send + 'static> Probe<T> {
    fn get_registry_map() -> &'static Registry {
        static REGISTRY: OnceLock<Registry> = OnceLock::new();
        REGISTRY.get_or_init(|| Mutex::new(HashMap::default()))
    }

    pub fn register_probe<F: Fn(&T) -> Option<String> + Send + 'static>(func: F) {
        let map = Self::get_registry_map();
        let mut lock = map.lock().unwrap();
        let mut probes = lock
            .entry(TypeId::of::<T>())
            .or_insert_with(|| Box::new(Mutex::new(Vec::<Probe<T>>::default())))
            .downcast_ref::<Mutex<Vec<Probe<T>>>>()
            .unwrap()
            .lock()
            .unwrap();
        probes.push(Probe {
            sentinel: Box::new(func),
        });
    }

    pub fn probe(tag: &str, subjects: &[T]) {
        let map = Self::get_registry_map();
        let lock = map.lock().unwrap();
        if let Some(probes) = lock
            .get(&TypeId::of::<T>())
            .map(|entry| entry.downcast_ref::<Mutex<Vec<Probe<T>>>>().unwrap())
        {
            let probes = probes.lock().unwrap();
            // let type_name = std::any::type_name::<T>();
            for probe in probes.iter() {
                for subject in subjects {
                    if let Some(msg) = probe.sentinel.deref()(subject) {
                        tracing::info!("[PROBE:{tag}] {msg}\n{subject:?}");
                    }
                }
            }
        }
    }
}

#[macro_export]
macro_rules! probe {
    ($tag:literal, $subjects:expr) => {
        if cfg!(debug_assertions) {
            $crate::probe::Probe::probe($tag, $subjects);
        }
    };
}
