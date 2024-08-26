//! [`Recorder`]s are used to track data for
//! plotting or saving for later analysis.
//!
//! Recorders are associated with an entity or a group of entities
//! in the model state and record their values (or a value derived from them)
//! at each time step.
//!
//! The [`recordables`](crate::recordables) macro should be used to define new recorders.

use std::path::Path;

use crate::file::write_csv;
use serde::Serialize;

/// This trait indicates that something has a name.
pub trait Named {
    fn name(&self) -> String;
}

/// This trait is required to process the model's current state
/// into a form that recorders can use.
pub trait Snapshot {
    /// Associated "global" data which is made available to all recorders.
    /// Otherwise recorders only access the entity/entities they are directly concerned with.
    type Context;
    fn get_context(&self) -> &Self::Context;
}

/// This trait is used to extract (borrow)
/// a type `T` from a [`Snapshot`].
///
/// For example, you have implemented `Snapshot`
/// on some struct with a field `income: f32`.
/// To extract the data you'd implement `SnapshotGet<f32>`
/// such that it returns `&self.income`.
///
/// A notable restriction here is that you can only implement
/// the trait once for each type, so in this example if there
/// are other `f32` fields you want to extract, you wouldn't be able
/// to. Thus it's suggested that you have wrapper types, e.g.
/// `struct Income(f32)`, to differentiate.
pub trait SnapshotGet<T> {
    fn get(&self) -> &T;
}

/// A macro for quickly implementing [`SnapshotGet`]
/// over a several different fields of a [`Snapshot`] struct.
#[macro_export]
macro_rules! snapshot_props {
    ($snapshot:ty, { $($ret:ty:$field:ident),* }) => {
        use $crate::record::SnapshotGet;
        $(impl SnapshotGet<$ret> for $snapshot {
            fn get(&self) -> &$ret {
                &self.$field
            }
        })*
    }
}

/// Implement this trait to define a new recorder.
/// You should use [`recordables`](crate::recordables) macro
/// instead of implementing this manually.
pub trait Recorder {
    /// Get the name of the recorder.
    fn name(&self) -> &'static str;

    /// Get the units of the recorder.
    fn units(&self) -> &'static str;

    /// Borrow the recorded data.
    fn data(&self) -> &RecorderData;
}

/// A macro to define recorders, which are associated with a particular
/// type that it extracts data from.
///
/// Usage:
///
///     recordables!(
///         singleton:InvestEvent => {
///             NewInvestmentSum: self {
///                 self.investments.values().sum()
///             }
///         });
///
/// Where `InvestEvent` is a struct defined elsewhere.
/// This will define a variant on [`Recorder`], named `NewInvestmentSum`,
/// which records the sum of `InvestEvent.investments`.
///
/// Note that you must prefix the type name `T` with either:
///
/// - `singleton`: indicating only one of `T` is expected.
/// - `entity`: indicating we expect a collection of `T`,
///     and we record data for each instance individually.
/// - `group`: indicating we expect a collection of `T`, and we
///     record data over the entire collection.
///
/// Note that [`Snapshot`] must implement [`SnapshotGet<T>`] as well, so
/// we know how to get `T` from a model snapshot.
///
/// This macro also defines a constant `RECORDERS` which contains the names
/// of all the registered recorders.
#[macro_export]
macro_rules! recordables {
    ($snapshot:ty, $($kind:ident:$target:ty => { $($name:ident: $units:literal ($sel:ident, $ctx:ident) -> $ret:ty => $extract:expr ),* $(,)? }),* $(,)?) => {
        use $crate::paste::paste;
        use $crate::record::{
            init_singleton_data,
            init_entity_data,
            init_group_data,
            update_singleton_recorder,
            update_entity_recorder,
            update_group_recorder,
            Snapshot as SnapshotTrait,
            Recorder as RecorderTrait,
            RecorderData};

        // Generate doc listing out all available recorders.
        #[doc = concat!(
            "The following recorders are defined:\n\n",
            $(concat!("* ", stringify!($target), "\n", $(concat!("    * ", stringify!($name), "\n")),*)),*
        )]
        pub const RECORDERS: &[&str] = &[
            $($(stringify!($name)),*),*
        ];

        pub enum Recorder {
            $($($name(RecorderData)),*),*
        }
        impl RecorderTrait for Recorder {
            fn name(&self) -> &'static str {
                match self {
                    $($(Self::$name(_) => stringify!($name)),*),*
                }
            }

            fn units(&self) -> &'static str {
                match self {
                    $($(Self::$name(_) => $units),*),*
                }
            }

            fn data(&self) -> &RecorderData {
                match self {
                    $($(Self::$name(rec) => rec),*),*
                }
            }
        }
        impl Recorder {
            paste! {
                pub fn record(&mut self, snapshot: &$snapshot) {
                    match self {
                        $($(Self::$name(rec) => {
                            [<update_ $kind _recorder>](rec, snapshot, |$sel: &$target, $ctx: &<$snapshot as SnapshotTrait>::Context| -> $ret { $extract });
                        }),*),*
                    }
                }
            }

            pub fn data_mut(&mut self) -> &mut RecorderData {
                match self {
                    $($(Self::$name(rec) => rec),*),*
                }
            }
        }
        impl From<&str> for Recorder {
            paste! {
                fn from(value: &str) -> Self {
                    match value {
                        $($(stringify!($name) => {
                            let data = [<init_ $kind _data>]::<$ret>();
                            Self::$name(data)
                        }),*),*,
                        _ => panic!("Unrecognized recorder name: {}", value)
                    }
                }
            }
        }
    };
}

#[derive(Serialize)]
struct Record<'a> {
    year: u16,
    facet: String,
    value: f32,
    units: &'a str,
}

/// An enum representing different types of recorder data.
/// This lets us avoid dealing with generics,
/// which lets us treat [`Recorder`]s as fungible (way more convenient).
///
/// A recorder is associated with a specific type of data
/// which implements [`RecorderDataType`].
/// This data is what's saved each frame.
pub enum RecorderData {
    /// A single timeseries.
    Single(Vec<f32>),

    /// Multiple labeled time series.
    Multiple(Vec<Vec<(String, f32)>>),
}
impl RecorderData {
    pub fn save_csv(&self, start_year: u16, units: &str, path: &Path) {
        let records: Vec<Record> = match self {
            Self::Single(data) => data
                .iter()
                .enumerate()
                .map(|(i, val)| Record {
                    year: start_year + i as u16,
                    facet: "".into(),
                    value: *val,
                    units,
                })
                .collect(),
            Self::Multiple(data) => data
                .iter()
                .enumerate()
                .flat_map(|(i, vals)| {
                    vals.iter().map(move |(label, val)| Record {
                        year: start_year + i as u16,
                        facet: label.to_string(),
                        value: *val,
                        units,
                    })
                })
                .collect(),
        };
        write_csv(path, &records);
    }
}

/// This trait is necessary so that recordable data
/// is placed into the correct [`RecorderData`] variant.
///
/// This trait is pre-implemented for common data types,
/// e.g. `f32`, and `Vec<(String, f32)>` (labeled data).
pub trait RecorderDataType {
    fn new_data() -> RecorderData;
    fn append_to(self, data: &mut RecorderData);
}

impl RecorderDataType for f32 {
    fn new_data() -> RecorderData {
        RecorderData::Single(Vec::default())
    }

    fn append_to(self, data: &mut RecorderData) {
        match data {
            RecorderData::Single(vals) => vals.push(self),
            _ => panic!("RecorderDataType -> RecorderData mismatch"),
        }
    }
}
impl RecorderDataType for Vec<(String, f32)> {
    fn new_data() -> RecorderData {
        RecorderData::Multiple(Vec::default())
    }

    fn append_to(self, data: &mut RecorderData) {
        match data {
            RecorderData::Multiple(vals) => vals.push(self),
            _ => panic!("RecorderDataType -> RecorderData mismatch"),
        }
    }
}

/// Initialize new singleton data.
#[doc(hidden)]
pub fn init_singleton_data<T: RecorderDataType>() -> RecorderData {
    T::new_data()
}

/// Initialize new group data.
#[doc(hidden)]
pub fn init_group_data<T: RecorderDataType>() -> RecorderData {
    T::new_data()
}

/// Initialize new entity data.
#[doc(hidden)]
pub fn init_entity_data<T: RecorderDataType>() -> RecorderData
where
    Vec<(String, T)>: RecorderDataType,
{
    Vec::<(String, T)>::new_data()
}

/// Collects the recorded data for a singleton (i.e. one datapoint per step)
/// of this step.
pub fn update_singleton_recorder<
    S: Snapshot + SnapshotGet<E>,
    E,
    T: RecorderDataType,
>(
    data: &mut RecorderData,
    snapshot: &S,
    mut extract_fn: impl FnMut(&E, &S::Context) -> T,
) {
    let singleton = SnapshotGet::<E>::get(snapshot);
    extract_fn(singleton, snapshot.get_context()).append_to(data);
}

/// Collects the recorded data from a collection of entities, e.g.
/// if we have `n` entities then `n` datapoints will be generated per
/// step, effectively producing `n` timeseries.
///
/// Note that the actual data is `Vec<(String, T)>` and not `T`
/// as with singletons, because we'll have one
/// labeled data point per component.
///
/// # Warning
///
/// **This requires that the recorded type implement the [`Named`] trait**
/// --this is required for labeling the recorded data points.
///
/// # Optimization
///
/// Because entity names don't have static lifetimes
/// we have to store them as `String` rather references.
/// This *could* have performance implications. If so,
/// one possibility is to have the names be cached
/// in a static lookup table, thus allowing us to convert
/// te names' `&str` to `&'static str`.
pub fn update_entity_recorder<
    S: Snapshot + SnapshotGet<Vec<C>>,
    C: Named,
    T: RecorderDataType,
>(
    data: &mut RecorderData,
    snapshot: &S,
    mut extract_fn: impl FnMut(&C, &S::Context) -> T,
) where
    Vec<(String, T)>: RecorderDataType,
{
    let entities = SnapshotGet::<Vec<C>>::get(snapshot);
    let records: Vec<_> = entities
        .iter()
        .map(|comp| {
            (
                <C as Named>::name(comp),
                extract_fn(comp, snapshot.get_context()),
            )
        })
        .collect();
    records.append_to(data);
}

/// Collect data over a collection of entities all together,
/// rather than individually.
///
/// Like the singleton recorder this gives us one datapoint per step.
pub fn update_group_recorder<
    S: Snapshot + SnapshotGet<G>,
    G,
    T: RecorderDataType,
>(
    data: &mut RecorderData,
    snapshot: &S,
    mut extract_fn: impl FnMut(&G, &S::Context) -> T,
) {
    let group = SnapshotGet::<G>::get(snapshot);
    extract_fn(group, snapshot.get_context()).append_to(data);
}
