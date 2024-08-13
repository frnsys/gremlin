//! Documentation helpers.

use crate::core::AnnualSeries;

/// This trait is meant to list the columns
/// and their expected types for a type `T`
/// that will be serialized to and deserialized from
/// a flattened CSV.
///
/// ## Caveats
///
/// There are a number of shortcomings with the current method.
/// Type reflection via e.g. `bevy_reflect` is insufficient because
/// it is not implemented for foreign types. This may be a tolerable
/// constraint but would require some re-working for structs that
/// include foreign types. `bevy_reflect` doesn't respect serde
/// attributes (e.g. `#[serde(flatten)]`) but we might be able to
/// work around that given that its `Reflect` trait lets us use
/// property paths, e.g. `"costs.fixed_om"`, in a way similar to
/// how we are flattening fields with `serde`. We would have to ensure
/// that the `serde`-flattened fields have a consistent path with the
/// reflected field paths (e.g. for a field `costs: Costs` where `Costs` has
/// a field `fixed_om`, the reflected path would be `costs.fixed_om` and so we
/// need to ensure that the `serde`-flattened field name is also `costs.fixed_om`.
/// This also means that we can't use `#[serde(rename="...")]`.
///
/// There is also the `serde_reflection` crate but it's long been unmaintained,
/// and doesn't support `#[serde(flatten)]`.
pub trait HasSchema {
    /// Returns `(name, type, description)`.
    fn schema() -> Vec<(String, &'static str, String)>;
}

impl HasSchema for f32 {
    fn schema() -> Vec<(String, &'static str, String)> {
        vec![(
            "value".to_string(),
            "f32",
            "Float value".to_string(),
        )]
    }
}

impl<N> HasSchema for AnnualSeries<N> {
    fn schema() -> Vec<(String, &'static str, String)> {
        vec![
            ("year".to_string(), "u16", "The year".to_string()),
            (
                "value".to_string(),
                std::any::type_name::<N>(),
                "The value".to_string(),
            ),
        ]
    }
}

/// Use for enums to extract documentation about
/// their variants.
pub trait HasVariants {
    fn describe_variants() -> Vec<(String, String)>;
}
