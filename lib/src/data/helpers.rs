//! Deserialization helpers.

pub use csv::invalid_option;
use serde::{
    de::{self, IntoDeserializer, Unexpected},
    Deserialize,
    Deserializer,
};

use crate::core::Unit;

/// Deserialize a list that's a string, e,g, `"A, B, C"`,
/// into a list of a type, e.g. `[MyEnum::A, MyEnum::B, MyEnum::C]`.
pub fn deserialize_delimited_list<
    'de,
    D,
    T: Deserialize<'de>,
    const S: char,
>(
    deserializer: D,
) -> Result<Vec<T>, D::Error>
where
    D: Deserializer<'de>,
{
    let string: String = Deserialize::deserialize(deserializer)?;
    let mut vals = vec![];
    for s in string.split(S) {
        let s = s.trim();
        if s.is_empty() {
            continue;
        }
        let val = T::deserialize(s.into_deserializer())?;
        vals.push(val);
    }
    Ok(vals)
}

/// Deserialize a `usize` for which the value `-999999` indicates a missing or N/A value.
pub fn deserialize_nullable_usize<'de, D>(
    deserializer: D,
) -> Result<Option<usize>, D::Error>
where
    D: Deserializer<'de>,
{
    let val: isize = Deserialize::deserialize(deserializer)?;
    if val == -999999 {
        Ok(None)
    } else {
        Ok(Some(val as usize))
    }
}

/// Deserialize a `f32` for which the value `-999999` indicates a missing or N/A value.
pub fn deserialize_nullable_f32<'de, D>(
    deserializer: D,
) -> Result<Option<f32>, D::Error>
where
    D: Deserializer<'de>,
{
    let val: f32 = Deserialize::deserialize(deserializer)?;
    if val == -999999. {
        Ok(None)
    } else {
        Ok(Some(val))
    }
}

/// Deserialize a nullable [`Unit`] value.
pub fn deserialize_nullable_unit<'de, D, U: Unit>(
    deserializer: D,
) -> Result<Option<U>, D::Error>
where
    D: Deserializer<'de>,
{
    let raw: String = Deserialize::deserialize(deserializer)?;
    if let Ok(val) = raw.parse::<f32>() {
        Ok(Some(val.into()))
    } else if raw == "NA" {
        Ok(None)
    } else {
        Err(de::Error::invalid_value(
            Unexpected::Str(&raw),
            &"float or NA",
        ))
    }
}
