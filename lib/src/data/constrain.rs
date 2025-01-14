/// Describe & validate any constraints on fields for this type.
pub trait Constrained {
    fn validate(&self) -> Vec<Breach>;
}

// NOTE: An improvement here is to have a trait Constrainable<C>
// which means that the implementing type T can be constrained
// by a value of type C. So we could have ByDayHour constrainable by
// an f32, which takes the mean of the ByDayHour.

#[derive(Debug, Clone)]
pub enum Constraint {
    GreaterThan(f32),
    LessThan(f32),
    GreaterEqualThan(f32),
    LessEqualThan(f32),
    Within((f32, f32)),
    WithinInclusive((f32, f32)),
}
impl std::fmt::Display for Constraint {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{}",
            match self {
                Self::GreaterThan(val) => format!(">{val}"),
                Self::LessThan(val) => format!("<{val}"),
                Self::GreaterEqualThan(val) => format!(">={val}"),
                Self::LessEqualThan(val) => format!("<={val}"),
                Self::Within((a, b)) => format!("∈[{a}, {b})"),
                Self::WithinInclusive((a, b)) => format!("∈[{a}, {b}]"),
            }
        )
    }
}
impl Constraint {
    pub fn validate(&self, value: f32) -> bool {
        match self {
            Self::GreaterThan(val) => value > *val,
            Self::LessThan(val) => value < *val,
            Self::GreaterEqualThan(val) => value >= *val,
            Self::LessEqualThan(val) => value <= *val,
            Self::Within((a, b)) => value >= *a && value < *b,
            Self::WithinInclusive((a, b)) => value >= *a && value <= *b,
        }
    }
}

/// Describe an invalid value.
#[derive(Debug, Clone)]
pub struct Breach {
    /// The field that is invalid.
    pub field: String,

    /// The provided value.
    // NOTE: This is a string for now just
    // for more flexibility, but a more generic system
    // might be better.
    pub value: String,

    /// Describe the violated constraint.
    pub constraint: String,
}
