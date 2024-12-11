/// Describe & validate any constraints on fields for this type.
pub trait Constrained {
    /// Return a string identifying this instance.
    fn id(&self) -> String;

    fn validate(&self) -> Vec<InvalidValue>;
}

/// Describe an invalid value.
#[derive(Debug)]
pub struct InvalidValue {
    /// The field that is invalid.
    pub field: &'static str,

    /// The provided value.
    pub value: String,

    /// Describe the violated constraint.
    pub constraint: String,
}

// TODO
// Macro for conveniently defining a [`Constraint`].
// #[macro_export]
// macro_rules! constraint {
//     (>$val: expr) => {
//         $crate::draw::Constraint::LowerBound($val)
//     };
//     (>=$val: expr) => {
//         $crate::draw::Constraint::LowerBoundInclusive($val)
//     };
//     (<$val: expr) => {
//         $crate::draw::Constraint::UpperBound($val)
//     };
//     (<=$val: expr) => {
//         $crate::draw::Constraint::UpperBoundInclusive($val)
//     };
//     ($val_a: expr => $val_b: expr) => {
//         $crate::draw::Constraint::RangeInclusive($val_a, $val_b)
//     }; // ($val_a: expr => $val_b: expr) => {
//        //     $crate::draw::Constraint::Range($val_a, $val_b)
//        // };
// }
