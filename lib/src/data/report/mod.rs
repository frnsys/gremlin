mod diff;
mod util;
mod view;

pub use diff::Diff;
pub use view::rows_html;

use super::{StepResult, Strictness, VarReferences};

#[derive(Default, Clone)]
pub struct RiverReport {
    pub name: String,
    pub n_invalid: usize,
    pub n_incomplete: usize,
    pub strictness: Strictness,
    pub tributaries: Vec<String>,
    pub play_by_play: Vec<StepResult>,
    pub references: VarReferences,
}
