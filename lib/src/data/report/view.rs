use std::collections::BTreeMap;

use crate::data::{
    profile::VarProfile, ByConstraint, ByFacet, ByField, Constrained, Dataset, FacetVarReferences,
    Row, StepResult, VarReferences,
};

use super::{util, Diff, RiverReport};
use itertools::Itertools;
use maud::html;
use thousands::Separable;

const CSS: &'static str = include_str!("assets/report.css");
const JS: &'static str = include_str!("assets/report.js");

const ERROR_SCALE: &[&str] = &[
    "#bbbbbb", "#bbbbbb", "#ffdf5f", "#fdcf50", "#fbbe43", "#f8ad37", "#f59b2e", "#f18a27",
    "#ed7722", "#e86420", "#e34e20", "#dd3522",
];

/// Get color for an error, expected as a percentage.
fn error_color(error: f32) -> &'static str {
    let idx = (error.abs() / 100. * ERROR_SCALE.len() as f32).floor() as usize;
    ERROR_SCALE[idx.min(ERROR_SCALE.len() - 1)]
}

pub fn report_css() -> maud::Markup {
    html! {
        style { (maud::PreEscaped(CSS)) }
    }
}

pub fn report_js() -> maud::Markup {
    html! {
        script type="text/javascript" { (maud::PreEscaped(JS)) }
    }
}

impl RiverReport {
    pub fn as_html(&self) -> maud::Markup {
        html! {
            style { (maud::PreEscaped(CSS)) }
            .river-report {
                h2 { (self.name) }
                div {
                    "Strictness:" (self.strictness)
                }
                div {
                    "Invalid:" (self.n_invalid)
                }
                div {
                    "Incomplete:" (self.n_incomplete)
                }
                .tributaries {
                    h3 { "Tributaries" }
                    ul {
                        @for tributary in &self.tributaries {
                            li { (tributary) }
                        }
                    }
                }
                .replay {
                    h3 { "Play-by-Play" }
                    @for step in &self.play_by_play {
                        (step.as_html(&self.references))
                    }
                }
            }
            script type="text/javascript" { (maud::PreEscaped(JS)) }
        }
    }
}

impl StepResult {
    pub fn as_html(&self, refs: &VarReferences) -> maud::Markup {
        let var_changes_by_facet = self.outputs.count_changes();
        let changes_by_facet = util::sum_inner(&var_changes_by_facet);
        let changes_by_var = util::transpose_sum_inner(&var_changes_by_facet);
        let changed_vars = changes_by_var
            .iter()
            .filter(|(_, count)| **count > 0)
            .map(|(var, count)| format!("{} ({})", var, count))
            .join(", ");

        let total_changes = changes_by_facet.values().sum::<usize>();
        let facet_changes = changes_by_facet
            .values()
            .filter(|count| **count > 0)
            .count();

        html! {
            .step-result {
                .step-header {
                    .step-info {
                        .step-title { (format!("{}: {}", self.num+1, self.name)) }
                        .step-summary {
                            @if total_changes == 0 {
                                "No changes identified"
                            } @else {
                                (format!("{} identified changes across {} facets", total_changes, facet_changes))
                            }
                        }
                    }
                    .step-controls {
                        .button.step-toggle-refs.focused {
                            "Show Refs"
                        }
                        .step-toggle {
                            .button.step-toggle-input {
                                "Input"
                            }
                            .button.step-toggle-output.focused {
                                "Output"
                            }
                        }
                    }
                }
                @if total_changes > 0 {
                    .step-changes {
                        (format!("Changed: {}", changed_vars))
                    }
                }
                .data-table {
                    .input-data {
                        (diffed_profiles_html(&Diff::new(self.inputs.clone()), refs))
                    }
                    .output-data {
                        (diffed_profiles_html(&self.outputs, refs))
                    }
                }
                .step-errors {
                    div {
                        (format!("Incomplete: {} ", self.incomplete))
                            (error_change(self.incomplete.change()))
                    }
                    @if !self.invalid.is_empty() {
                        .step-breaches {
                            "Constraint Breaches:"
                                @for (field, errors) in &self.invalid {
                                    .step-breaches-field {
                                        div { (format!("{}: ", field)) }
                                        @for (error, diff) in errors {
                                            .step-breach-error {
                                                span {
                                                    (error)
                                                }
                                                span {
                                                    (format!("{}", diff))
                                                }
                                                (error_change(diff.change()))
                                            }
                                        }
                                    }
                                }
                        }
                    }
                }
            }
        }
    }
}

fn value_col(missing: bool, value: f32) -> maud::Markup {
    html! {
        @if missing {
            (empty_col())
        } @else if value.is_nan() {
            td.data-nan {
                "NaN!"
            }
        } @else {
            (float_col(value))
        }
    }
}

fn float_col(value: f32) -> maud::Markup {
    html! {
        td title=(value.to_string()) {
            (format!("{:.3}", value))
        }
    }
}

fn count_col(value: isize) -> maud::Markup {
    html! {
        td {
            (format!("{}", value))
        }
    }
}

fn field_row(name: &str, current: &VarProfile, prev: Option<&VarProfile>) -> maud::Markup {
    macro_rules! diff_col {
        ($summary:ident.$field:ident) => {
            prev.as_ref().map_or_else(empty_col, |prev| {
                diff_col(current.$summary.$field, prev.$summary.$field)
            })
        };
    }

    macro_rules! count_change_col {
        ($summary:ident.$field:ident) => {
            prev.as_ref().map_or_else(empty_col, |prev| {
                count_change_col(current.$summary.$field, prev.$summary.$field)
            })
        };
    }

    html! {
        tr {
            td title=(name) { (name) }
            (value_col(current.missing.all(), current.summary.mean))
            (diff_col!(summary.mean))
            (value_col(current.missing.all(), current.summary.min))
            (diff_col!(summary.min))
            (value_col(current.missing.all(), current.summary.max))
            (diff_col!(summary.max))
            td { (current.summary.histogram) }
            (count_col(current.missing.count))
            (count_change_col!(missing.count))
            (count_col(current.negative.count))
            (count_change_col!(negative.count))
            (count_col(current.zero.count))
            (count_change_col!(zero.count))
            (count_col(current.infinite.count))
            (count_change_col!(infinite.count))
        }
    }
}

fn empty_col() -> maud::Markup {
    html! {
        td.data-empty {
            "---"
        }
    }
}

fn diff_col(cur: f32, prev: f32) -> maud::Markup {
    let diff = cur - prev;
    html! {
        @if diff.is_nan() {
            @if cur.is_nan() {
                (empty_col())
            } @else {
                td.change-new {
                    "new"
                }
            }
        } @else if diff == 0. {
            td.change-none title=(diff) {
                (format!("{:+.2}", diff))
            }
        } @else if diff > 0. {
            td.change-up title=(diff) {
                (format!("{:+.2}", diff))
            }
        } @else {
            td.change-down title=(diff) {
                (format!("{:+.2}", diff))
            }
        }
    }
}

fn count_change_span(cur: isize, prev: isize) -> maud::Markup {
    let diff = cur - prev;
    html! {
        @if diff == 0 {
            span.change-none {
                (format!(" {}", diff.separate_with_commas()))
            }
        } @else if  diff > 0 {
            span.change-up {
                (format!(" +{}", diff.separate_with_commas()))
            }
        } @else {
            span.change-down {
                (format!(" {}", diff.separate_with_commas()))
            }
        }
    }
}

fn count_change_col(cur: isize, prev: isize) -> maud::Markup {
    let diff = cur - prev;
    html! {
        @if diff == 0 {
            td.change-none {
                (format!("{:+}", diff))
            }
        } @else if  diff > 0 {
            td.change-up {
                (format!("{:+}", diff))
            }
        } @else {
            td.change-down {
                (format!("{:+}", diff))
            }
        }
    }
}

fn error_change(change: isize) -> maud::Markup {
    html! {
        @if change < 0 {
            span.change-good {
                (format!("{:+}", change))
            }
        } @else if change > 0 {
            span.change-bad {
                (format!("{:+}", change))
            }
        } @else {
            span.change-neutral {
                (format!("{:+}", change))
            }
        }
    }
}

pub fn rows_html<T: Constrained + Row>(items: Vec<T>, refs: &VarReferences) -> maud::Markup {
    let profile = items.var_profiles();
    let mut invalid: ByField<ByConstraint<isize>> = BTreeMap::default();
    for breach in items.iter().flat_map(|item| item.validate()) {
        let inner = invalid.entry(breach.field.clone()).or_default();
        let count = inner.entry(breach.constraint.clone()).or_default();
        *count += 1;
    }

    html! {
        style { (maud::PreEscaped(CSS)) }
        .data-table {
            (diffed_profiles_html(&Diff::new(profile), refs))
        }

        .step-errors {
            @if invalid.is_empty() {
                ("No constraint breaches.")
            } @else {
                .step-breaches {
                    "Constraint Breaches:"
                        @for (field, errors) in &invalid {
                            .step-breaches-field {
                                div { (format!("{}: ", field)) }
                                @for (error, count) in errors {
                                    .step-breach-error {
                                        span {
                                            (error)
                                        }
                                        span {
                                            (count)
                                        }
                                    }
                                }
                            }
                        }
                }
            }
        }

        script type="text/javascript" { (maud::PreEscaped(JS)) }
    }
}

fn diffed_profiles_html(
    vars: &Diff<ByFacet<ByField<VarProfile>>>,
    refs: &VarReferences,
) -> maud::Markup {
    let changes = vars.count_changes();
    let counts_by_facet = util::sum_inner(&changes);

    // Facet -> Source -> Source Facet -> Vars
    let refs_by_facet = util::transpose(refs.clone());

    html! {
        .profiles {
            .profiles-side {
                .facet-header {
                    "Facets"
                }
                .profiles-tabs {
                    @for facet in vars.current.keys() {
                        @let changed = *counts_by_facet.get(facet).unwrap_or(&0) > 0;
                        .profiles-tab { (
                            format!("{}{}", if changed {
                                "*"
                            } else {
                                ""
                            }, facet_name(facet))) }
                    }
                }
            }
            .profiles-profiles {
                @for (facet, profile) in &vars.current {
                    @let diff = {
                        let mut diff = Diff::new(profile);
                        diff.previous = vars.previous.as_ref().and_then(|prev| {
                            prev.get(facet)
                        });
                        diff
                    };
                    .profiles-profile {
                        {(profile_diff_table(diff, refs_by_facet.get(facet)))}
                    }
                }
            }
        }
    }
}

fn profile_diff_table(
    vars: Diff<&ByField<VarProfile>>,
    refs: Option<&FacetVarReferences>,
) -> maud::Markup {
    let has_diffs = vars.previous.is_some();
    let mut columns = if !has_diffs {
        vec!["Variable", "Mean", "Min", "Max"]
    } else {
        vec!["Variable", "Mean", "Δ", "Min", "Δ", "Max", "Δ"]
    };
    columns.append(&mut vec!["Dist"]);
    if !has_diffs {
        columns.append(&mut vec!["#∅", "#-", "#0", "#∞"]);
    } else {
        columns.append(&mut vec!["#∅", "Δ", "#-", "Δ", "#0", "Δ", "#∞", "Δ"]);
    }

    // TODO kinda hacky
    let total = vars
        .current
        .first_key_value()
        .map_or(0, |profile| profile.1.total);
    let last_total = vars
        .previous
        .map(|vars| vars.first_key_value().map_or(0, |profile| profile.1.total));

    html! {
        .profile-stats {
            (format!("Total: {}", total.separate_with_commas()))
            (last_total.map(|last| {
                count_change_span(total as isize, last as isize)
            }).unwrap_or_default())
        }
        table.profile {
            tr {
                @for col in columns {
                    th { (col) }
                }
            }
            @for (var, prof, prev) in vars.iter() {
                (field_row(var, prof, prev))
                @match refs {
                    None => {},
                    Some(refs) => {
                        @for (source, ref_vals_by_source_facet) in refs.iter() {
                            @for (source_facet, ref_vals) in ref_vals_by_source_facet.iter() {
                                (ref_vals.get(var).map(|ref_val| {
                                    let id = format!("{}:{}", source, facet_name(&source_facet));
                                    html! {
                                        tr.ref-vals {
                                            td title=(&id) {
                                                (&id)
                                            }
                                            (float_col(*ref_val))
                                            @let pd = util::per_diff(*ref_val, prof.summary.mean);
                                            @if pd.is_nan() {
                                                (empty_col())
                                            } @else {
                                                td style=(format!("color:{};", error_color(pd))) {
                                                    (format!("{:+.1}%", pd))
                                                }
                                            }
                                            td colspan="100" {}
                                        }
                                    }
                                }).unwrap_or_default())
                            }
                        }
                    }
                }
            }
        }
    }
}

/// Get the facet name, assuming `None` indicates
/// no facet, i.e. "All".
fn facet_name(facet: &Option<String>) -> &str {
    facet.as_ref().map_or("All", |f| f.as_str())
}
