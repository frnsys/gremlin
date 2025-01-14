//! For displaying various tables and such.

use std::collections::BTreeMap;

use iocraft::prelude::*;
use itertools::Itertools;

use crate::data::profile::Count;

use super::DataProfile;

#[derive(Default, Props)]
pub(crate) struct CountTableProps {
    pub name: String,
    pub total: Option<usize>,
    pub counts: BTreeMap<String, isize>,
}

#[component]
pub(crate) fn CountTable<'a>(props: &CountTableProps) -> impl Into<AnyElement<'a>> {
    element! {
        Box(
            flex_direction: FlexDirection::Column,
            margin_top: 1,
            margin_bottom: 1,
            border_style: BorderStyle::Single,
            border_color: Color::Black,
        ) {
            Box(
                flex_direction: FlexDirection::Row,
                width: Size::Auto,
                justify_content: JustifyContent::SpaceBetween,
                column_gap: Gap::Length(2),
                margin_bottom: 1,
                margin_left: 1,
                margin_right: 1,
            ) {
                Text(content: &props.name, weight: Weight::Bold)
                Text(content: props.total.map_or_else(String::new, |total| {
                    format!("{} total", total)
                }))
            }
            Box(
                flex_direction: FlexDirection::Row,
                width: Size::Auto,
            ) {
                Box(flex_direction: FlexDirection::Column, align_items: AlignItems::End, padding_left: 1, padding_right: 1) {
                    Text(content: "")
                    #(props.counts.keys().map(|label| element! {
                        Text(content: label, decoration: TextDecoration::Underline)
                    }))
                }
                Box(flex_direction: FlexDirection::Column, align_items: AlignItems::End, padding_left: 1, padding_right: 1) {
                    Text(content: "Count", weight: Weight::Bold)
                    #(props.counts.values().map(|count| element! {
                        Text(content: format!("{}", count))
                    }))
                }

                #(props.total.map(|total| {
                    element! {
                        Box(flex_direction: FlexDirection::Column, padding_right: 1, align_items: AlignItems::End) {
                            Text(content: "Percent", weight: Weight::Bold)
                            #(props.counts.values().map(|count| element! {
                                Text(content: Count::new(*count, total).display_percent())
                            }))
                        }
                    }
                }))
            }
        }
    }
}

// May need to lower/raise this for smaller/larger screens.
const MAX_VARS_PER_TABLE: usize = 4;

#[derive(Default, Props)]
pub(crate) struct VarTableProps {
    pub name: String,
    pub profile: DataProfile,
}

#[derive(Default, Props)]
pub(crate) struct FacetedVarTablesProps {
    pub name: String,
    pub profiles: BTreeMap<Option<String>, DataProfile>,
}

#[component]
pub(crate) fn FacetedVarTables<'a>(props: &FacetedVarTablesProps) -> impl Into<AnyElement<'a>> {
    element! {
        Box(
            flex_direction: FlexDirection::Column,
            flex_wrap: FlexWrap::Wrap,
        ) {
            #(props.profiles.iter().map(|(facet, profile)| {
                let title = facet.as_ref().map_or(props.name.to_string(), |facet| {
                    format!("{}: {}", props.name, facet)
                });
                element! {
                    VarTables(name: title, profile: profile.clone())
                }
            }))
        }
    }
}

#[component]
fn VarTables<'a>(props: &VarTableProps) -> impl Into<AnyElement<'a>> {
    // TODO get rid of cloning/clean up
    let tables: Vec<VarTableProps> = props
        .profile
        .profiles
        .clone()
        .into_iter()
        .chunks(MAX_VARS_PER_TABLE)
        .into_iter()
        .map(|chunk| VarTableProps {
            name: props.name.clone(),
            profile: DataProfile {
                total: props.profile.total,
                profiles: chunk.collect(),
            },
        })
        .collect();

    element! {
        Box(
            flex_direction: FlexDirection::Column,
            margin_top: 1,
            margin_bottom: 1,
            border_style: BorderStyle::Single,
            border_color: Color::Black,
        ) {
            Box(
                flex_direction: FlexDirection::Row,
                width: Size::Auto,
                justify_content: JustifyContent::SpaceBetween,
                column_gap: Gap::Length(2),
                margin_bottom: 1,
                margin_left: 1,
                margin_right: 1,
            ) {
                Text(content: &props.name, weight: Weight::Bold)
                Text(content: format!("{} rows", props.profile.total))
            }
            #(tables.into_iter().map(|table| element! {
                VarTable(name: table.name, profile: table.profile)
            }))
        }
    }
}

#[component]
fn VarTable<'a>(props: &VarTableProps) -> impl Into<AnyElement<'a>> {
    element! {
        Box(
            flex_direction: FlexDirection::Column,
        ) {
            Box(
                flex_direction: FlexDirection::Row,
                width: Size::Auto,
            ) {
                Box(flex_direction: FlexDirection::Column, align_items: AlignItems::End, padding_left: 1, padding_right: 1) {
                    Text(content: "", weight: Weight::Bold)
                    Text(content: "Missing", weight: Weight::Bold)
                    Text(content: "Distinct", weight: Weight::Bold)
                    Text(content: "Duplicate", weight: Weight::Bold)
                    Text(content: "Zero", weight: Weight::Bold)
                    Text(content: "Negative", weight: Weight::Bold)
                    Text(content: "Infinite", weight: Weight::Bold)
                    Text(content: "Outliers", weight: Weight::Bold)
                    Text(content: "Mean", weight: Weight::Bold)
                    Text(content: "Median", weight: Weight::Bold)
                    Text(content: "Std Dev", weight: Weight::Bold)
                    Text(content: "Min", weight: Weight::Bold)
                    Text(content: "Max", weight: Weight::Bold)
                    Text(content: "Range", weight: Weight::Bold)
                    Text(content: "Dist", weight: Weight::Bold)
                    Text(content: "------", color: Color::Black)
                }

                #(props.profile.profiles.iter().map(|(var, prof)| element! {
                    Box(flex_direction: FlexDirection::Column, padding_right: 1, align_items: AlignItems::End) {
                        Text(content: var, decoration: TextDecoration::Underline)
                        Text(content: format!("{}", prof.missing.count), color: if prof.missing.all() {
                            Color::Red
                        } else {
                            Color::Reset
                        })
                        Text(content: format!("{}", prof.distinct.count))
                        Text(content: format!("{}", prof.duplicate.count))
                        Text(content: format!("{}", prof.zero.count))
                        Text(content: format!("{}", prof.negative.count))
                        Text(content: format!("{}", prof.infinite.count))
                        Text(content: format!("{}", prof.summary.outliers.count))
                        Text(content: format!("{:.3}", prof.summary.mean), weight: Weight::Bold)
                        Text(content: format!("{:.3}", prof.summary.median))
                        Text(content: format!("{:.3}", prof.summary.std_dev))
                        Text(content: format!("{:.3}", prof.summary.min))
                        Text(content: format!("{:.3}", prof.summary.max))
                        Text(content: format!("{:.3}", prof.summary.range))
                        Text(content: &prof.summary.histogram, color: Color::DarkGrey)
                        Text(content: "------", color: Color::Black)
                    }
                }))
            }
        }
    }
}
