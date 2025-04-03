use std::fmt::Display;

pub use comfy_table::Cell;
use comfy_table::{CellAlignment, Row, Table};
use ordered_float::OrderedFloat;
use palette::{LinSrgb, Mix};

use crate::core::{Percent, Unit};

pub trait AsCell {
    fn as_cell(&self) -> Cell;
}

pub fn simple_table<const N: usize>(
    cols: [&str; N],
    rows: impl Iterator<Item = (String, [Cell; N])>,
) -> Table {
    let cols = [""].into_iter().chain(cols.iter().cloned());
    build_table(
        cols,
        rows.map(|(name, vals)| {
            let mut row = vec![Cell::new(name)];
            row.extend(vals.into_iter());
            row
        }),
    )
}

pub fn print_table(title: &str, table: Table) {
    let width = table.trim_fmt().lines().collect::<Vec<_>>()[0].len();
    let divider_width = (width / 3 - title.len()) / 2;
    let divider = "-".repeat(divider_width);
    println!("\n{divider}{title}{divider}");
    println!("{table}\n");
}

pub fn print_simple_table<const N: usize>(
    title: &str,
    cols: [&str; N],
    rows: impl Iterator<Item = (String, [Cell; N])>,
) {
    let table = simple_table(cols, rows);
    print_table(title, table);
}

fn build_table(cols: impl Into<Row>, rows: impl Iterator<Item = Vec<Cell>>) -> Table {
    let mut table = Table::new();
    table.load_preset(comfy_table::presets::UTF8_BORDERS_ONLY);
    table.set_content_arrangement(comfy_table::ContentArrangement::Dynamic);
    table.enforce_styling();
    table.set_header(cols);
    table.add_rows(rows);
    table
}

pub fn timeseries_table<T: Display>(time_step: &str, series: Vec<(T, Cell, f32)>) -> Table {
    let histogram = horizontal_bars(
        &series.iter().map(|(_, _, val)| *val).collect::<Vec<_>>(),
        8,
        false,
    );
    let cols = vec![time_step, "Value", ""];
    let rows = series
        .into_iter()
        .zip(&histogram)
        .map(|((time, value, _), h)| {
            vec![
                Cell::new(time.to_string()),
                value,
                Cell::new(h).fg(NEUT_COLOR),
            ]
        });
    build_table(cols, rows)
}

/// Shows change in distribution over time.
pub fn compare_timeseries_table<T: Display, U: Copy + Into<f32>>(
    time_step: &str,
    times: &[T],
    series: Vec<(String, Vec<U>)>,
) -> Table {
    let neut_color: LinSrgb<f32> = LinSrgb::new(0x33_u8, 0x33_u8, 0x33_u8).into_format();
    let high_color: LinSrgb<f32> = LinSrgb::new(0xe7_u8, 0xad_u8, 0x3f_u8).into_format();

    let histograms: Vec<_> = times
        .iter()
        .enumerate()
        .map(|(i, _)| {
            let vals: Vec<f32> = series.iter().map(|(_, ts)| ts[i].into()).collect();
            let sum: f32 = vals.iter().sum();
            vals.iter()
                .map(|f| {
                    let p = *f / sum;
                    let color = neut_color.mix(high_color, p);
                    let color: LinSrgb<u8> = color.into_format();
                    (
                        p,
                        comfy_table::Color::Rgb {
                            r: color.red,
                            g: color.green,
                            b: color.blue,
                        },
                    )
                })
                .zip(horizontal_bars(&vals, 4, true))
                .collect::<Vec<_>>()
        })
        .collect();

    let mut cols = vec![time_step];
    cols.extend(series.iter().map(|(label, _)| label.as_str()));
    let rows = times.iter().zip(histograms).map(|(time, hist)| {
        let mut cells = vec![Cell::new(time.to_string())];
        for ((_p, color), hs) in hist {
            cells.push(Cell::new(hs).fg(color));
        }
        cells
    });
    build_table(cols, rows)
}

pub const NEUT_COLOR: comfy_table::Color = comfy_table::Color::Rgb {
    r: 0x33,
    g: 0x33,
    b: 0x38,
};

impl<T: Copy + Into<f32>> AsCell for (T, T) {
    fn as_cell(&self) -> Cell {
        pct_cell(self.0.into(), self.1.into())
    }
}

impl<U: Unit> AsCell for U {
    fn as_cell(&self) -> Cell {
        let val = self.value();
        if val.is_nan() {
            Cell::new("--")
                .fg(NEUT_COLOR)
                .set_alignment(CellAlignment::Right)
        } else {
            Cell::new(format!("{:.2}{}", val, U::abbrev())).set_alignment(CellAlignment::Right)
        }
    }
}

impl AsCell for Percent {
    fn as_cell(&self) -> Cell {
        let val = self.value();
        if val.is_nan() {
            Cell::new("--")
                .fg(NEUT_COLOR)
                .set_alignment(CellAlignment::Right)
        } else {
            Cell::new(format!("{:.2}%", val * 100.)).set_alignment(CellAlignment::Right)
        }
    }
}

impl AsCell for f32 {
    fn as_cell(&self) -> Cell {
        let val = self;
        if val.is_nan() {
            Cell::new("--")
                .fg(NEUT_COLOR)
                .set_alignment(CellAlignment::Right)
        } else {
            Cell::new(val.to_string()).set_alignment(CellAlignment::Right)
        }
    }
}

impl AsCell for u16 {
    fn as_cell(&self) -> Cell {
        Cell::new(self.to_string()).set_alignment(CellAlignment::Right)
    }
}

impl AsCell for usize {
    fn as_cell(&self) -> Cell {
        Cell::new(self.to_string()).set_alignment(CellAlignment::Right)
    }
}

fn pct_cell<T: Into<f32>>(a: T, b: T) -> Cell {
    let p = percent_change(a.into(), b.into());
    let up_color = comfy_table::Color::Rgb {
        r: 0x1F,
        g: 0x6F,
        b: 0xEB,
    };
    let down_color = comfy_table::Color::Rgb {
        r: 0xDB,
        g: 0x4F,
        b: 0x4F,
    };
    match p {
        Some(p) => {
            if p.is_nan() {
                Cell::new("--%")
                    .fg(NEUT_COLOR)
                    .set_alignment(CellAlignment::Right)
            } else if p > 0. {
                Cell::new(format!("{:+.2}%", p))
                    .fg(up_color)
                    .set_alignment(CellAlignment::Right)
            } else if p < 0. {
                Cell::new(format!("{:+.2}%", p))
                    .fg(down_color)
                    .set_alignment(CellAlignment::Right)
            } else {
                Cell::new(format!("{:+.2}%", p))
                    .fg(NEUT_COLOR)
                    .set_alignment(CellAlignment::Right)
            }
        }
        None => Cell::new("--%")
            .fg(NEUT_COLOR)
            .set_alignment(CellAlignment::Right),
    }
}

pub fn horizontal_bars(vals: &[f32], n_chars: i32, use_share: bool) -> Vec<String> {
    let denom = if use_share {
        vals.iter().sum::<f32>()
    } else {
        *vals.iter().map(|f| OrderedFloat(*f)).max().unwrap()
    };
    let mut bars = vec![];
    for val in vals {
        let p = val / denom * n_chars as f32;
        let whole = p.trunc() as i32;
        let mut s = String::new();
        for _ in 0..whole {
            s.push('█');
        }
        let decimal = p.fract();
        let numerator = (decimal * 8.0).round() as i32;
        let last_block = match numerator {
            i32::MIN..=0_i32 => "",
            1 => "▏",
            2 => "▎",
            3 => "▍",
            4 => "▌",
            5 => "▋",
            6 => "▊",
            7 => "▉",
            8_i32..=i32::MAX => "█",
        };
        s.push_str(last_block);
        bars.push(s);
    }
    bars
}

fn percent_change(a: f32, b: f32) -> Option<f32> {
    if b == 0.0 {
        return None; // Prevent division by zero
    }
    Some(((a - b) / b) * 100.0)
}
