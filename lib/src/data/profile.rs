use ahash::HashMap;
use ordered_float::NotNan;
use serde::{Deserialize, Serialize};

use super::Vecf32Ext;

serde_with::with_prefix!(missing "missing.");
serde_with::with_prefix!(infinite "infinite.");
serde_with::with_prefix!(negative "negative.");
serde_with::with_prefix!(zero "zero.");
serde_with::with_prefix!(duplicate "duplicate.");
serde_with::with_prefix!(distinct "distinct.");
serde_with::with_prefix!(outliers "outliers.");
serde_with::with_prefix!(summary "summary.");

#[derive(Debug, Default, Serialize, Deserialize, Clone)]
pub struct Count {
    pub count: isize,
    pub percent: f32,
}
impl Count {
    pub fn new(count: isize, total: usize) -> Self {
        let percent = count as f32 / total as f32;
        Self { count, percent }
    }

    pub fn all(&self) -> bool {
        self.percent >= 1.
    }

    pub fn display_percent(&self) -> String {
        if self.percent < 0.01 {
            "<1%".to_string()
        } else {
            format!("{:.1}%", self.percent * 100.)
        }
    }
}
impl std::fmt::Display for Count {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{} ({})", self.count, self.display_percent())
    }
}
impl<'a, 'b> std::ops::Sub<&'b Count> for &'a Count {
    type Output = Count;
    fn sub(self, rhs: &'b Count) -> Self::Output {
        Count {
            count: self.count - rhs.count,
            percent: self.percent - rhs.percent,
        }
    }
}

/// Data profile for a dataset of multiple variables.
#[derive(Debug, Default, Serialize, Deserialize)]
pub struct SetProfile {
    pub missing: Count,
    pub duplicate: Count,
    pub n_variables: usize,
    pub observations: usize,
    pub variables: HashMap<String, VarProfile>,
}

/// Data profile for a single variable.
#[derive(Debug, Default, Serialize, Deserialize, Clone)]
pub struct VarProfile {
    pub total: usize,

    #[serde(flatten, with = "missing")]
    pub missing: Count,

    #[serde(flatten, with = "infinite")]
    pub infinite: Count,

    #[serde(flatten, with = "negative")]
    pub negative: Count,

    #[serde(flatten, with = "zero")]
    pub zero: Count,

    #[serde(flatten, with = "duplicate")]
    pub duplicate: Count,

    #[serde(flatten, with = "distinct")]
    pub distinct: Count,

    pub is_constant: bool,

    #[serde(flatten, with = "summary")]
    pub summary: Summary,
}
impl std::ops::Sub<&VarProfile> for &VarProfile {
    type Output = VarProfile;
    fn sub(self, rhs: &VarProfile) -> Self::Output {
        VarProfile {
            total: &self.total - &rhs.total,
            missing: &self.missing - &rhs.missing,
            infinite: &self.infinite - &rhs.infinite,
            negative: &self.negative - &rhs.negative,
            zero: &self.zero - &rhs.zero,
            duplicate: &self.duplicate - &rhs.duplicate,
            distinct: &self.distinct - &rhs.distinct,
            is_constant: rhs.is_constant,
            summary: &self.summary - &rhs.summary,
        }
    }
}

#[cfg(feature = "console")]
mod console {
    //! Stuck behind a feature flag for environments
    //! that don't have a console; e.g. WASM.
    use comfy_table::{presets::NOTHING, *};
    impl super::VarProfile {
        pub fn print(&self) {
            #[rustfmt::skip]
            let headers = vec![
                "∅",
                "∞",
                "-",
                "0",
                "∃!",
                "¬∃!",
            ];

            let mut table = Table::new();
            table
                .load_preset(NOTHING)
                .set_content_arrangement(ContentArrangement::Dynamic)
                .set_header(headers)
                .add_row(vec![
                    Cell::new(&self.missing),
                    Cell::new(&self.infinite),
                    Cell::new(&self.negative),
                    Cell::new(&self.zero),
                    Cell::new(&self.distinct),
                    Cell::new(&self.duplicate),
                ]);
            println!("{table}");

            let summary = &self.summary;
            let headers = vec![
                "min",
                "max",
                "range",
                "mean",
                "median",
                "std",
                "p5",
                "p95",
                "q1",
                "q3",
                "iqr",
                "kurt.",
                "skew.",
                "coef of var",
                "mean abs dev",
                "ols",
                "ols lwr bnd",
                "ols upr bnd",
            ];

            let mut table = Table::new();
            table
                .load_preset(NOTHING)
                .set_content_arrangement(ContentArrangement::Dynamic)
                .set_header(headers)
                .add_row(vec![
                    Cell::new(summary.min),
                    Cell::new(summary.max),
                    Cell::new(summary.range),
                    Cell::new(summary.mean),
                    Cell::new(summary.median),
                    Cell::new(summary.std_dev),
                    Cell::new(summary.p5),
                    Cell::new(summary.p95),
                    Cell::new(summary.q1),
                    Cell::new(summary.q3),
                    Cell::new(summary.iqr),
                    Cell::new(summary.kurtosis),
                    Cell::new(summary.skewness),
                    Cell::new(summary.coef_of_var),
                    Cell::new(summary.mean_abs_dev),
                    Cell::new(&summary.outliers),
                    Cell::new(summary.outlier_lower_bound),
                    Cell::new(summary.outlier_upper_bound),
                ]);
            println!("{table}");

            println!(
                "{}|{}|{}",
                summary.outlier_lower_bound, summary.histogram, summary.outlier_upper_bound
            );
        }
    }
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct Summary {
    pub min: f32,
    pub max: f32,
    pub mean: f32,
    pub median: f32,
    pub std_dev: f32,
    pub p5: f32,
    pub p95: f32,
    pub q1: f32,
    pub q3: f32,
    pub iqr: f32,
    pub range: f32,
    pub kurtosis: f32,
    pub skewness: f32,
    pub coef_of_var: f32,
    pub mean_abs_dev: f32,

    #[serde(flatten, with = "outliers")]
    pub outliers: Count,

    pub outlier_lower_bound: f32,
    pub outlier_upper_bound: f32,
    pub histogram: String,
    pub sample_size: isize,
}
impl Summary {
    pub fn empty() -> Self {
        Self {
            min: f32::NAN,
            max: f32::NAN,
            mean: f32::NAN,
            median: f32::NAN,
            std_dev: f32::NAN,
            p5: f32::NAN,
            p95: f32::NAN,
            q1: f32::NAN,
            q3: f32::NAN,
            iqr: f32::NAN,
            range: f32::NAN,
            kurtosis: f32::NAN,
            skewness: f32::NAN,
            coef_of_var: f32::NAN,
            mean_abs_dev: f32::NAN,
            outliers: Count::default(),
            outlier_lower_bound: f32::NAN,
            outlier_upper_bound: f32::NAN,
            histogram: String::default(),
            sample_size: 0,
        }
    }
}
impl Default for Summary {
    fn default() -> Self {
        Self::empty()
    }
}
impl std::ops::Sub<&Summary> for &Summary {
    type Output = Summary;
    fn sub(self, rhs: &Summary) -> Self::Output {
        Summary {
            min: self.min - rhs.min,
            max: self.max - rhs.max,
            mean: self.mean - rhs.mean,
            median: self.median - rhs.median,
            std_dev: self.std_dev - rhs.std_dev,
            p5: self.p5 - rhs.p5,
            p95: self.p95 - rhs.p95,
            q1: self.q1 - rhs.q1,
            q3: self.q3 - rhs.q3,
            iqr: self.iqr - rhs.iqr,
            range: self.range - rhs.range,
            kurtosis: self.kurtosis - rhs.kurtosis,
            skewness: self.skewness - rhs.skewness,
            coef_of_var: self.coef_of_var - rhs.coef_of_var,
            mean_abs_dev: self.mean_abs_dev - rhs.mean_abs_dev,
            outliers: &self.outliers - &rhs.outliers,
            outlier_lower_bound: self.outlier_lower_bound - rhs.outlier_lower_bound,
            outlier_upper_bound: self.outlier_upper_bound - rhs.outlier_upper_bound,
            sample_size: self.sample_size - rhs.sample_size,

            // TODO show actual difference
            histogram: rhs.histogram.clone(),
        }
    }
}

pub fn profile(values: impl Iterator<Item = impl Into<f32>>) -> VarProfile {
    let values: Vec<f32> = values.map(|val| val.into()).collect();
    let total = values.len();

    let valid: Vec<f32> = values.into_iter().filter(|val| !val.is_nan()).collect();
    let n_valid = valid.len() as isize;

    let missing = Count::new(total as isize - n_valid as isize, total);

    let finite: Vec<f32> = valid.into_iter().filter(|val| val.is_finite()).collect();
    let infinite = Count::new(n_valid - finite.len() as isize, total);

    let negative = finite.iter().filter(|val| **val < 0.0).count();
    let negative = Count::new(negative as isize, total);

    let zero = finite.iter().filter(|val| **val == 0.0).count();
    let zero = Count::new(zero as isize, total);

    let mut valid = finite;

    // SAFETY: We've already filtered out NaN values.
    let ord_float: Vec<_> = valid
        .iter()
        .map(|val| unsafe { NotNan::new_unchecked(*val) })
        .collect();

    let mut counts: HashMap<NotNan<f32>, usize> = HashMap::default();
    for val in ord_float {
        let count = counts.entry(val).or_default();
        *count += 1;
    }
    let n_duplicate = counts.values().filter(|count| **count > 1).sum::<usize>() as isize;
    let duplicate = Count::new(n_duplicate, valid.len());
    let distinct = Count::new(valid.len() as isize - n_duplicate, valid.len());
    let is_constant = counts.len() == 1;

    let summary = if valid.is_empty() {
        Summary::empty()
    } else {
        let n_valid = valid.len();

        let min: f32 = valid.min();
        let max: f32 = valid.max();
        let range = max - min;

        let sum: f32 = valid.iter().sum::<f32>();
        let mean = sum / n_valid as f32;

        valid.sort_by(|a, b| a.partial_cmp(b).expect("Only non-nan values"));
        let q1 = calculate_percentile(&valid, 25.);
        let q3 = calculate_percentile(&valid, 75.);
        let median = median(&valid).expect("Non-empty sequence");

        let iqr = q3 - q1;
        let outlier_lower_bound = q1 - 1.5 * iqr;
        let outlier_upper_bound = q3 + 1.5 * iqr;

        let p5 = calculate_percentile(&valid, 5.);
        let p95 = calculate_percentile(&valid, 95.);
        let sample_size = valid.len() as isize;

        let non_outlier: Vec<f32> = valid
            .into_iter()
            .filter(move |val| *val >= outlier_lower_bound && *val <= outlier_upper_bound)
            .collect();
        let outliers = Count::new(n_valid as isize - non_outlier.len() as isize, n_valid);

        // TODO should this be calculated after or before removing outliers?
        let valid = non_outlier;
        let n_valid = valid.len();

        let rel_var = relative_variance(&valid, mean, median).expect("Has at least one value");
        let coef_of_var = rel_var.sqrt();

        let variance = valid.iter().map(|val| (val - mean).powi(2)).sum::<f32>() / n_valid as f32;
        let std_dev = variance.sqrt();

        let skewness = valid
            .iter()
            .map(|val| ((val - mean) / std_dev).powi(3))
            .sum::<f32>()
            / n_valid as f32;
        let kurtosis = valid
            .iter()
            .map(|val| ((val - mean) / std_dev).powi(4))
            .sum::<f32>()
            / n_valid as f32
            - 3.0; // Subtract 3 for excess kurtosis
        let mean_abs_dev = valid.iter().map(|val| (val - mean).abs()).sum::<f32>() / n_valid as f32;

        let histogram = histogram(&valid, 6);

        Summary {
            min,
            max,
            range,
            mean,
            median,
            p5,
            p95,
            q1,
            q3,
            iqr,
            outlier_lower_bound,
            outlier_upper_bound,
            outliers,
            coef_of_var,
            std_dev,
            skewness,
            kurtosis,
            mean_abs_dev,
            histogram,
            sample_size,
        }
    };

    VarProfile {
        total,
        missing,
        infinite,
        negative,
        zero,
        distinct,
        duplicate,
        is_constant,
        summary,
    }
}

fn calculate_percentile(sorted_data: &[f32], percentile: f32) -> f32 {
    let index = (percentile / 100.0) * (sorted_data.len() as f32 - 1.0);
    let lower = index.floor() as usize;
    let upper = index.ceil() as usize;
    if lower == upper {
        sorted_data[lower]
    } else {
        let weight = index - lower as f32;
        sorted_data[lower] * (1.0 - weight) + sorted_data[upper] * weight
    }
}

fn median(sorted_data: &[f32]) -> Option<f32> {
    if sorted_data.is_empty() {
        return None;
    }

    let mid = sorted_data.len() / 2;
    let val = if sorted_data.len() % 2 == 0 {
        (sorted_data[mid - 1] + sorted_data[mid]) / 2.0
    } else {
        sorted_data[mid]
    };
    Some(val)
}

/// Compute the relative variance, i.e. the coefficient of variance.
fn relative_variance(data: &[f32], mean: f32, median: f32) -> Option<f32> {
    if data.is_empty() {
        return None;
    }

    let denom = if median == 0. { mean } else { median };
    let variance = data.iter().map(|x| (x - denom).powi(2)).sum::<f32>() / data.len() as f32;
    Some(variance / denom.powi(2))
}

fn histogram(values: &[f32], bins: usize) -> String {
    const SYMBOLS: [char; 8] = [' ', '▁', '▂', '▃', '▄', '▅', '▆', '▇'];

    let mut s = String::new();
    if !values.is_empty() {
        let bins = bin_values(values, bins);
        let counts: Vec<_> = bins.iter().map(|bin| bin.len()).collect();
        let max = counts.iter().max().expect("Checked for values");
        for count in &counts {
            let p = *count as f32 / *max as f32;
            let p = (p * 7.).round() as usize;
            let sym = SYMBOLS[p];
            s.push(sym);
        }
    }
    s
}

fn bin_values(values: &[f32], num_bins: usize) -> Vec<Vec<f32>> {
    if num_bins == 0 || values.is_empty() {
        return vec![vec![]; num_bins];
    }

    let min_value = values.iter().cloned().fold(f32::INFINITY, f32::min);
    let max_value = values.iter().cloned().fold(f32::NEG_INFINITY, f32::max);

    let bin_width = (max_value - min_value) / num_bins as f32;

    let mut bins: Vec<Vec<f32>> = vec![Vec::new(); num_bins];

    for &value in values {
        let bin_index = if bin_width > 0.0 {
            ((value - min_value) / bin_width).floor() as usize
        } else {
            0 // Handle the case where all values are the same
        }
        .min(num_bins - 1); // Ensure the index does not exceed the number of bins

        bins[bin_index].push(value);
    }

    bins
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_profile() {
        #[rustfmt::skip]
        let vals = vec![
            1.,
            f32::NAN,
            -2.,
            1.,
            f32::INFINITY,
            0.,
            -6.,
            0.,
            8.,
            1000.,
        ];
        let profile = profile(vals.iter().cloned());
        // profile.print();

        assert_eq!(profile.infinite.count, 1);
        assert_eq!(profile.missing.count, 1);
        assert_eq!(profile.zero.count, 2);
        assert_eq!(profile.negative.count, 2);
        assert_eq!(profile.duplicate.count, 4);
        assert_eq!(profile.distinct.count, 4);
    }
}
