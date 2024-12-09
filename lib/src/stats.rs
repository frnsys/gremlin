/// Compute the median.
///
/// This will sort the values before computing the median.
pub fn median(nums: &mut [f32]) -> Option<f32> {
    if nums.is_empty() {
        return None;
    }

    nums.sort_by(|a, b| a.partial_cmp(b).unwrap());
    let mid = nums.len() / 2;
    let val = if nums.len() % 2 == 0 {
        (nums[mid - 1] + nums[mid]) / 2.0
    } else {
        nums[mid]
    };
    Some(val)
}

/// Compute the relative variance, i.e. the coefficient of variance.
pub fn relative_variance(data: &[f32], mean: f32, median: f32) -> Option<f32> {
    if data.is_empty() {
        return None;
    }

    let denom = if median == 0. { mean } else { median };
    let variance = data.iter().map(|x| (x - denom).powi(2)).sum::<f32>() / data.len() as f32;
    Some(variance / denom.powi(2))
}

/// Compute the specified percentile (e.g. `75.`).
///
/// Assumes the data is already sorted.
pub fn calculate_percentile(sorted_data: &[f32], percentile: f32) -> f32 {
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

/// Compute Q1 and Q3 for the interquartile range.
///
/// Will sort the data.
pub fn compute_q1_q3(data: impl Iterator<Item = f32>) -> Option<(f32, f32)> {
    let mut data: Vec<f32> = data.collect();
    if data.is_empty() {
        None
    } else {
        data.sort_by(|a, b| a.partial_cmp(b).unwrap());
        let q1 = calculate_percentile(&data, 25.0);
        let q3 = calculate_percentile(&data, 75.0);
        Some((q1, q3))
    }
}

/// Filter outliers using +/- IQR.
///
/// This returns an iterator where removed values
/// are `None` and preserved values are `Some(..)`.
pub fn filter_outliers(
    data: &[f32],
    q1: f32,
    q3: f32,
) -> impl Iterator<Item = Option<f32>> + use<'_> {
    let iqr = q3 - q1;
    let lower_bound = q1 - 1.5 * iqr;
    let upper_bound = q3 + 1.5 * iqr;

    data.iter().map(move |&x| {
        if x < lower_bound || x > upper_bound {
            None
        } else {
            Some(x)
        }
    })
}
