use ordered_float::OrderedFloat;

enum Values<N>
where
    f32: From<N>,
{
    Sorted(Vec<N>),
    Unsorted(Vec<N>),
}

#[derive(Debug)]
pub struct Data(Vec<OrderedFloat<f32>>);
impl Data {
    pub fn from_sorted<N>(values: Vec<N>) -> Self
    where
        f32: From<N>,
    {
        Values::Sorted(values).into()
    }

    pub fn from_unsorted<N>(values: Vec<N>) -> Self
    where
        f32: From<N>,
    {
        Values::Unsorted(values).into()
    }
}

impl<N> From<Values<N>> for Data
where
    f32: From<N>,
{
    fn from(values: Values<N>) -> Self {
        let values: Vec<OrderedFloat<f32>> = match values {
            Values::Sorted(vec) => vec
                .into_iter()
                .map(|v| OrderedFloat(f32::from(v)))
                .collect(),
            Values::Unsorted(vec) => {
                let mut values: Vec<_> = vec
                    .into_iter()
                    .map(|v| OrderedFloat(f32::from(v)))
                    .collect();
                values.sort();
                values
            }
        };
        Self(values)
    }
}

impl Data {
    pub fn empty() -> Self {
        Self(vec![])
    }

    pub fn len(&self) -> usize {
        self.0.len()
    }

    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    pub fn push(&mut self, value: f32) {
        let value = OrderedFloat(value);
        let pos = self.0.binary_search(&value).unwrap_or_else(|e| e);
        self.0.insert(pos, value);
    }

    pub fn min(&self) -> Option<f32> {
        self.0.first().copied().map(OrderedFloat::into_inner)
    }

    pub fn max(&self) -> Option<f32> {
        self.0.last().copied().map(OrderedFloat::into_inner)
    }

    pub fn range(&self) -> Option<f32> {
        self.min().zip(self.max()).map(|(min, max)| max - min)
    }

    /// Compute the median.
    pub fn median(&self) -> Option<f32> {
        let vals = &self.0;
        if vals.is_empty() {
            return None;
        }
        let mid = vals.len() / 2;
        let val = if vals.len() % 2 == 0 {
            (vals[mid - 1] + vals[mid]) / 2.0
        } else {
            vals[mid]
        };
        Some(*val)
    }

    pub fn mean(&self) -> Option<f32> {
        let vals = &self.0;
        if vals.is_empty() {
            None
        } else if vals.first() == vals.last() {
            Some(*vals[0])
        } else {
            let mean = vals.iter().map(|v| **v).sum::<f32>() / vals.len() as f32;
            Some(mean)
        }
    }

    pub fn variance(&self) -> Option<f32> {
        self.mean().map(|mean| {
            let n = self.len() as f32;
            self.0.iter().map(|val| (val - mean).powi(2)).sum::<f32>() / n
        })
    }

    pub fn mean_abs_dev(&self) -> Option<f32> {
        self.mean().map(|mean| {
            let n = self.len() as f32;
            self.0.iter().map(|val| (val - mean).abs()).sum::<f32>() / n
        })
    }

    pub fn std_dev(&self) -> Option<f32> {
        self.variance().map(|var| var.sqrt())
    }

    /// Compute the relative variance, i.e. the coefficient of variance.
    pub fn relative_variance(&self) -> Option<f32> {
        let vals = &self.0;
        if vals.is_empty() {
            return None;
        }

        let median = self.median().expect("Checked if vals was empty.");
        let mean = self.mean().expect("Checked if vals was empty.");
        let denom = if median == 0. { mean } else { median };
        let variance = vals.iter().map(|x| (x - denom).powi(2)).sum::<f32>() / vals.len() as f32;
        Some(variance / denom.powi(2))
    }

    pub fn skewness(&self) -> Option<f32> {
        self.mean().zip(self.std_dev()).map(|(mean, std_dev)| {
            let n = self.len() as f32;
            self.0
                .iter()
                .map(|val| ((val - mean) / std_dev).powi(3))
                .sum::<f32>()
                / n
        })
    }

    pub fn kurtosis(&self) -> Option<f32> {
        self.mean().zip(self.std_dev()).map(|(mean, std_dev)| {
            let n = self.len() as f32;
            self.0
                .iter()
                .map(|val| ((val - mean) / std_dev).powi(4))
                .sum::<f32>()
                / n
                - 3.0 // Subtract 3 for excess kurtosis
        })
    }

    /// Compute the specified percentile (e.g. `75.`).
    pub fn percentile(&self, percentile: f32) -> f32 {
        let vals = &self.0;
        let index = (percentile / 100.0) * (vals.len() as f32 - 1.0);
        let lower = index.floor() as usize;
        let upper = index.ceil() as usize;
        if lower == upper || vals[lower] == vals[upper] {
            *vals[lower]
        } else {
            let weight = index - lower as f32;
            *(vals[lower] * (1.0 - weight) + vals[upper] * weight)
        }
    }

    /// Compute Q1 and Q3 for the interquartile range.
    fn compute_q1_q3(&self) -> Option<(f32, f32)> {
        let vals = &self.0;
        if vals.is_empty() {
            None
        } else {
            let q1 = self.percentile(25.0);
            let q3 = self.percentile(75.0);
            Some((q1, q3))
        }
    }

    fn outlier_bounds(&self) -> Option<(f32, f32)> {
        self.compute_q1_q3().map(|(q1, q3)| {
            let iqr = q3 - q1;
            let lower_bound = q1 - 1.5 * iqr;
            let upper_bound = q3 + 1.5 * iqr;
            (lower_bound, upper_bound)
        })
    }

    /// Return an iterator with outliers filtered out.
    pub fn filter_outliers(&self) -> impl Iterator<Item = f32> + use<'_> {
        self.non_outliers().flatten()
    }

    /// Calculate non-outliers using +/- IQR.
    ///
    /// This returns an iterator where outliers
    /// are `None` and non-outliers values are `Some(..)`.
    pub fn non_outliers(&self) -> impl Iterator<Item = Option<f32>> + use<'_> {
        let (lower, upper) = self
            .outlier_bounds()
            .unwrap_or((f32::INFINITY, f32::NEG_INFINITY));
        self.0.iter().map(move |&x| {
            let x = *x;
            if x < lower || x > upper {
                None
            } else {
                Some(x)
            }
        })
    }

    /// Remove outliers in this data.
    pub fn remove_outliers(&mut self) {
        let (lower, upper) = self
            .outlier_bounds()
            .unwrap_or((f32::INFINITY, f32::NEG_INFINITY));
        self.0.retain(|&x| *x >= lower && *x <= upper);
    }

    fn bin_values(&self, num_bins: usize) -> Vec<Vec<f32>> {
        let vals = &self.0;
        if num_bins == 0 || vals.is_empty() {
            return vec![vec![]; num_bins];
        }

        let min_value = self.min().expect("Checked if empty");
        let max_value = self.max().expect("Checked if empty");
        let bin_width = (max_value - min_value) / num_bins as f32;

        let mut bins: Vec<Vec<f32>> = vec![Vec::new(); num_bins];
        for &value in vals {
            let bin_index = if bin_width > 0.0 {
                ((value - min_value) / bin_width).floor() as usize
            } else {
                0 // Handle the case where all values are the same
            }
            .min(num_bins - 1); // Ensure the index does not exceed the number of bins

            bins[bin_index].push(*value);
        }

        bins
    }

    pub fn histogram(&self, bins: usize) -> String {
        const SYMBOLS: [char; 8] = [' ', '▁', '▂', '▃', '▄', '▅', '▆', '▇'];
        let mut s = String::new();
        if !self.is_empty() {
            let bins = self.bin_values(bins);
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
}

pub struct MultiData(Vec<Data>);
impl MultiData {
    pub fn new<N>(mut values: Vec<Vec<N>>) -> Self
    where
        f32: From<N>,
    {
        let mut datas = vec![];
        while !values[0].is_empty() {
            let data =
                Data::from_unsorted(values.iter_mut().filter_map(|vals| vals.pop()).collect());
            datas.push(data);
        }
        MultiData(datas)
    }

    pub fn iter(&self) -> impl Iterator<Item = &Data> {
        self.0.iter()
    }
}
