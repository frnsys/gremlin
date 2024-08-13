use std::{marker::PhantomData, path::Path};

use generic_array::{
    functional::FunctionalSequence,
    GenericArray,
};
use itertools::Itertools;

use crate::core::{Annual, Interval, Numeric, TimeSeries};

pub enum MissingStrategy<T: Default + Clone> {
    Skip,
    Default(T),
}

pub enum MergingStrategy {
    Ignore,
    Mean,
    Median,
}

/// Convenience function for reading a time series
/// from a CSV. Ignores missing values.
pub fn read_series_csv<T, S, N: Numeric, I: Interval>(
    value_col: &str,
    time_cols: T,
    path: &Path,
) -> anyhow::Result<TimeSeries<N, I>>
where
    T: IntoIterator<Item = S>,
    S: AsRef<str>,
{
    let time_cols = time_cols
        .into_iter()
        .map(|s| s.as_ref().to_string())
        .collect();

    let reader = TimeSeriesReader {
        missing_strategy: MissingStrategy::Skip,
        merging_strategy: MergingStrategy::Ignore,
        time_cols,
        value_col: value_col.into(),
        marker: PhantomData::<I>,
    };
    let items: Vec<(I::Key, N)> = reader.read(path)?;
    Ok(TimeSeries::from(items.into_iter()))
}

pub struct TimeSeriesReader<N: Numeric, I: Interval> {
    missing_strategy: MissingStrategy<N>,
    merging_strategy: MergingStrategy,
    time_cols: GenericArray<String, I::KeySize>,
    value_col: String,
    marker: PhantomData<I>,
}
impl<N: Numeric, I: Interval> TimeSeriesReader<N, I> {
    pub fn with_time<T, S>(mut self, columns: T) -> Self
    where
        T: IntoIterator<Item = S>,
        S: AsRef<str>,
    {
        self.time_cols = columns
            .into_iter()
            .map(|s| s.as_ref().to_string())
            .collect();
        self
    }

    pub fn with_value(mut self, name: &str) -> Self {
        self.value_col = name.into();
        self
    }

    pub fn with_default(mut self, default: N) -> Self {
        self.missing_strategy =
            MissingStrategy::Default(default);
        self
    }

    pub fn read(
        &self,
        path: &Path,
    ) -> anyhow::Result<Vec<(I::Key, N)>> {
        let mut rdr = csv::Reader::from_path(path)?;
        let headers =
            rdr.headers().expect("Should have headers");
        let time_idxs: GenericArray<usize, I::KeySize> =
            headers
                .iter()
                .enumerate()
                .filter(|(_, field)| {
                    self.time_cols.contains(&field.to_string())
                })
                .map(|(i, _)| i)
                .collect();
        assert_eq!(time_idxs.len(), self.time_cols.len());

        let value_idx = headers
            .iter()
            .position(|field| field == self.value_col)
            .expect("Should have value column");

        let mut results = vec![];
        for result in rdr.records() {
            let record = result?;
            let raw_keys: GenericArray<String, I::KeySize> =
                time_idxs
                    .iter()
                    .map(|i| record[*i].to_string())
                    .collect();
            let time = raw_keys.map(|k| k.parse().unwrap());
            let time: I::Key =
                time.into_iter().collect_tuple().unwrap();
            let value = record.get(value_idx);
            match value {
                Some(value) => {
                    let parsed =
                        value.parse::<N>().map_err(|_err| {
                            anyhow::Error::msg(format!(
                                "Failed to parse value: {:?}",
                                value
                            ))
                        })?;
                    results.push((time, parsed));
                }
                None => match &self.missing_strategy {
                    MissingStrategy::Skip => continue,
                    MissingStrategy::Default(value) => {
                        results.push((time, value.clone()));
                    }
                },
            }
        }
        Ok(results)
    }
}

pub type AnnualReader<N> = TimeSeriesReader<N, Annual>;

impl<N: Numeric> Default for TimeSeriesReader<N, Annual> {
    fn default() -> Self {
        Self {
            missing_strategy: MissingStrategy::Skip,
            merging_strategy: MergingStrategy::Median,
            time_cols: ["Year".to_string()].into(),
            value_col: "Value".into(),
            marker: PhantomData,
        }
    }
}
