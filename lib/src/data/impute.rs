//! Defines common ways to impute missing data.
//!
//! The [`PmlModel`](crate::model::PmlModel) can also be used to impute data based
//! on the learned distribution(s); these methods are more naive
//! for when you need to impute values *before* fitting the `PmlModel`.

use polars::{lazy::dsl::*, prelude::*};
use serde::{Deserialize, Serialize};

use super::error::DataResult;

/// Strategy to use for imputing missing values.
/// This enum defines the relevant reference for
/// deriving the fill values; see [`FillWith`] for
/// the actual strategies for computing the fill values.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ImputeStrategy {
    /// The fill null strategy's value
    /// will be derived from the entire dataframe.
    /// For example, if you specify [`FillWith::Mean`]
    /// it will take the mean of *all* the rows.
    All(FillWith),

    /// The fill null strategy's value
    /// will derived per group, using the `column`
    /// for creating groups. For example, with [`FillWith::Mean`],
    /// groups A and B the mean of group A will be used
    /// to fill its missing values and correspondingly
    /// the mean for group B will be used for its own missing
    /// values.
    Group { column: String, strategy: FillWith },
}

/// Defines how we compute the fill values.
#[derive(Debug, Copy, Clone, Serialize, Deserialize)]
pub enum FillWith {
    /// Fill all missing values with `1.`.
    One,

    /// Fill all missing values with `0.`.
    Zero,

    /// Fill missing values the column mean.
    Mean,

    /// Fill missing values the column median.
    Median,

    /// Fill missing values the column minimum.
    Min,

    /// Fill missing values the column maximum.
    Max,
}
impl From<FillWith> for FillNullStrategy {
    fn from(val: FillWith) -> Self {
        match val {
            FillWith::One => FillNullStrategy::One,
            FillWith::Zero => FillNullStrategy::Zero,
            FillWith::Mean => FillNullStrategy::Mean,
            FillWith::Min => FillNullStrategy::Min,
            FillWith::Max => FillNullStrategy::Max,

            // WARN: There is no "median" equivalent fill null strategy in polars.
            FillWith::Median => FillNullStrategy::Mean,
        }
    }
}

/// Impute the column `column` using the specified [`ImputeStrategy`].
///
/// # Assumptions
///
/// - This assumes that all columns are floats (`f64`).
///
/// # Note
///
/// When using [`ImputeStrategy::Group`] with a
/// non-constant/computed fill value, e.g. [`FillWith::Mean`],
/// if a group has no reference values the corresponding value of
/// the entire dataframe will be used instead. For example, say we have
/// two groups A and B and [`FillWith::Mean`] and we're imputing over
/// some column `x`. 4 out of 10 of group A are missing values for `x`,
/// so in that case we can impute the missing values using the mean of the
/// 6 available values. However for group B all of its rows are missing
/// values for `x`, so there is nothing to take the mean of. In this case
/// we instead use the mean of `x` for the entire dataframe (which in this
/// particular case is equivalent to the mean of the non-missing values of group A).
pub fn impute(mut df: DataFrame, column: &str, strategy: &ImputeStrategy) -> DataResult<DataFrame> {
    // For imputing, ensure the column is float.
    df.with_column(df[column].cast(&DataType::Float64)?)?;
    match strategy {
        ImputeStrategy::All(strategy) => {
            df = df
                .lazy()
                .with_column(
                    col(column)
                        .fill_null(match strategy {
                            FillWith::One => lit(1.),
                            FillWith::Zero => lit(0.),
                            FillWith::Min => col(column).drop_nulls().min(),
                            FillWith::Max => col(column).drop_nulls().max(),
                            FillWith::Mean => col(column).drop_nulls().mean(),
                            FillWith::Median => col(column).drop_nulls().median(),
                        })
                        .alias(column),
                )
                .collect()?;
        }
        ImputeStrategy::Group {
            column: group_col,
            strategy,
        } => {
            df = df
                .lazy()
                .with_column(
                    when(col(column).is_null().all(false).over([col(group_col)]))
                        // If all values in the group are null, use the global mean
                        .then(match strategy {
                            FillWith::One => lit(1.),
                            FillWith::Zero => lit(0.),
                            FillWith::Min => col(column).drop_nulls().min(),
                            FillWith::Max => col(column).drop_nulls().max(),
                            FillWith::Mean => col(column).drop_nulls().mean(),
                            FillWith::Median => col(column).drop_nulls().median(),
                        })
                        .otherwise(match strategy {
                            FillWith::One => lit(1.),
                            FillWith::Zero => lit(0.),
                            FillWith::Min => col(column).min().over([col(group_col)]),
                            FillWith::Max => col(column).max().over([col(group_col)]),
                            FillWith::Mean => col(column).mean().over([col(group_col)]),
                            FillWith::Median => col(column).median().over([col(group_col)]),
                        })
                        .alias("group_mean"),
                )
                .with_column(col(column).fill_null(col("group_mean")).alias(column))
                .drop(["group_mean"])
                .collect()?;
        }
    }
    Ok(df)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_impute_all(strat: FillWith, expected: f32) -> DataResult<()> {
        let df = CsvReader::from_path("assets/tests/impute.csv")?
            .has_header(true)
            .finish()?;

        let imputed = impute(df, "B", &ImputeStrategy::All(strat))?;

        let expected = df! {
            "A" => ["a", "a", "a", "b", "b", "b", "b", "c", "c"],
            "B" => [0., 1., expected, 5., expected, 10., 9., expected, expected]
        }?;
        assert_eq!(imputed, expected);

        Ok(())
    }

    #[test]
    fn test_impute_all_mean() -> DataResult<()> {
        let expected = (0. + 1. + 5. + 9. + 10.) / 5.;
        test_impute_all(FillWith::Mean, expected)
    }

    #[test]
    fn test_impute_all_median() -> DataResult<()> {
        test_impute_all(FillWith::Median, 5.)
    }

    #[test]
    fn test_impute_all_min() -> DataResult<()> {
        test_impute_all(FillWith::Min, 0.)
    }

    #[test]
    fn test_impute_all_max() -> DataResult<()> {
        test_impute_all(FillWith::Max, 10.)
    }

    #[test]
    fn test_impute_all_zero() -> DataResult<()> {
        test_impute_all(FillWith::Zero, 0.)
    }

    #[test]
    fn test_impute_all_one() -> DataResult<()> {
        test_impute_all(FillWith::One, 1.)
    }

    fn test_impute_by_group(
        strat: FillWith,
        (ex_a, ex_b, ex_c): (f32, f32, f32),
    ) -> DataResult<()> {
        let df = CsvReader::from_path("assets/tests/impute.csv")?
            .has_header(true)
            .finish()?;

        let imputed = impute(
            df,
            "B",
            &ImputeStrategy::Group {
                column: "A".into(),
                strategy: strat,
            },
        )?;

        let expected = df! {
            "A" => ["a", "a", "a", "b", "b", "b", "b", "c", "c"],
            "B" => [0., 1., ex_a, 5., ex_b, 10., 9., ex_c, ex_c]
        }?;
        assert_eq!(imputed, expected);

        Ok(())
    }

    #[test]
    fn test_impute_group_mean() -> DataResult<()> {
        let expected_a = (0. + 1.) / 2.;
        let expected_b = (5. + 10. + 9.) / 3.;
        let expected_c = (0. + 1. + 5. + 9. + 10.) / 5.;
        test_impute_by_group(FillWith::Mean, (expected_a, expected_b, expected_c))
    }

    #[test]
    fn test_impute_group_median() -> DataResult<()> {
        let expected_a = (0. + 1.) / 2.;
        let expected_b = 9.;
        let expected_c = 5.;
        test_impute_by_group(FillWith::Median, (expected_a, expected_b, expected_c))
    }

    #[test]
    fn test_impute_group_min() -> DataResult<()> {
        test_impute_by_group(FillWith::Min, (0., 5., 0.))
    }

    #[test]
    fn test_impute_group_max() -> DataResult<()> {
        test_impute_by_group(FillWith::Max, (1., 10., 10.))
    }

    #[test]
    fn test_impute_group_zero() -> DataResult<()> {
        test_impute_by_group(FillWith::Zero, (0., 0., 0.))
    }

    #[test]
    fn test_impute_group_one() -> DataResult<()> {
        test_impute_by_group(FillWith::One, (1., 1., 1.))
    }
}
