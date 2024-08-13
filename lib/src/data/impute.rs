//! Defines common ways to impute missing data.
//!
//! The [`PmlModel`](crate::model::PmlModel) can also be used to impute data based
//! on the learned distribution(s); these methods are more naive
//! for when you need to impute values *before* fitting the `PmlModel`.

use anyhow::Result;
use polars::{lazy::dsl::*, prelude::*};
use serde::{Deserialize, Serialize};

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
pub fn impute(
    mut df: DataFrame,
    column: &str,
    strategy: &ImputeStrategy,
) -> Result<DataFrame> {
    // For imputing, ensure the column is float.
    df.with_column(df[column].cast(&DataType::Float64)?)?;
    match strategy {
        ImputeStrategy::All(fill) => {
            df.with_column(
                df[column].fill_null((*fill).into())?,
            )?;
        }
        ImputeStrategy::Group { column, strategy } => {
            // First we need to identify which groups
            // have *no* reference values for the column,
            // in which case we need to use the overall dataframe
            // mean/min/max/etc.
            df = df
                .lazy()
                .with_column(
                    when(
                        col(column)
                            .drop_nulls()
                            .len()
                            .over([col(column)])
                            .eq(0),
                    )
                    .then(match strategy {
                        FillWith::One => lit(1.),
                        FillWith::Zero => lit(0.),
                        FillWith::Min => col(column).min(),
                        FillWith::Max => col(column).max(),
                        FillWith::Mean => col(column).mean(),
                    })
                    .otherwise(col(column)),
                )
                .collect()?;

            df = df
                .lazy()
                .with_column(
                    col(column)
                        .fill_null(match strategy {
                            FillWith::One => lit(1.),
                            FillWith::Zero => lit(0.),
                            FillWith::Min => col(column)
                                .min()
                                .over([col(column)]),
                            FillWith::Max => col(column)
                                .max()
                                .over([col(column)]),
                            FillWith::Mean => col(column)
                                .mean()
                                .over([col(column)]),
                        })
                        .alias(column),
                )
                .collect()?;
        }
    }
    Ok(df)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_impute() -> Result<()> {
        let df =
            CsvReader::from_path("assets/tests/impute.csv")?
                .has_header(true)
                .finish()?;

        let imputed = impute(
            df,
            "B",
            &ImputeStrategy::Group {
                column: "A".into(),
                strategy: FillWith::Mean,
            },
        )?;

        let expected_a = (0. + 1.) / 2.;
        let expected_b = (5. + 10.) / 2.;
        let expected_c = (0. + 1. + 5. + 10.) / 4.;
        let expected = df! {
            "A" => ["a", "a", "a", "b", "b", "b", "c", "c"],
            "B" => [0., 1., expected_a, 5., expected_b, 10., expected_c, expected_c]
        }?;
        assert_eq!(imputed, expected);

        Ok(())
    }
}
