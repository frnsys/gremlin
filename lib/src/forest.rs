use ndarray::{Array1, Array2};
use polars::prelude::*;
use serde::{Deserialize, Serialize};
use smartcore::{
    ensemble::random_forest_regressor::RandomForestRegressor,
    metrics::{mean_absolute_error, mean_squared_error, r2},
};
use std::{collections::HashMap, path::Path};
use thiserror::Error;

#[derive(Debug, Error)]
pub enum ForestError {
    #[error("Polars error: {0}")]
    Polars(#[from] PolarsError),

    #[error("Fit failed: {0}")]
    FitFailed(#[from] smartcore::error::Failed),

    #[error("Serialization error: {0}")]
    Serialize(#[from] Box<bincode::ErrorKind>),

    #[error("IO error: {0}")]
    IO(#[from] std::io::Error),

    #[error("Wrong input shape: {0}")]
    WrongShape(#[from] ndarray::ShapeError),
}

fn build_features(df: &DataFrame, columns: &[String]) -> Result<Array2<f64>, ForestError> {
    let cols: Vec<_> = columns.iter().map(|name| col(name)).collect();
    let df = df.clone().lazy().select(cols).collect()?;
    let data: Vec<f64> = df
        .get_columns()
        .iter()
        .flat_map(|series| series.f64().unwrap().into_iter().map(|x| x.unwrap()))
        .collect();
    let shape = (df.height(), df.width());
    let arr = Array2::from_shape_vec(shape, data).expect("Shape is correct");
    Ok(arr)
}

fn build_target(df: &DataFrame, column: &str) -> Result<Array1<f64>, ForestError> {
    let df = df.clone().lazy().select([col(column)]).collect()?;

    let data: Vec<f64> = df
        .get_columns()
        .iter()
        .flat_map(|series| series.f64().unwrap().into_iter().map(|x| x.unwrap()))
        .collect();

    Ok(Array1::from(data))
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Forest {
    models: HashMap<String, RandomForestRegressor<f64, f64, Array2<f64>, Array1<f64>>>,
    n_features: usize,
}
impl Forest {
    pub fn fit(
        df: DataFrame,
        feature_cols: &[String],
        target_cols: &[String],
    ) -> Result<Self, ForestError> {
        let mut models = HashMap::default();
        let n_features = feature_cols.len();

        let features_arr = build_features(&df, feature_cols)?;
        for col in target_cols {
            let target_arr = build_target(&df, col)?;

            // let (x_train, x_test, y_train, y_test) = smartcore::model_selection::train_test_split(
            //     &features_arr,
            //     &target_arr,
            //     0.05,
            //     true,
            //     None,
            // );
            let model = RandomForestRegressor::fit(&features_arr, &target_arr, Default::default())?;

            // Evaluation
            let y_hat_rf = model.predict(&features_arr)?;
            tracing::debug!("[{col}] R2 : {}", r2(&target_arr, &y_hat_rf));
            tracing::debug!(
                "[{col}] MSE: {}",
                mean_squared_error(&target_arr, &y_hat_rf)
            );
            tracing::debug!(
                "[{col}] MAE: {}",
                mean_absolute_error(&target_arr, &y_hat_rf)
            );

            models.insert(col.to_string(), model);
        }
        Ok(Self { models, n_features })
    }

    pub fn predict(&self, features: Vec<Vec<f64>>) -> Result<HashMap<&str, Vec<f64>>, ForestError> {
        let n_rows = features.len();
        let data: Vec<f64> = features.into_iter().flatten().collect();
        let shape = (n_rows, self.n_features);
        let arr = Array2::from_shape_vec(shape, data)?;

        let mut results: HashMap<&str, Vec<f64>> = HashMap::default();
        for (col, model) in &self.models {
            let y = model.predict(&arr)?;
            results.insert(col, y.to_vec());
        }

        Ok(results)
    }

    pub fn save(&self, path: &Path) -> Result<(), ForestError> {
        let data = bincode::serialize(&self)?;
        std::fs::write(path, data)?;
        Ok(())
    }

    pub fn load(path: &Path) -> Result<Self, ForestError> {
        let data = std::fs::read(path)?;
        let result: Self = bincode::deserialize(&data)?;
        Ok(result)
    }
}
