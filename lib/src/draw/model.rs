//! The model underlying a [`Sampler`].
//! In short it learns a joint distribution over some
//! set of variables.

use std::{
    fs,
    path::{Path, PathBuf},
};

use crate::file::write_yaml;
use ahash::HashMap;
use anyhow::Result;
use lace::{prelude::*, update_handler::ProgressBar, Oracle};
use polars::{io::RowCount, prelude::*};
use rand::SeedableRng;
use rand_xoshiro::Xoshiro256Plus;
use serde::{Deserialize, Serialize};
use tracing::{info, warn};

#[cfg(feature = "plotting")]
use infra_plot::{
    CatScatterItem,
    Plots,
    ScatterItem,
    StaticChart,
    Symbol,
};

use super::sampler::{Column, Sampler, SamplerError};
use crate::data::impute::*;

/// The probabilistic ML model, for training.
pub struct PmlModel {
    /// Where to save to/load an existing model from.
    pub save_dir: PathBuf,

    /// The columns (variables) that we're using.
    columns: Vec<ImputedColumn>,

    /// The underlying Lace engine.
    engine: Engine,

    /// Sampler to generate samples.
    sampler: Option<Sampler>,

    /// Is this model trained or not?
    pub is_new: bool,
}

/// Like [`Column`] but additionally
/// takes an optional imputation strategy for filling missing values.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ImputedColumn {
    // Flatten so this also deserializes to `Column`.
    #[serde(flatten)]
    pub column: Column,
    pub impute_with: Option<ImputeStrategy>,
}
impl ImputedColumn {
    pub fn new(
        column: Column,
        impute_with: Option<ImputeStrategy>,
    ) -> ImputedColumn {
        ImputedColumn {
            column,
            impute_with,
        }
    }
}
impl std::ops::Deref for ImputedColumn {
    type Target = Column;
    fn deref(&self) -> &Self::Target {
        &self.column
    }
}

impl PmlModel {
    /// Create a new PML model from a CSV
    /// with the specified columns. The resulting model
    /// will be saved to the specified `save_dir`.
    pub fn from_csv(
        csv_path: &Path,
        columns: &[ImputedColumn],
        save_dir: PathBuf,
    ) -> Result<PmlModel> {
        let cols: Vec<_> = columns
            .iter()
            .map(|col| col.name.to_string())
            .collect();
        let mut df = CsvReader::from_path(csv_path)?
            .has_header(true)
            .with_row_count(Some(RowCount {
                name: "ID".into(),
                offset: 0,
            }))
            .with_columns(Some(cols))
            .finish()?;

        for column in columns {
            let name = column.name.as_str();
            let nulls = df[name].null_count();
            if nulls > 0 {
                let n = df[name].len();
                let percent = (nulls as f32 / n as f32) * 100.;

                if let Some(impute_strat) = &column.impute_with
                {
                    info!(
                        "Column \"{name}\" has {nulls} ({percent:.2}%) null values, imputing with: {:?}.",
                        impute_strat
                    );
                    df = impute(df, name, impute_strat)?;
                } else {
                    warn!(
                        "Column \"{name}\" has {nulls} ({percent:.2}%) null values but no imputation strategy was provided."
                    );
                }
            }
        }

        Ok(PmlModel {
            columns: columns.to_vec(),
            engine: prepare_engine(&save_dir, df)?,
            is_new: !save_dir.exists(),
            sampler: None,
            save_dir,
        })
    }

    /// Fit/train the model. This isn't necessary
    /// if the model has already been fit and loaded.
    pub fn fit(&mut self, iters: usize) {
        let run_config = EngineUpdateConfig::new()
            .n_iters(iters)
            .transitions(vec![
                // See <https://github.com/promised-ai/lace/issues/148>
                // > Currently Engine::run uses slice for row and column transitions, which can be slow to converge for large tables because they don't propose large moves. We should use both slice and sams on the rows and gibbs as default for columns.
                StateTransition::ColumnAssignment(
                    ColAssignAlg::Gibbs,
                ),
                StateTransition::StateAlpha,
                StateTransition::RowAssignment(
                    RowAssignAlg::Sams,
                ),
                StateTransition::ComponentParams,
                StateTransition::RowAssignment(
                    RowAssignAlg::Slice,
                ),
                StateTransition::ComponentParams,
                StateTransition::ViewAlphas,
                StateTransition::FeaturePriors,
            ]);
        self.engine
            .update(run_config.clone(), ProgressBar::new())
            .unwrap();

        // Create missing parent directories if needed
        fs::create_dir_all(
            self.save_dir
                .parent()
                .expect("Has a parent directory"),
        )
        .expect("Can create parent directories");
        self.engine
            .save(&self.save_dir, SerializedType::Yaml)
            .unwrap();
        write_yaml(
            &self.columns,
            &self.save_dir.join("columns.yml"),
        );
    }

    /// Plot the MCMC log-likelihoods of the model,
    /// to verify if the model has converged.
    #[cfg(feature = "plotting")]
    pub fn plot_convergences(
        &self,
        save_path: &Path,
    ) -> Result<()> {
        let pattern = self.save_dir.join("*.diagnostics.csv");
        let paths =
            glob::glob(pattern.as_os_str().to_str().unwrap())
                .expect("Failed to read glob pattern")
                .collect::<Result<Vec<PathBuf>, _>>()?;

        let lines: Vec<(String, Vec<f32>)> = paths
            .into_iter()
            .enumerate()
            .map(|(i, path)| {
                let vals: Vec<f32> =
                    CsvReader::from_path(path)?
                        .has_header(true)
                        .finish()?
                        .column("loglike")?
                        .iter()
                        .map(|v| {
                            v.try_extract::<f32>().unwrap()
                        })
                        .collect();
                Ok((i.to_string(), vals))
            })
            .collect::<Result<Vec<_>>>()?;

        let labels: Vec<_> = lines[0]
            .1
            .iter()
            .enumerate()
            .map(|(i, _)| i.to_string())
            .collect();
        StaticChart::<()>::timeseries(lines, &labels, "")
            .with_title("PML MCMC Convergence Plots")
            .render((1200, 800), save_path);
        Ok(())
    }

    /// Plot the dependency matrix, which shows the relationship
    /// (specifically the [dependency probability](https://www.lace.dev/pcc/depprob.html))
    /// between two variables.
    #[cfg(feature = "plotting")]
    pub fn plot_dep_matrix(
        &self,
        save_path: &Path,
    ) -> Result<()> {
        let cols: Vec<_> =
            self.columns.iter().skip(1).collect();
        let oracle = Oracle::from_engine(self.engine.clone());
        let mut data = vec![];
        for (i, col) in cols.iter().enumerate() {
            for (j, _col_) in cols.iter().enumerate() {
                if i == j {
                    data.push((i, j, 0.));
                } else {
                    data.push((
                        i,
                        j,
                        oracle.depprob(
                            col.name.clone(),
                            col.name.clone(),
                        )? as f32,
                    ));
                }
            }
        }

        let col_names: Vec<_> = self
            .columns
            .iter()
            .map(|c| c.name.as_str())
            .collect();
        StaticChart::<()>::correlation(&col_names, data)
            .with_title("PML Dependency Matrix")
            .render((1200, 800), save_path);

        Ok(())
    }

    pub fn sample<'a>(
        &'a mut self,
        n_samples: usize,
        given: &[(&str, Datum)],
        columns: &[&'a str],
    ) -> Result<Vec<HashMap<&str, f32>>, SamplerError> {
        if self.sampler.is_none() {
            let columns = self
                .columns
                .iter()
                .map(|c| c.column.clone())
                .collect();
            self.sampler = Some(Sampler::new(
                self.engine.clone(),
                columns,
            ));
        }
        self.sampler
            .as_ref()
            .expect("Sampler has been initialized")
            .sample_columns(n_samples, given, columns)
    }

    /// Summarize the columns/variables in the data.
    pub fn summarize(&self) {
        let oracle = Oracle::from_engine(self.engine.clone());
        for col in &self.columns {
            let summary =
                oracle.summarize_col(col.name.clone()).unwrap();
            println!("{:?} -> {:?}", col, summary);
        }
    }
}

/// Load an existing engine or initialize a new one.
fn prepare_engine(
    save_dir: &Path,
    df: DataFrame,
) -> Result<Engine> {
    if save_dir.exists() {
        Ok(Engine::load(save_dir)?)
    } else {
        let rng = Xoshiro256Plus::from_entropy();
        let codebook =
            Codebook::from_df(&df, None, None, false).unwrap();
        Ok(Engine::new(
            32, // n_states
            codebook,
            DataSource::Polars(df),
            0,
            rng,
        )?)
    }
}

/// Macro for defining an [`ImputedColumn`].
#[macro_export]
macro_rules! icol {
    ($name: literal) => {
        $crate::draw::ImputedColumn::new($crate::col!($name), None)
    };
    ($name: literal, $impute:expr) => {
        $crate::draw::ImputedColumn::new($crate::col!($name), Some($impute))
    };
    ($name: literal, $impute:expr, $($constraint:tt)*) => {
        $crate::draw::ImputedColumn::new($crate::col!($name, $($constraint)*), Some($impute))
    };
    ($name: literal, $($constraint:tt)*) => {
        $crate::draw::ImputedColumn::new($crate::col!($name, $($constraint)*), None)
    };
}

/// Macro for conveniently defining a [`Column`].
#[macro_export]
macro_rules! col {
    ($name: literal) => {
        $crate::draw::Column::new($name.to_string(), None)
    };
    ($name: literal, $($constraint:tt)*) => {
        $crate::draw::Column::new($name.to_string(), Some($crate::constraint!($($constraint)*)))
    };
}

/// Macro for conveniently defining a [`Constraint`].
#[macro_export]
macro_rules! constraint {
    (>$val: expr) => {
        $crate::draw::Constraint::LowerBound($val)
    };
    (>=$val: expr) => {
        $crate::draw::Constraint::LowerBoundInclusive($val)
    };
    (<$val: expr) => {
        $crate::draw::Constraint::UpperBound($val)
    };
    (<=$val: expr) => {
        $crate::draw::Constraint::UpperBoundInclusive($val)
    };
    ($val_a: expr => $val_b: expr) => {
        $crate::draw::Constraint::Range($val_a, $val_b)
    };
    ($val_a: expr => $val_b: expr) => {
        $crate::draw::Constraint::RangeInclusive($val_a, $val_b)
    };
}

/// Draw samples for each group and plot their properties.
#[cfg(feature = "plotting")]
pub fn plot_pml_samples(
    model: &mut PmlModel,
    pml_input_path: &Path,
    group_col: &str,
    sample_cols: &[&str],
    plot_groups: &[Vec<&str>],
    plots: &mut Plots,
) -> Result<()> {
    let df = CsvReader::from_path(pml_input_path)?
        .has_header(true)
        .finish()?;
    let series =
        df.column(group_col).unwrap().unique_stable().unwrap();
    let group_names: Vec<&str> =
        series.str().unwrap().into_no_null_iter().collect();

    for group in group_names {
        info!("Sampling for {}", group);
        let sample = model.sample(
            200,
            &[(group_col, Datum::Categorical(group.into()))],
            sample_cols,
        )?;

        let mut datas: HashMap<&str, Vec<f32>> =
            HashMap::default();
        for row in sample {
            for (col, val) in row.iter() {
                let v = datas.entry(col).or_default();
                v.push(*val);
            }
        }

        for (i, plot_group) in plot_groups.iter().enumerate() {
            let mut scatter_group = vec![];
            for column in plot_group {
                let values = datas.remove(column).unwrap();
                let ref_vals_df = df
                    .clone()
                    .lazy()
                    .filter(
                        polars::prelude::col(group_col)
                            .eq(polars::prelude::lit(group)),
                    )
                    .collect()
                    .unwrap();

                let ref_vals_series = ref_vals_df
                    .column(column)
                    .unwrap()
                    .cast(&DataType::Float64)
                    .unwrap();
                let ref_vals = ref_vals_series
                    .f64()
                    .unwrap()
                    .into_no_null_iter()
                    .map(|v| v as f32)
                    .collect();
                scatter_group.push(CatScatterItem {
                    var: column.to_string(),
                    units: "",
                    groups: vec![
                        ScatterItem {
                            name: "References".to_string(),
                            symbol: Some(Symbol::Triangle),
                            values: ref_vals,
                        },
                        ScatterItem::from((*column, values)),
                    ],
                });
            }
            let fname = format!("sampledvars_{group}_{i}.png");
            plots.insert(&fname, || {
                StaticChart::cat_scatter(scatter_group, false)
                    .with_title(&format!("Sample: {group}"))
            });
        }
    }
    Ok(())
}
