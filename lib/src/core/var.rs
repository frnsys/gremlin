//! Exogenous variable utilities.

use std::collections::VecDeque;

use tracing::warn;

#[derive(Debug, Clone, Default, PartialEq)]
pub enum Fallback {
    /// Repeat the last available value.
    #[default]
    Repeat,

    /// Linearly project based on the provided data.
    Linear,
}

/// Exogenous variables are basically queues of pre-defined values,
/// with one value used per step. If the queue is exhausted before
/// the simulation finishes then subsequent steps will re-use the last value.
#[derive(Debug, Clone)]
pub struct ExogVariable<T: Clone + From<f32>>
where
    f32: From<T>,
{
    i: usize,
    values: Vec<f32>,
    queue: VecDeque<T>,
    fallback: Fallback,
}
impl<T: Clone + From<f32>> ExogVariable<T>
where
    f32: From<T>,
{
    /// At least one value must be provided.
    pub fn new(vals: Vec<T>, fallback: Fallback) -> Self {
        if vals.is_empty() {
            panic!("Cannot initialize an exogenous variable with no provided values.");
        }

        Self {
            i: 0,
            values: vals.iter().cloned().map(f32::from).collect(),
            queue: vals.into(),
            fallback,
        }
    }

    /// Get the value defined for the next step.
    /// If the queue would be exhausted then the last value is re-used.
    pub fn next_value(&mut self) -> T {
        self.i += 1;
        if self.queue.len() == 1 && self.fallback == Fallback::Repeat {
            warn!("Exogenous variable will be exhausted. Will re-use the last available value.");
            self.queue[0].clone()
        } else if self.queue.is_empty() {
            linear_estimate(&self.values, self.i - 1).into()
        } else {
            self.queue
                .pop_front()
                .expect("We checked that valid values remain")
        }
    }

    pub fn is_exhausted(&self) -> bool {
        self.queue.len() <= 1
    }

    pub fn nth_value(&self, n: usize) -> &T {
        if n >= self.queue.len() {
            &self.queue[0]
        } else {
            &self.queue[n]
        }
    }
}

fn linear_estimate(data: &[f32], n: usize) -> f32 {
    let len = data.len();
    if len == 0 {
        return 0.0;
    }

    let n = n as f32;

    let mean_x = (len - 1) as f32 / 2.0;
    let mean_y = data.iter().sum::<f32>() / len as f32;

    let mut numerator = 0.0;
    let mut denominator = 0.0;

    for (i, &y) in data.iter().enumerate() {
        let x = i as f32;
        numerator += (x - mean_x) * (y - mean_y);
        denominator += (x - mean_x).powi(2);
    }

    let a = if denominator != 0.0 {
        numerator / denominator
    } else {
        0.0
    };
    let b = mean_y - a * mean_x;
    a * n + b
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_exog_variable() {
        let data = vec![1, 2, 3];
        let mut exog = ExogVariable::new(data, Fallback::Repeat);

        assert!(!exog.is_exhausted());
        assert_eq!(exog.next_value(), 1);
        assert_eq!(exog.next_value(), 2);
        assert_eq!(exog.next_value(), 3);
        assert_eq!(exog.next_value(), 3);
        assert_eq!(exog.next_value(), 3);
        assert_eq!(exog.next_value(), 3);
        assert_eq!(exog.next_value(), 3);
        assert!(exog.is_exhausted());
    }
}
