//! Exogenous variable utilities.

use std::collections::VecDeque;

use tracing::warn;

/// Exogenous variables are basically queues of pre-defined values,
/// with one value used per step. If the queue is exhausted before
/// the simulation finishes then subsequent steps will re-use the last value.
pub struct ExogVariable<T: Clone> {
    queue: VecDeque<T>,
}
impl<T: Clone> ExogVariable<T> {
    /// At least one value must be provided.
    pub fn new(vals: Vec<T>) -> Self {
        if vals.is_empty() {
            panic!("Cannot initialize an exogenous variable with no provided values.");
        }

        Self { queue: vals.into() }
    }

    /// Get the value defined for the next step.
    /// If the queue would be exhausted then the last value is re-used.
    pub fn next(&mut self) -> T {
        if self.queue.len() == 1 {
            warn!("Exogenous variable will be exhausted. Will re-use the last available value.");
            self.queue[0].clone()
        } else {
            self.queue
                .pop_front()
                .expect("We checked that valid values remain")
        }
    }

    pub fn is_exhausted(&self) -> bool {
        self.queue.len() <= 1
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_exog_variable() {
        let data = vec![1, 2, 3];
        let mut exog = ExogVariable::new(data);

        assert!(!exog.is_exhausted());
        assert_eq!(exog.next(), 1);
        assert_eq!(exog.next(), 2);
        assert_eq!(exog.next(), 3);
        assert_eq!(exog.next(), 3);
        assert_eq!(exog.next(), 3);
        assert_eq!(exog.next(), 3);
        assert_eq!(exog.next(), 3);
        assert!(exog.is_exhausted());
    }
}
