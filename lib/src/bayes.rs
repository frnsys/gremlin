use rand::thread_rng;
use rand_distr::Distribution;
use statrs::distribution::{self as stats, ContinuousCDF};

pub trait Bayes {
    type Distribution: Distribution<f64> + ContinuousCDF<f64, f64>;

    fn update(&mut self, new_data: &[f64]);
    fn distribution(&self) -> Self::Distribution;

    /// Normalized uncertainty.
    // TODO may be preferable to also take variance into consideration?
    fn uncertainty(&self) -> f64;

    fn confidence_interval(&self, confidence: f64) -> (f64, f64) {
        let dist = self.distribution();
        let lower = dist.inverse_cdf((1.0 - confidence) / 2.0);
        let upper = dist.inverse_cdf(1.0 - (1.0 - confidence) / 2.0);
        (lower, upper)
    }

    fn sample(&self) -> f64 {
        let dist = self.distribution();
        let mut rng = thread_rng();
        dist.sample(&mut rng)
    }

    fn sample_n(&self, n: usize) -> Vec<f64> {
        let dist = self.distribution();
        let mut rng = thread_rng();
        (0..n).map(|_| dist.sample(&mut rng)).collect()
    }

    fn sample_confidence(&self, confidence: f64) -> f64 {
        let (lower, upper) = self.confidence_interval(confidence);
        let dist = self.distribution();
        let mut rng = thread_rng();

        // WARN: This can lock up the program if the confidence
        // bounds are impossible to satisfy, e.g. when `lower` is `NaN`.
        loop {
            let sample = dist.sample(&mut rng);
            if sample >= lower && sample <= upper {
                return sample;
            }
        }
    }

    fn sample_confidence_n(&self, confidence: f64, n: usize) -> Vec<f64> {
        let (lower, upper) = self.confidence_interval(confidence);
        let dist = self.distribution();
        let mut rng = thread_rng();

        let mut samples = Vec::with_capacity(n);

        // WARN: This can lock up the program if the confidence
        // bounds are impossible to satisfy, e.g. when `lower` is `NaN`.
        while samples.len() < n {
            let sample = dist.sample(&mut rng);
            if sample >= lower && sample <= upper {
                samples.push(sample);
            }
        }
        samples
    }
}

#[derive(Debug)]
pub struct Gamma {
    /// Shape parameter (prior + observed sum)
    alpha: f64,

    /// Rate parameter (prior + count)
    beta: f64,

    /// Observation count after which we consider
    /// the distribution to be "certain".
    max_alpha: f64,
}
impl Gamma {
    // Initialize with prior shape and rate parameters
    pub fn new(alpha_prior: f64, beta_prior: f64, max_alpha: f64) -> Self {
        Self {
            alpha: alpha_prior,
            beta: beta_prior,
            max_alpha,
        }
    }

    pub fn from_data(data: &[f64], max_alpha: f64) -> Self {
        let n = data.len() as f64;
        let mean: f64 = data.iter().sum::<f64>() / n;
        let variance: f64 = data.iter().map(|&x| (x - mean).powi(2)).sum::<f64>() / n;

        println!("variance: {:?}", variance);
        println!("mean: {:?}", mean);
        if variance < 1e-3 {
            let prior_strength = data.iter().sum::<f64>();
            println!("prior_strength: {:?}", prior_strength);
            let alpha = prior_strength;
            let beta = prior_strength / mean;
            return Self {
                alpha,
                beta,
                max_alpha,
            };
        } else {
            let alpha = mean * mean / variance;
            let beta = mean / variance;

            Self {
                alpha,
                beta,
                max_alpha,
            }
        }
    }
}
impl Bayes for Gamma {
    type Distribution = stats::Gamma;

    fn update(&mut self, new_data: &[f64]) {
        for &x in new_data {
            if x > 0.0 {
                self.alpha += x;
                self.beta += 1.0;
            }
        }
    }

    fn distribution(&self) -> Self::Distribution {
        stats::Gamma::new(self.alpha, 1.0 / self.beta).unwrap()
    }

    // Uncertainty measure (normalized)
    fn uncertainty(&self) -> f64 {
        (1.0 - (self.alpha / self.max_alpha)).max(0.0)
    }

    fn confidence_interval(&self, confidence: f64) -> (f64, f64) {
        let dist = self.distribution();
        let lower = dist.inverse_cdf((1.0 - confidence) / 2.0);
        let upper = dist.inverse_cdf(1.0 - (1.0 - confidence) / 2.0);
        if lower.is_nan() {
            (0., upper)
        } else {
            (lower, upper)
        }
    }
}

#[derive(Debug)]
pub struct Exponential {
    lambda: f64,
    count: f64,
}
impl Exponential {
    pub fn from_data(data: &[f64]) -> Self {
        let sum_x: f64 = data.iter().sum();
        let count = data.len() as f64;
        let lambda = if sum_x > 0.0 { count / sum_x } else { 1.0 };
        Self { lambda, count }
    }
}
impl Bayes for Exponential {
    type Distribution = stats::Exp;

    fn distribution(&self) -> Self::Distribution {
        stats::Exp::new(self.lambda).unwrap()
    }

    fn uncertainty(&self) -> f64 {
        (1.0 / self.count.sqrt()).min(1.0)
    }

    fn update(&mut self, new_data: &[f64]) {
        let sum_x: f64 = new_data.iter().sum();
        let count = new_data.len() as f64;
        if sum_x > 0.0 {
            self.lambda = (self.lambda + count / sum_x) / 2.0;
        }
        self.count += count;
    }
}

#[derive(Debug)]
pub struct LogNormal {
    mu: f64,
    sigma2: f64,
    count: f64,
}

impl LogNormal {
    pub fn from_data(data: &[f64]) -> Self {
        let log_data: Vec<f64> = data.iter().filter(|&&x| x > 0.0).map(|&x| x.ln()).collect();
        let count = log_data.len() as f64;
        let mean_log = log_data.iter().sum::<f64>() / count;
        let variance_log = log_data
            .iter()
            .map(|&x| (x - mean_log).powi(2))
            .sum::<f64>()
            / count;

        Self {
            mu: mean_log,
            sigma2: variance_log,
            count,
        }
    }
}
impl Bayes for LogNormal {
    type Distribution = stats::Normal;

    fn distribution(&self) -> Self::Distribution {
        stats::Normal::new(self.mu, self.sigma2.sqrt()).unwrap()
    }

    fn uncertainty(&self) -> f64 {
        (self.sigma2.sqrt() / self.count.sqrt()).min(1.0)
    }

    fn update(&mut self, new_data: &[f64]) {
        let log_data: Vec<f64> = new_data
            .iter()
            .filter(|&&x| x > 0.0)
            .map(|&x| x.ln())
            .collect();
        let count = log_data.len() as f64;
        let mean_log = log_data.iter().sum::<f64>() / count;
        let variance_log = log_data
            .iter()
            .map(|&x| (x - mean_log).powi(2))
            .sum::<f64>()
            / count;

        self.mu = (self.mu + mean_log) / 2.0;
        self.sigma2 = (self.sigma2 + variance_log) / 2.0;
        self.count += count;
    }
}

#[derive(Debug)]
pub struct Beta {
    /// Prior successes
    alpha: f64,

    /// Prior failures
    beta: f64,
}
impl Beta {
    pub fn new(alpha_prior: f64, beta_prior: f64) -> Self {
        Self {
            alpha: alpha_prior,
            beta: beta_prior,
        }
    }

    pub fn from_mean(mean: f64, pseudo_count: f64) -> Self {
        Self {
            alpha: mean * pseudo_count,
            beta: (1. - mean) * pseudo_count,
        }
    }

    pub fn from_data(data: &[f64]) -> Self {
        let n = data.len() as f64;
        let mean: f64 = data.iter().sum::<f64>() / n;
        let variance: f64 = data.iter().map(|&x| (x - mean).powi(2)).sum::<f64>() / n;

        // Ensure variance is non-zero (to avoid division by zero)
        let adjusted_variance = variance.max(1e-6);

        let common_factor = mean * (1.0 - mean) / adjusted_variance - 1.0;
        let alpha = mean * common_factor;
        let beta = (1.0 - mean) * common_factor;

        Self { alpha, beta }
    }
}
impl Bayes for Beta {
    type Distribution = stats::Beta;

    fn update(&mut self, new_data: &[f64]) {
        for &x in new_data {
            if x >= 0.0 && x <= 1.0 {
                self.alpha += x;
                self.beta += 1.0 - x;
            }
        }
    }

    fn distribution(&self) -> Self::Distribution {
        stats::Beta::new(self.alpha, self.beta).unwrap()
    }

    fn uncertainty(&self) -> f64 {
        let total = self.alpha + self.beta;
        (1.0 / total).min(1.0)
    }
}
