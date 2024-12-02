/// Transform `[a, b] -> (-inf, +inf)`
pub fn logit_gen(x: f64, a: f64, b: f64) -> f64 {
    if x <= a || x >= b {
        f64::NAN
    } else {
        ((x - a) / (b - x)).ln()
    }
}

/// Transform `(-inf, +inf) -> [a, b]`
pub fn inv_logit_gen(y: f64, a: f64, b: f64) -> f64 {
    a + (b - a) / (1.0 + (-y).exp())
}

/// Transform `[a, +inf) -> (-inf, +inf)`
pub fn log_lb(x: f64, a: f64) -> f64 {
    if x <= a {
        f64::NAN
    } else {
        (x - a).ln()
    }
}

/// Transform `(-inf, +inf) -> [a, +inf)`
pub fn inv_log_lb(y: f64, a: f64) -> f64 {
    a + y.exp()
}

/// Transform `(-inf, b] -> (-inf, +inf)`
pub fn log_ub(x: f64, b: f64) -> f64 {
    if x >= b {
        f64::NAN
    } else {
        (b - x).ln()
    }
}

/// Transform `(-inf, +inf) -> (-inf, b]`
pub fn inv_log_ub(y: f64, b: f64) -> f64 {
    b - y.exp()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn approx_eq(a: f64, b: f64) -> bool {
        (a - b).abs() < 1e-6
    }

    #[test]
    fn test_range_transform() {
        let range_lb = 2.;
        let range_ub = 5.;
        let range = range_ub - range_lb;

        for _ in 0..1000 {
            let val = range_lb + (rand::random::<f64>() * range);
            assert!(approx_eq(
                val,
                inv_logit_gen(logit_gen(val, range_lb, range_ub), range_lb, range_ub)
            ));
        }

        let val = 10.;
        assert!(logit_gen(val, range_lb, range_ub).is_nan());
    }

    #[test]
    fn test_lower_bound_transform() {
        let range_lb = 2.;
        let range_ub = 5000.; // Arbitrary upper bound
        let range = range_ub - range_lb;

        for _ in 0..1000 {
            let val = range_lb + (rand::random::<f64>() * range);
            assert!(approx_eq(val, inv_log_lb(log_lb(val, range_lb), range_lb)));
        }

        let val = -5.;
        assert!(log_lb(val, range_lb).is_nan());
    }

    #[test]
    fn test_upper_bound_transform() {
        let range_lb = -2000.; // Arbitrary lower bound
        let range_ub = 5.;
        let range = range_ub - range_lb;

        for _ in 0..1000 {
            let val = range_lb + (rand::random::<f64>() * range);
            assert!(approx_eq(val, inv_log_ub(log_ub(val, range_ub), range_ub)));
        }

        let val = 1000.;
        assert!(log_ub(val, range_ub).is_nan());
    }
}
