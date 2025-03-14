use std::fmt;

use number_prefix::NumberPrefix;

/// Formats count for human readability using SI prefixes
///
/// # Examples
/// ```rust
/// # use plsync::DecimalCount;
/// assert_eq!("15 ",    format!("{}", DecimalCount(15.0)));
/// assert_eq!("1.50 k", format!("{}", DecimalCount(1_500.0)));
/// assert_eq!("1.50 M", format!("{}", DecimalCount(1_500_000.0)));
/// assert_eq!("1.50 G", format!("{}", DecimalCount(1_500_000_000.0)));
/// assert_eq!("1.50 T", format!("{}", DecimalCount(1_500_000_000_000.0)));
/// assert_eq!("1.50 P", format!("{}", DecimalCount(1_500_000_000_000_000.0)));
/// ```
#[derive(Debug)]
pub struct DecimalCount(pub f64);

impl fmt::Display for DecimalCount {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match NumberPrefix::decimal(self.0) {
            NumberPrefix::Standalone(number) => write!(f, "{number:.0} "),
            NumberPrefix::Prefixed(prefix, number) => write!(f, "{number:.2} {prefix}"),
        }
    }
}