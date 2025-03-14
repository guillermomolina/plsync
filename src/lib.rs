mod sync;
mod format;

use rayon::{ThreadPoolBuildError, ThreadPoolBuilder};
pub use sync::*;

pub use crate::format::DecimalCount;

pub fn set_thread_pool(num_threads: usize) -> Result<(), ThreadPoolBuildError> {
    if num_threads == 0 {
        ThreadPoolBuilder::new().build_global()
    } else {
        ThreadPoolBuilder::new()
            .num_threads(num_threads)
            .build_global()
    }
}
