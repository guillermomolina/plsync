#![cfg_attr(windows, feature(windows_by_handle))]

mod sync;
pub use sync::*;

mod common;
pub use common::*;