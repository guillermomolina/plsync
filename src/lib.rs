#![cfg_attr(windows, feature(windows_by_handle))]
#![forbid(unsafe_code, rust_2018_idioms)]

mod sync;
pub use crate::sync::SyncOptions;
pub use crate::sync::sync;

mod common;
pub use common::*;