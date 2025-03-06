#![cfg_attr(windows, feature(windows_by_handle))]

mod sync;
pub use crate::sync::SyncOptions;
pub use crate::sync::sync;

mod common;
pub use common::*;