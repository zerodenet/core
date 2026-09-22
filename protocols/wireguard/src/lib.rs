#![no_std]

extern crate alloc;

#[cfg(feature = "validation")]
pub mod validation;

#[cfg(feature = "routing")]
pub mod routing;
