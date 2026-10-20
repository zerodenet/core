//! Shared original users/driver bookkeeping, independent of protocol ADTs.
mod logical;
pub use logical::{LogicalUserPermit, LogicalUsers, UserAdmission};
