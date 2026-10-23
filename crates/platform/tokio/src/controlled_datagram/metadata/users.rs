//! Shared original users/driver bookkeeping, independent of protocol ADTs.
pub mod drivers;
mod logical;
pub use logical::{LogicalUserPermit, LogicalUsers, UserAdmission};

mod graph;
pub use graph::{
    BoundMetadataUsers, MetadataUserGraph, MetadataUsersClosedReceipt, MetadataUsersCompletion,
};
