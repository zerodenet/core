//! Endpoint regression modules share one fixture and one integration executable.
#![cfg(feature = "wireguard")]

#[cfg(feature = "socks5")]
#[path = "support/host.rs"]
mod host;
#[path = "endpoint_contracts/management_support.rs"]
mod management_support;
mod support;

#[path = "endpoint_contracts/catalog.rs"]
mod catalog;
#[path = "endpoint_contracts/directions.rs"]
mod directions;
#[path = "endpoint_contracts/management.rs"]
mod management;
#[path = "endpoint_contracts/payload.rs"]
mod payload;
#[path = "endpoint_contracts/persistence.rs"]
mod persistence;
#[path = "endpoint_contracts/preconditions.rs"]
mod preconditions;
#[path = "endpoint_contracts/recovery.rs"]
mod recovery;

#[path = "endpoint_contracts/listening.rs"]
mod listening;

#[path = "endpoint_contracts/lifecycle.rs"]
mod lifecycle;
