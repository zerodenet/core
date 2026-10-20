//! Listener integration fixtures stay outside the production module tree.
#[cfg(feature = "socks5")]
use super::{
    bind::PendingInbound,
    inbound::{reconcile_inbounds, spawn_inbound_listener, InboundReconcileState},
};
#[cfg(feature = "socks5")]
use crate::runtime::route_runtime::InboundListenerRuntimeFactory;
#[cfg(feature = "socks5")]
use std::collections::HashMap;
#[cfg(feature = "socks5")]
use tokio::{sync::watch, task::JoinSet};
#[cfg(feature = "socks5")]
use zero_config::{InboundConfig, RuntimeConfig};
#[cfg(feature = "socks5")]
use zero_engine::EngineError;

#[cfg(feature = "socks5")]
mod inbound;
