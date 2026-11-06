use super::*;
use crate::{grpc::GrpcPool, profile::OwnedClientTlsProfile};
use std::{
    num::NonZeroUsize,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpListener,
    time::timeout,
};
use zero_core::{
    component::{ComponentId, ComponentRoleIds, NetworkGeneration, ResourceGeneration},
    request::Destination,
};
use zero_platform_tokio::{
    EgressInterfaceControl,
    contract::{
        BoxFuture,
        component::{BuildContext, ComponentFactory, ComponentInstance, ResourceAccess},
        lifecycle::{BuildError, CloseReport},
        network::{UpstreamBinding, UpstreamTransport},
        packet::BoxPacketSession,
        scope::TaskScope,
    },
    controlled_carrier::NativeCarrierOpener,
};
#[path = "fixtures.rs"]
mod fixtures;
use fixtures::*;
mod admission;
mod logical;
mod recipe;
#[cfg(feature = "tls")]
mod tls;
mod unknown;
mod wire;

mod hosted;

mod loan;
