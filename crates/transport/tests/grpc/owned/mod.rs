use super::*;
use crate::{grpc::GrpcPool, profile::OwnedClientTlsProfile};
use std::{
    num::NonZeroUsize,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
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
    contract::{
        component::{BuildContext, ComponentFactory, ComponentInstance, ResourceAccess},
        lifecycle::{BuildError, CloseReport},
        network::{UpstreamBinding, UpstreamTransport},
        packet::BoxPacketSession,
        scope::TaskScope,
        BoxFuture,
    },
    controlled_carrier::NativeCarrierOpener,
    EgressInterfaceControl,
};
#[path = "fixtures.rs"]
mod fixtures;
use fixtures::*;
mod admission;
mod recipe;
#[cfg(feature = "tls")]
mod tls;
mod unknown;
mod wire;
