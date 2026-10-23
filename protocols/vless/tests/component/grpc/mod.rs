use super::*;
use std::{
    num::NonZeroUsize,
    sync::{Arc, Mutex},
    time::Duration,
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpListener,
    time::{timeout, Instant},
};
use zero_core::{
    component::*,
    request::{Destination, ServiceOrigin, ServicePurpose, ServiceRequestFacts},
    Address,
};
use zero_platform_tokio::{
    contract::{
        component::{BuildContext, ComponentFactory, ComponentInstance, ResourceAccess},
        context::{ConnectFact, ConnectObserver, OpenContext, RequestIdentity},
        lifecycle::{BuildError, CloseReport},
        network::{UpstreamBinding, UpstreamTransport},
        packet::BoxPacketSession,
        scope::TaskScope,
        stream::{ConnectRequest, PeerConfirmation},
        BoxFuture, CancellationToken, ChannelError, Impact, Reason, Stage,
    },
    controlled_carrier::NativeCarrierOpener,
    EgressInterfaceControl,
};
use zero_transport::profile::{
    OwnedClientTlsProfile, OwnedGrpcProfile, OwnedH2Profile, OwnedHttpUpgradeProfile,
    OwnedSplitHttpProfile, OwnedWebSocketProfile,
};
#[path = "fixtures.rs"]
mod fixtures;
use fixtures::*;
mod bindings;
mod generations;
mod wire;
