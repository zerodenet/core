//! Explicit library owner using real native File and controlled TCP grants.
use super::fixtures::*;
use std::{
    num::NonZeroUsize,
    sync::{atomic::Ordering, Arc},
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpListener,
    time::{timeout, Duration},
};
use zero_core::{
    component::*,
    request::{ServiceOrigin, ServicePurpose, ServiceRequestFacts},
};
use zero_platform_tokio::contract::{
    component::{BuildContext, ComponentFactory, ComponentInstance, ResourceDeclaration},
    context::{ConnectFact, OpenContext, RequestIdentity},
    network::{UpstreamBinding, UpstreamTransport},
    scope::TaskScope,
    stream::{ConnectRequest, PeerConfirmation},
    CancellationToken, ChannelError, DeliveryState, Reason, Stage,
};
use zero_transport::profile::{OwnedClientTlsProfile, OwnedGrpcProfile, OwnedWebSocketProfile};
mod fixtures;
use fixtures::*;
mod admission;
mod wire;

mod faults;
