use super::*;
use std::{
    future::Future,
    num::NonZeroUsize,
    sync::atomic::{AtomicBool, Ordering},
    time::Duration,
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpListener,
    time::timeout,
};
use zero_core::{
    component::*,
    request::{Destination, ServiceOrigin, ServicePurpose, ServiceRequestFacts},
    Address,
};
use zero_platform_tokio::{
    contract::{
        component::{BuildContext, ComponentFactory, ResourceAccess},
        context::{OpenContext, RequestIdentity},
        lifecycle::BuildError,
        network::{UpstreamBinding, UpstreamTransport},
        packet::BoxPacketSession,
        scope::{SpawnError, TaskClass, TaskScope},
        stream::{ConnectRequest, PeerConfirmation},
        BoxFuture, CancellationToken,
    },
    controlled_carrier::NativeCarrierOpener,
    EgressInterfaceControl,
};
use zero_transport::profile::{OwnedClientTlsProfile, OwnedGrpcProfile, OwnedWebSocketProfile};
const ID: &str = "ad14d22b-6340-45f4-834e-f09f948c3f9c";
fn end() -> Instant {
    Instant::now() + Duration::from_secs(5)
}
fn destination() -> Destination {
    Destination::new(Address::Domain("original.example".into()), 443)
}
#[derive(Debug, Default)]
struct FailActualRead(AtomicBool);
impl zero_traits::IoObserver for FailActualRead {
    fn stream_boundary(&self) {}
    fn sent(&self, _: usize) {}
    fn received(&self, _: usize) {
        assert!(
            !self.0.load(Ordering::SeqCst),
            "original VMess carrier read failed"
        );
    }
    fn error(&self) {}
    fn dropped(&self) {}
}
struct Denied;
fn denied<T: Send + 'static>() -> BoxFuture<'static, Result<T, BuildError>> {
    Box::pin(async {
        Err(ChannelError::new(
            Stage::Prepare,
            Reason::Unsupported,
            Impact::Component,
        ))
    })
}
impl ResourceAccess for Denied {
    fn read_file(
        &self,
        _: std::path::PathBuf,
        _: usize,
    ) -> BoxFuture<'_, Result<Vec<u8>, BuildError>> {
        denied()
    }
    fn listen_tcp(
        &self,
        _: Destination,
    ) -> BoxFuture<'_, Result<zero_platform_tokio::ControlledTcpListener, BuildError>> {
        denied()
    }
    fn listen_datagram(
        &self,
        _: Destination,
    ) -> BoxFuture<
        '_,
        Result<zero_platform_tokio::contract::component::GrantedDatagramListener, BuildError>,
    > {
        denied()
    }
    fn open_device(&self, _: Arc<str>) -> BoxFuture<'_, Result<BoxPacketSession, BuildError>> {
        denied()
    }
}
#[path = "grpc_hosted/fixture.rs"]
mod fixture;

#[tokio::test]
async fn hosted_vmess_real_grpc_user_error_accepts_the_original_auxiliary_exit() {
    fixture::actual_failure(true).await;
}
#[tokio::test]
async fn hosted_vmess_real_grpc_no_user_cleanup_accepts_failure_without_global_revocation() {
    fixture::actual_failure(false).await;
}
