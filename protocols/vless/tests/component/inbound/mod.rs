//! Candidate standard source regressions, independent of ordinary registration.
use super::*;
use std::{
    net::SocketAddr,
    num::NonZeroUsize,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc, Mutex,
    },
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpStream,
    time::{timeout, Duration, Instant},
};
use zero_core::{component::*, request::Destination, Address};
use zero_platform_tokio::{
    contract::{
        component::*,
        context::{AcceptContext, SessionIdentitySource},
        lifecycle::ChannelControl,
        scope::{TaskClass, TaskScope},
        BoxFuture, CancellationToken, ChannelError, Impact, Reason, Stage,
    },
    ControlledTcpListener, ControlledTcpListenerOwner,
};
use zero_transport::profile::*;
mod fixture;
mod grpc;
mod h2;
mod mkcp;
mod mux_udp;
mod packet_fixture;
mod publication;
mod reverse;
mod wire;
const USER: &str = "eeb6c16e-9257-47cf-a7b9-d01e05259c14";
fn request(id: &'static str, grpc: bool) -> VlessInboundListenerRequest {
    let profile = OwnedGrpcProfile {
        service_names: vec!["zero.vless/Tun".into()],
        ..Default::default()
    };
    request_profiles(id, grpc.then_some(&profile), None)
}
fn request_profiles(
    id: &'static str,
    grpc: Option<&OwnedGrpcProfile>,
    h2: Option<&OwnedH2Profile>,
) -> VlessInboundListenerRequest {
    request_options(id, grpc, h2, None)
}
fn request_options(
    id: &'static str,
    grpc: Option<&OwnedGrpcProfile>,
    h2: Option<&OwnedH2Profile>,
    reverse_tag: Option<&str>,
) -> VlessInboundListenerRequest {
    VlessInboundListenerRequest::from_options_refs(
        None,
        VlessInboundOptionsRef::<
            _,
            OwnedServerTlsProfile,
            OwnedWebSocketProfile,
            OwnedGrpcProfile,
            OwnedH2Profile,
            OwnedHttpUpgradeProfile,
            OwnedSplitHttpProfile,
            NoFallback,
        > {
            users: [VlessInboundUserRef {
                id,
                reverse_tag,
                flow: None,
                testseed: &[],
                principal_key: Some("account:source"),
                up_bps: None,
                down_bps: None,
                device_limit: None,
                quota_remaining_bytes: None,
                policy_revision: Some(7),
            }],
            decryption: None,
            reality: None,
            tls: None,
            ws: None,
            grpc,
            h2,
            http_upgrade: None,
            split_http: None,
            fallback: None,
            mux_response_backlog_frames: None,
            mux_response_backlog_bytes: None,
        },
    )
    .unwrap()
}
fn header(command: u8) -> Vec<u8> {
    let mut bytes = vec![0];
    bytes.extend(crate::parse_uuid(USER).unwrap());
    bytes.extend([0, command]);
    if command == 3 {
        return bytes;
    }
    bytes.extend(53u16.to_be_bytes());
    bytes.push(1);
    bytes.extend([127, 0, 0, 1]);
    bytes
}
fn frame(sid: u16, network: u8, bytes: &[u8]) -> Vec<u8> {
    let mut meta = sid.to_be_bytes().to_vec();
    meta.extend([1, 1, network]);
    meta.extend(53u16.to_be_bytes());
    meta.extend([1, 127, 0, 0, 1]);
    let mut frame = (meta.len() as u16).to_be_bytes().to_vec();
    frame.extend(meta);
    frame.extend((bytes.len() as u16).to_be_bytes());
    frame.extend(bytes);
    frame
}

struct NoFallback;
impl zero_traits::InboundFallbackProfile for NoFallback {
    fn server(&self) -> &str {
        ""
    }
    fn port(&self) -> u16 {
        0
    }
    fn alpn(&self) -> Option<&str> {
        None
    }
}
