use std::{
    future::Future,
    path::{Path, PathBuf},
};

use zero_platform_tokio::{RelayCarrier, TcpRelayStream, TokioSocket};
use zero_traits::{
    ClientTlsProfile, GrpcTransportProfile, StreamMuxTransportHints, WebSocketTransportProfile,
};
use zero_transport::outbound_stack::{
    connect_relay_transport_stack, connect_socket_transport_stack, StreamTransportStack,
};
use zero_transport::profile::{
    OwnedClientTlsProfile, OwnedGrpcProfile, OwnedH2Profile, OwnedHttpUpgradeProfile,
    OwnedWebSocketProfile,
};
use zero_transport::RuntimeError;

#[derive(Clone, Copy)]
pub(super) struct VmessTransportOptions<'a> {
    tls: Option<&'a OwnedClientTlsProfile>,
    ws: Option<&'a OwnedWebSocketProfile>,
    grpc: Option<&'a OwnedGrpcProfile>,
    source_dir: Option<&'a Path>,
}

#[derive(Debug, Clone)]
struct OwnedVmessTransportOptions {
    tls: Option<OwnedClientTlsProfile>,
    ws: Option<OwnedWebSocketProfile>,
    grpc: Option<OwnedGrpcProfile>,
    source_dir: Option<PathBuf>,
}

impl OwnedVmessTransportOptions {
    fn from_profile_refs<TTls, TWs, TGrpc>(
        source_dir: Option<&Path>,
        tls: Option<&TTls>,
        ws: Option<&TWs>,
        grpc: Option<&TGrpc>,
    ) -> Self
    where
        TTls: ClientTlsProfile + ?Sized,
        TWs: WebSocketTransportProfile + ?Sized,
        TGrpc: GrpcTransportProfile + ?Sized,
    {
        Self {
            tls: tls.map(OwnedClientTlsProfile::from_profile),
            ws: ws.map(OwnedWebSocketProfile::from_profile),
            grpc: grpc.map(OwnedGrpcProfile::from_profile),
            source_dir: source_dir.map(PathBuf::from),
        }
    }

    fn as_borrowed(&self) -> VmessTransportOptions<'_> {
        VmessTransportOptions {
            tls: self.tls.as_ref(),
            ws: self.ws.as_ref(),
            grpc: self.grpc.as_ref(),
            source_dir: self.source_dir.as_deref(),
        }
    }
}

#[derive(Debug, Clone)]
pub struct OwnedVmessOutboundTransportPlan {
    grpc_state: Option<std::sync::Arc<crate::component::grpc::registry::State>>,
    server: String,
    port: u16,
    transport: OwnedVmessTransportOptions,
}

impl OwnedVmessOutboundTransportPlan {
    pub(in crate::transport) fn from_profile_refs<TTls, TWs, TGrpc>(
        source_dir: Option<&Path>,
        server: &str,
        port: u16,
        tls: Option<&TTls>,
        ws: Option<&TWs>,
        grpc: Option<&TGrpc>,
    ) -> Self
    where
        TTls: ClientTlsProfile + ?Sized,
        TWs: WebSocketTransportProfile + ?Sized,
        TGrpc: GrpcTransportProfile + ?Sized,
    {
        Self {
            grpc_state: None,
            server: server.to_owned(),
            port,
            transport: OwnedVmessTransportOptions::from_profile_refs(source_dir, tls, ws, grpc),
        }
    }

    pub(super) fn set_grpc_state(
        &mut self,
        state: std::sync::Arc<crate::component::grpc::registry::State>,
    ) {
        self.grpc_state = Some(state);
    }
    fn enter_legacy_grpc(&self) -> Result<(), RuntimeError> {
        if let Some(state) = &self.grpc_state {
            state.enter_legacy()?;
        }
        Ok(())
    }
    pub(super) fn server(&self) -> &str {
        &self.server
    }

    pub(super) fn port(&self) -> u16 {
        self.port
    }

    pub(super) fn transport(&self) -> VmessTransportOptions<'_> {
        self.transport.as_borrowed()
    }

    pub fn mux_transport_hints(&self) -> StreamMuxTransportHints {
        let transport = self.transport();
        StreamMuxTransportHints::new(
            transport
                .tls
                .as_ref()
                .and_then(|config| config.server_name.clone()),
            transport.ws.as_ref().map(|config| config.path.clone()),
            transport
                .grpc
                .as_ref()
                .map(|config| config.service_names.clone()),
            None,
            None,
        )
    }

    pub(super) async fn open_direct<OpenSocket, OpenSocketFut, E>(
        &self,
        open_socket: OpenSocket,
    ) -> Result<TcpRelayStream, RuntimeError>
    where
        OpenSocket: FnOnce(&str, u16) -> OpenSocketFut,
        OpenSocketFut: Future<Output = Result<TokioSocket, E>>,
        E: Into<RuntimeError>,
    {
        self.enter_legacy_grpc()?;
        let socket = open_socket(self.server(), self.port())
            .await
            .map_err(Into::into)?;
        build_vmess_outbound_transport(VmessOutboundTransportRequest {
            socket,
            options: self.transport(),
            server: self.server(),
            port: self.port(),
        })
        .await
    }

    pub(super) async fn open_relay(
        &self,
        stream: TcpRelayStream,
    ) -> Result<TcpRelayStream, RuntimeError> {
        self.enter_legacy_grpc()?;
        build_vmess_outbound_transport_over_stream(VmessFinalHopTransportRequest {
            carrier: RelayCarrier {
                stream,
                server: self.server().to_owned(),
                port: self.port(),
            },
            options: self.transport(),
        })
        .await
    }
}

pub(super) struct VmessOutboundTransportRequest<'a> {
    pub(super) socket: TokioSocket,
    pub(super) options: VmessTransportOptions<'a>,
    pub(super) server: &'a str,
    pub(super) port: u16,
}

struct VmessFinalHopTransportRequest<'a> {
    carrier: RelayCarrier,
    options: VmessTransportOptions<'a>,
}

pub(super) async fn build_vmess_outbound_transport(
    request: VmessOutboundTransportRequest<'_>,
) -> Result<TcpRelayStream, RuntimeError> {
    let VmessOutboundTransportRequest {
        socket,
        options,
        server,
        port,
    } = request;
    let VmessTransportOptions {
        tls: tls_config,
        ws: ws_config,
        grpc: grpc_config,
        source_dir,
    } = options;
    connect_socket_transport_stack(
        socket,
        StreamTransportStack {
            tls: tls_config,
            ws: ws_config,
            grpc: grpc_config,
            h2: None::<&OwnedH2Profile>,
            http_upgrade: None::<&OwnedHttpUpgradeProfile>,
            source_dir,
        },
        server,
        port,
        "vmess: ws and grpc are mutually exclusive",
    )
    .await
}

async fn build_vmess_outbound_transport_over_stream(
    request: VmessFinalHopTransportRequest<'_>,
) -> Result<TcpRelayStream, RuntimeError> {
    let VmessFinalHopTransportRequest { carrier, options } = request;
    let RelayCarrier {
        stream,
        server,
        port,
    } = carrier;
    let server: &str = &server;
    let VmessTransportOptions {
        tls: tls_config,
        ws: ws_config,
        grpc: grpc_config,
        source_dir,
    } = options;
    connect_relay_transport_stack(
        stream,
        StreamTransportStack {
            tls: tls_config,
            ws: ws_config,
            grpc: grpc_config,
            h2: None::<&OwnedH2Profile>,
            http_upgrade: None::<&OwnedHttpUpgradeProfile>,
            source_dir,
        },
        server,
        port,
        "vmess: ws and grpc are mutually exclusive",
    )
    .await
}
