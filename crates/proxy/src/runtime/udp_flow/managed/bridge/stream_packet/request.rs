use zero_core::Session;

use crate::protocol_registry::UdpRuntimeServices;
use crate::runtime::tcp_dispatch::operation::LazyTcpRelayCarrier;
use crate::runtime::udp_flow::managed::flow::ManagedRelayStreamCarrier;

pub(crate) struct ManagedStreamPacketRelay<'a> {
    pub(crate) carrier: ManagedRelayStreamCarrier<'a>,
    pub(crate) tls_server_name: Option<&'a str>,
}

pub(crate) struct ManagedStreamPacketStartBridge<'a, T> {
    pub(super) services: Option<UdpRuntimeServices>,
    pub(super) tag: &'a str,
    pub(super) session: &'a Session,
    pub(super) carrier: Option<ManagedRelayStreamCarrier<'a>>,
    pub(super) tls_server_name: Option<&'a str>,
    pub(super) server: &'a str,
    pub(super) port: u16,
    pub(super) resume: T,
    pub(super) payload: &'a [u8],
    pub(super) relay_chain: bool,
}

impl<'a, T> ManagedStreamPacketStartBridge<'a, T> {
    pub(crate) fn direct(
        services: UdpRuntimeServices,
        tag: &'a str,
        session: &'a Session,
        endpoint: (&'a str, u16),
        resume: T,
        payload: &'a [u8],
    ) -> Self {
        let (server, port) = endpoint;
        Self {
            services: Some(services),
            tag,
            session,
            carrier: None,
            tls_server_name: None,
            server,
            port,
            resume,
            payload,
            relay_chain: false,
        }
    }

    pub(crate) fn relay(
        services: Option<UdpRuntimeServices>,
        tag: &'a str,
        session: &'a Session,
        relay: ManagedStreamPacketRelay<'a>,
        endpoint: (&'a str, u16),
        resume: T,
        payload: &'a [u8],
    ) -> Self {
        let (server, port) = endpoint;
        Self {
            services,
            tag,
            session,
            carrier: Some(relay.carrier),
            tls_server_name: relay.tls_server_name,
            server,
            port,
            resume,
            payload,
            relay_chain: true,
        }
    }

    pub(crate) fn lazy_relay(
        services: Option<UdpRuntimeServices>,
        tag: &'a str,
        session: &'a Session,
        carrier: LazyTcpRelayCarrier<'a>,
        endpoint: (&'a str, u16),
        resume: T,
        payload: &'a [u8],
    ) -> Self {
        let (server, port) = endpoint;
        Self {
            services,
            tag,
            session,
            carrier: Some(carrier.into()),
            tls_server_name: None,
            server,
            port,
            resume,
            payload,
            relay_chain: true,
        }
    }
}
