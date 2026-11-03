use super::*;
use crate::runtime::{udp_dispatch::UdpDispatch, udp_ingress::UdpIngressRuntime};
use std::pin::Pin;
use zero_platform_tokio::contract::{
    datagram::{DatagramLimits, DatagramRx},
    inbound::{DynInboundDatagramTx, InboundDatagramEvent as Event, InboundDatagramTraffic},
    lifecycle::DynChannelControl,
};
type Tx = Pin<Box<DynInboundDatagramTx>>;
type Rx = Pin<Box<dyn DatagramRx<Event>>>;

#[derive(Clone, Copy, PartialEq, Eq)]
pub(super) enum Exit {
    CarrierDetached,
    AssociationEnded,
}
pub(super) async fn run(
    runtime: &UdpIngressRuntime,
    tag: &str,
    dispatch: &mut UdpDispatch,
    control: &mut dyn DynChannelControl,
    mut tx: Tx,
    mut rx: Rx,
    limits: DatagramLimits,
    negotiation: InboundDatagramTraffic,
    auth: Option<&SessionAuth>,
    cancel: &CancellationToken,
) -> Result<Exit, EngineError> {
    let stop = match runtime.claim_listener_stop() {
        Ok(stop) => stop,
        Err(EngineError::Io(error)) if error.kind() == std::io::ErrorKind::Interrupted => {
            return Ok(Exit::AssociationEnded)
        }
        Err(error) => return Err(error),
    };
    let mut pending = crate::transport::StreamTraffic {
        read_bytes: negotiation.read_bytes,
        written_bytes: negotiation.written_bytes,
    };
    let mut direct_buffer = vec![0; 65536];
    let mut upstream_buffer = vec![0; 65536];
    loop {
        let (direct, upstream, idle, chains, cancellations) = dispatch.poll_refs();
        tokio::select! {
            _ = cancel.cancelled() => return Ok(Exit::CarrierDetached),
            _ = async { match &stop { Some(stop) => stop.cancelled().await, None => std::future::pending().await } } => return Ok(Exit::AssociationEnded),
            event = zero_platform_tokio::contract::datagram::recv(rx.as_mut()) => {
                let event = match event {
                    Ok(Some(event)) => event, Ok(None) => return Ok(Exit::CarrierDetached),
                    Err(error) if error.impact == Impact::Datagram => { tracing::warn!(inbound_tag = tag, error = %error, "discarded malformed inbound Datagram"); continue; },
                    Err(error) => return Err(std::io::Error::other(error).into()),
                };
                let result = match event {
                    Event::Packet { dispatch: packet, sender, protocol_bytes } => {
                        match runtime.dispatch_inbound_packet(dispatch, &packet, auth, Some(sender)).await {
                            Ok(session) => {
                                runtime.services().record_session_inbound_traffic(session, pending);
                                pending = Default::default();
                                runtime.services().record_session_inbound_rx(session, protocol_bytes);
                                Ok(())
                            },
                            Err(error) => Err(error),
                        }
                    },
                    Event::AuthenticatedPacket { dispatch: packet, sender, protocol_bytes, authentication } => {
                        match runtime.dispatch_inbound_packet(dispatch, &packet, Some(&authentication), Some(sender)).await {
                            Ok(session) => {
                                runtime.services().record_session_inbound_traffic(session, pending);
                                pending = Default::default();
                                runtime.services().record_session_inbound_rx(session, protocol_bytes);
                                Ok(())
                            },
                            Err(error) => Err(error),
                        }
                    },
                    Event::Resolve { domain } => { runtime.resolve_local_dns(&domain).await; Ok(()) },
                    Event::PeerResponse { sender, payload } => {
                        response::direct(runtime, dispatch, &mut tx, control, &limits, crate::runtime::udp_socket::DirectUdpResponseSource { sender, session_id: None, guard: None }, &payload).await
                    },
                };
                if let Err(error) = result { tracing::warn!(inbound_tag = tag, error = %error, "failed to process inbound Datagram packet"); }
            },
            incoming = direct.recv_from_addr(&mut direct_buffer) => {
                let (read, sender) = incoming?;
                if let Err(error) = response::direct(runtime, dispatch, &mut tx, control, &limits, sender, &direct_buffer[..read]).await {
                    tracing::warn!(inbound_tag = tag, error = %error, "failed to forward direct UDP response");
                }
            },
            incoming = upstream.recv_response(&mut upstream_buffer) => {
                match incoming {
                    Ok(incoming) => response::upstream(runtime, dispatch, &mut tx, control, &limits, incoming).await?,
                    Err(error) => {
                        if let Some(closed) = dispatch.drop_upstream_association() {
                            runtime.services().record_udp_upstream_recv_failure();
                            crate::logging::log_udp_upstream_association_dropped(tag, &closed.outbound_tag, &closed.server, closed.port, &error);
                        }
                    },
                }
            },
            Some(incoming) = chains.join_next_with_id() => {
                if let Err(error) = response::chain(runtime, dispatch, &mut tx, control, &limits, incoming).await {
                    tracing::warn!(inbound_tag = tag, error = %error, "failed to forward UDP chain response");
                }
            },
            _ = crate::runtime::udp_delivery::wait_for_upstream_idle(idle) => {
                if let Some(closed) = dispatch.drop_idle_upstream_association() {
                    crate::logging::log_udp_upstream_association_idle_timeout(tag, &closed.outbound_tag, &closed.server, closed.port, runtime.services().udp_upstream_idle_timeout());
                }
            },
            Some(cancellation) = cancellations.recv() => {
                if dispatch.finish_cancelled_flow(cancellation) { return Ok(Exit::AssociationEnded); }
            },
        }
    }
}
