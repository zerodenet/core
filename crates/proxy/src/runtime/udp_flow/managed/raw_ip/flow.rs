//! Per-session UDP socket over a shared raw-IP device.

use std::{
    net::{IpAddr, SocketAddr},
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc, Mutex,
    },
};

use tokio::sync::{broadcast, mpsc, oneshot};
use zero_core::Address;
use zero_engine::EngineError;
use zero_stack::{
    client_udp::{ClientUdpEvent, ClientUdpSocket},
    packet::{Endpoint, IcmpErrorKind},
};

use crate::{
    runtime::raw_ip::SharedRawIpDevice, runtime::udp_flow::managed::ManagedTupleUdpFlowConnection,
};

type Response = (Address, u16, Vec<u8>);

struct SendRequest {
    payload: Vec<u8>,
    done: oneshot::Sender<Result<(), zero_stack::client_udp::ClientUdpStackError>>,
}

pub(crate) struct RawIpUdpFlow {
    target: Address,
    target_port: u16,
    commands: mpsc::Sender<SendRequest>,
    responses: broadcast::Sender<Response>,
    closed: Arc<AtomicBool>,
    failure: Arc<Mutex<Option<IcmpErrorKind>>>,
    device: Arc<SharedRawIpDevice>,
}

impl RawIpUdpFlow {
    pub(crate) fn open(
        device: Arc<SharedRawIpDevice>,
        target: Address,
        target_port: u16,
        destination: SocketAddr,
        local_ip: IpAddr,
        observer: Option<Arc<dyn zero_traits::IoObserver>>,
    ) -> Result<Self, zero_stack::client_udp::ClientUdpStackError> {
        let client = device.bind_udp_observed(local_ip, observer)?;
        let (commands, requests) = mpsc::channel(64);
        let (responses, _) = broadcast::channel(64);
        let closed = Arc::new(AtomicBool::new(false));
        let failure = Arc::new(Mutex::new(None));
        tokio::spawn(run_flow(
            client,
            Endpoint {
                ip: destination.ip(),
                port: destination.port(),
            },
            requests,
            responses.clone(),
            closed.clone(),
            failure.clone(),
        ));
        Ok(Self {
            target,
            target_port,
            commands,
            responses,
            closed,
            failure,
            device,
        })
    }
}

#[async_trait::async_trait]
impl ManagedTupleUdpFlowConnection for RawIpUdpFlow {
    fn is_closed(&self) -> bool {
        self.closed.load(Ordering::Acquire) || self.device.is_closed()
    }

    async fn send(
        &self,
        target: &Address,
        port: u16,
        payload: &[u8],
    ) -> Result<usize, EngineError> {
        if target != &self.target || port != self.target_port {
            return Err(io_error("raw-IP UDP flow target changed"));
        }
        if let Some(kind) = *self.failure.lock().unwrap_or_else(|e| e.into_inner()) {
            return Err(io_error(format!("raw-IP UDP ICMP error: {kind:?}")));
        }
        if self.is_closed() {
            return Err(io_error("raw-IP UDP device closed"));
        }
        let (done, result) = oneshot::channel();
        self.commands
            .send(SendRequest {
                payload: payload.to_vec(),
                done,
            })
            .await
            .map_err(|_| io_error("raw-IP UDP flow closed"))?;
        result
            .await
            .map_err(|_| io_error("raw-IP UDP flow closed"))?
            .map_err(|error| io_error(format!("raw-IP UDP stack: {error:?}")))?;
        Ok(payload.len())
    }

    fn subscribe_responses(&self) -> broadcast::Receiver<Response> {
        self.responses.subscribe()
    }

    fn closed_message(&self) -> &'static str {
        "raw-IP UDP flow closed"
    }
}

async fn run_flow(
    mut client: ClientUdpSocket,
    destination: Endpoint,
    mut requests: mpsc::Receiver<SendRequest>,
    responses: broadcast::Sender<Response>,
    closed: Arc<AtomicBool>,
    failure: Arc<Mutex<Option<IcmpErrorKind>>>,
) {
    loop {
        tokio::select! {
            request = requests.recv() => {
                let Some(request) = request else { break; };
                let result = client.send_to(&request.payload, destination).await;
                let _ = request.done.send(result);
            }
            response = client.recv_event() => {
                let Some(response) = response else { break; };
                match response {
                    ClientUdpEvent::Datagram(response) => {
                        let address = match response.source.ip {
                            IpAddr::V4(ip) => Address::Ipv4(ip.octets()),
                            IpAddr::V6(ip) => Address::Ipv6(ip.octets()),
                        };
                        let _ = responses.send((address, response.source.port, response.payload));
                    }
                    ClientUdpEvent::IcmpError(error) => {
                        if matches!(error.kind, IcmpErrorKind::PacketTooBig { .. }) {
                            tracing::debug!(kind = ?error.kind, "raw-IP UDP flow adjusted path MTU");
                            continue;
                        }
                        *failure.lock().unwrap_or_else(|e| e.into_inner()) = Some(error.kind);
                        tracing::debug!(kind = ?error.kind, "raw-IP UDP flow received ICMP error");
                        break;
                    }
                }
            }
        }
    }
    closed.store(true, Ordering::Release);
}

fn io_error(message: impl ToString) -> EngineError {
    EngineError::Io(std::io::Error::other(message.to_string()))
}
