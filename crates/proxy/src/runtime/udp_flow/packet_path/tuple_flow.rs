//! A bounded packet carrier over target-specific logical datagram connections.
use super::PacketPathCarrier;
use crate::protocol_registry::UdpAssociationCloseKind;
use crate::runtime::udp_flow::managed::ManagedTupleUdpFlowConnection;
use std::sync::atomic::{AtomicBool, Ordering};
use std::{
    collections::HashMap,
    future::Future,
    io,
    pin::Pin,
    sync::{Arc, Mutex},
    time::Duration,
};
use tokio::sync::{mpsc, OnceCell};
use zero_core::Address;
use zero_engine::EngineError;

pub(crate) type OpenFuture = Pin<
    Box<dyn Future<Output = Result<Box<dyn ManagedTupleUdpFlowConnection>, EngineError>> + Send>,
>;
pub(crate) type Open = Arc<dyn Fn(Address, u16) -> OpenFuture + Send + Sync>;
type Accounting = Arc<dyn Fn(Option<UdpAssociationCloseKind>) + Send + Sync>;
const MAX_TARGETS: usize = 64;

pub(crate) fn carrier(
    services: crate::protocol_registry::UdpNetworkServices,
    open: Open,
) -> Arc<dyn PacketPathCarrier> {
    let idle = services.upstream_idle_timeout();
    Arc::new(TupleCarrier::new(
        open,
        idle,
        Arc::new(move |closed| {
            if let Some(kind) = closed {
                services.record_association_close(kind);
            } else {
                services.record_association_created();
            }
        }),
    ))
}

struct Driver(tokio::task::JoinHandle<()>);
impl Drop for Driver {
    fn drop(&mut self) {
        self.0.abort();
    }
}
struct Flow {
    connection: Box<dyn ManagedTupleUdpFlowConnection>,
    _driver: Driver,
    accounting: Accounting,
    idle: AtomicBool,
}
impl Drop for Flow {
    fn drop(&mut self) {
        (self.accounting)(Some(if self.idle.load(Ordering::Relaxed) {
            UdpAssociationCloseKind::IdleTimeout
        } else {
            UdpAssociationCloseKind::Closed
        }));
    }
}
struct Slot {
    flow: OnceCell<Flow>,
    touched: Arc<Mutex<tokio::time::Instant>>,
}
impl Slot {
    fn touch(&self) {
        *self.touched.lock().unwrap() = tokio::time::Instant::now();
    }
    fn expired(&self, idle: Duration) -> bool {
        self.touched.lock().unwrap().elapsed() >= idle
    }
}
type Slots = Arc<Mutex<HashMap<(Address, u16), Arc<Slot>>>>;

pub(crate) struct TupleCarrier {
    open: Open,
    slots: Slots,
    sender: mpsc::Sender<Result<Vec<u8>, EngineError>>,
    receiver: tokio::sync::Mutex<mpsc::Receiver<Result<Vec<u8>, EngineError>>>,
    accounting: Accounting,
    _cleanup: Driver,
}

impl TupleCarrier {
    pub(crate) fn new(open: Open, idle: Duration, accounting: Accounting) -> Self {
        let slots: Slots = Arc::new(Mutex::new(HashMap::new()));
        let weak = Arc::downgrade(&slots);
        let idle = idle.max(Duration::from_millis(1));
        let period = idle.min(Duration::from_secs(5));
        let cleanup = tokio::spawn(async move {
            loop {
                tokio::time::sleep(period).await;
                let Some(slots) = weak.upgrade() else { break };
                slots.lock().unwrap().retain(|_, slot| {
                    // A caller holding the slot may be establishing or sending.
                    let retain = Arc::strong_count(slot) > 1 || !slot.expired(idle);
                    if !retain {
                        if let Some(flow) = slot.flow.get() {
                            flow.idle.store(true, Ordering::Relaxed);
                        }
                    }
                    retain
                });
            }
        });
        let (sender, receiver) = mpsc::channel(128);
        Self {
            open,
            slots,
            sender,
            receiver: tokio::sync::Mutex::new(receiver),
            accounting,
            _cleanup: Driver(cleanup),
        }
    }

    fn slot(&self, target: &Address, port: u16) -> Result<Arc<Slot>, EngineError> {
        let mut slots = self.slots.lock().unwrap();
        let key = (target.clone(), port);
        if let Some(slot) = slots.get(&key) {
            slot.touch();
            return Ok(slot.clone());
        }
        if slots.len() >= MAX_TARGETS {
            return Err(failure("packet carrier target capacity exhausted"));
        }
        let slot = Arc::new(Slot {
            flow: OnceCell::new(),
            touched: Arc::new(Mutex::new(tokio::time::Instant::now())),
        });
        slots.insert(key, slot.clone());
        Ok(slot)
    }

    fn remove_slot(&self, target: &Address, port: u16, slot: &Arc<Slot>, failed_open: bool) {
        let mut slots = self.slots.lock().unwrap();
        // Another waiter can retry initialization after a failed attempt.
        // Do not detach a slot while that caller still owns it.
        if failed_open && (Arc::strong_count(slot) > 2 || slot.flow.get().is_some()) {
            return;
        }
        let key = (target.clone(), port);
        if slots
            .get(&key)
            .is_some_and(|current| Arc::ptr_eq(current, slot))
        {
            slots.remove(&key);
        }
    }
}

#[async_trait::async_trait]
impl PacketPathCarrier for TupleCarrier {
    async fn send_to(
        &self,
        target: &Address,
        port: u16,
        payload: &[u8],
    ) -> Result<(), EngineError> {
        let slot = self.slot(target, port)?;
        let opened = slot
            .flow
            .get_or_try_init(|| async {
                let connection = (self.open)(target.clone(), port).await?;
                // Subscribe before sending the first packet: immediate replies must
                // not fall between the handshake and response-driver registration.
                let mut responses = connection.subscribe_responses();
                let sender = self.sender.clone();
                let touched = slot.touched.clone();
                let driver = tokio::spawn(async move {
                    loop {
                        let response = tokio::select! {
                            response = responses.recv() => response,
                            _ = sender.closed() => break,
                        };
                        let (packet, failed) = match response {
                            Ok((_, _, data)) => (Ok(data), false),
                            Err(_) => (
                                Err(failure(
                                    "packet carrier logical connection closed or overflowed",
                                )),
                                true,
                            ),
                        };
                        *touched.lock().unwrap() = tokio::time::Instant::now();
                        if sender.send(packet).await.is_err() || failed {
                            break;
                        }
                    }
                });
                (self.accounting)(None);
                Ok::<_, EngineError>(Flow {
                    connection,
                    _driver: Driver(driver),
                    accounting: self.accounting.clone(),
                    idle: AtomicBool::new(false),
                })
            })
            .await;
        let flow = match opened {
            Ok(flow) => flow,
            Err(error) => {
                self.remove_slot(target, port, &slot, true);
                return Err(error);
            }
        };
        let result = flow.connection.send(target, port, payload).await;
        slot.touch();
        if result.is_err() {
            self.remove_slot(target, port, &slot, false);
        }
        result.map(|_| ())
    }

    async fn recv_from(&self, buf: &mut [u8]) -> Result<usize, EngineError> {
        let packet = self
            .receiver
            .lock()
            .await
            .recv()
            .await
            .ok_or_else(|| failure("packet carrier closed"))??;
        if packet.len() > buf.len() {
            return Err(failure("packet carrier receive buffer too small"));
        }
        buf[..packet.len()].copy_from_slice(&packet);
        Ok(packet.len())
    }
}
fn failure(message: &'static str) -> EngineError {
    EngineError::Io(io::Error::other(message))
}

#[cfg(test)]
#[path = "../../../../tests/runtime/packet_path_tuple_flow.rs"]
mod tests;
