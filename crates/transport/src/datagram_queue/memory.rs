//! Actual prepaid storage for one existing packet socket and its returned hops.
use super::*;
use std::sync::atomic::{AtomicUsize, Ordering};
use zero_platform_tokio::controlled_carrier::{
    CarrierBufferBudget, CarrierBufferCredit, CarrierBufferFrameCredit, CarrierBufferWorkingLane,
    CarrierBufferWorkingShape,
};
pub(crate) const RECEIVE_BYTES: usize = 65536;
pub(crate) const MAX_DATAGRAM_BYTES: usize = 65507;
// Cap budgeted GSO batches to the 64-segment QUIC ceiling (matching Linux
// UDP_MAX_SEGMENTS) so Windows Winsock's 512-segment USO limit never starves
// the shared 256-entry CarrierBufferBudget.
pub(crate) const MAX_TRANSMIT_SEGMENTS: usize = 64;
// Backing dies before its original byte credit, including across hop return.
pub(crate) struct Scratch {
    pub bytes: Vec<u8>,
    _credit: CarrierBufferCredit,
}
pub(crate) struct Memory {
    pub receive: Receive,
    pub shared: Arc<Shared>,
}
pub(crate) struct Receive {
    pub scratch: Scratch,
    shared: Arc<Shared>,
}
pub(crate) struct Shared {
    pub book: CarrierBufferBudget,
    pub transmit: CarrierBufferWorkingLane,
    receive: CarrierBufferWorkingLane,
    pub needed: AtomicUsize,
    pub ready: Mutex<Option<CarrierBufferFrameCredit>>,
    codec: Mutex<Option<crate::finalmask::udp::Codec>>,
    masked: bool,
    individual: std::sync::atomic::AtomicBool,
    pub max_transmit_datagram_bytes: usize,
}
impl Memory {
    pub fn prepare(
        book: CarrierBufferBudget,
        masks: &[crate::finalmask::udp::Mask],
        max_transmit_datagram_bytes: usize,
    ) -> io::Result<Self> {
        // This concrete consumer is HY2's existing zero/one Salamander path.
        // Other mask workers retain their own production owners and queues.
        let masked = match masks {
            [] => false,
            [crate::finalmask::udp::Mask::Salamander { .. }] => true,
            _ => {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "not a stateless single-mask packet recipe",
                ))
            }
        };
        let transmit_peak = peak_transmit(masked, max_transmit_datagram_bytes)?;
        let receive_peak = peak_receive(masked, RECEIVE_BYTES)?;
        let mut working = book.try_working_set(CarrierBufferWorkingShape {
            receive_scratch_bytes: RECEIVE_BYTES,
            transmit_working_bytes: transmit_peak,
            receive_working_bytes: receive_peak,
        })?;
        let scratch_credit = working
            .take_receive_scratch()
            .expect("one original scratch");
        let mut bytes = Vec::new();
        bytes
            .try_reserve_exact(RECEIVE_BYTES)
            .map_err(io::Error::other)?;
        bytes.resize(RECEIVE_BYTES, 0);
        let codec = masked
            .then(|| crate::finalmask::udp::Codec::new(masks, false))
            .transpose()?;
        let shared = Arc::new(Shared {
            book,
            transmit: working.transmit(),
            receive: working.receive(),
            needed: AtomicUsize::new(1),
            ready: Mutex::new(None),
            codec: Mutex::new(codec),
            masked,
            individual: std::sync::atomic::AtomicBool::new(false),
            max_transmit_datagram_bytes,
        });
        Ok(Self {
            receive: Receive {
                scratch: Scratch {
                    bytes,
                    _credit: scratch_credit,
                },
                shared: shared.clone(),
            },
            shared,
        })
    }
}
fn peak_transmit(masked: bool, bytes: usize) -> io::Result<usize> {
    if !masked {
        return Ok(bytes);
    }
    // Codec's original input copy and the actual Salamander salt/output Vec
    // coexist. Vec growth from its initial eight-byte salt has minimum cap16.
    bytes
        .checked_add(
            bytes
                .checked_add(8)
                .ok_or(io::ErrorKind::InvalidInput)?
                .max(16),
        )
        .ok_or_else(|| io::ErrorKind::InvalidInput.into())
}
fn peak_receive(masked: bool, bytes: usize) -> io::Result<usize> {
    if !masked {
        return Ok(bytes);
    }
    // Original queued wire, Codec's input copy and decoded output coexist.
    bytes
        .checked_mul(3)
        .and_then(|bytes| bytes.checked_add(16))
        .ok_or_else(|| io::ErrorKind::InvalidInput.into())
}
impl Shared {
    pub fn cap_transmit_segments(&self, segments: usize) -> usize {
        if self.individual_datagrams() {
            1
        } else {
            segments.clamp(1, MAX_TRANSMIT_SEGMENTS)
        }
    }
    pub fn grow_native(&self, segments: usize) -> io::Result<()> {
        let segments = self.cap_transmit_segments(segments);
        let bytes = self
            .max_transmit_datagram_bytes
            .checked_mul(segments)
            .ok_or(io::ErrorKind::InvalidInput)?;
        self.transmit
            .try_grow_bound(peak_transmit(self.masked, bytes)?)
    }
    pub fn masked(&self) -> bool {
        self.masked
    }
    pub fn force_individual_datagrams(&self) {
        self.individual.store(true, Ordering::Release);
    }
    pub fn individual_datagrams(&self) -> bool {
        self.masked || self.individual.load(Ordering::Acquire)
    }
    pub fn reserve_transmit(&self, bytes: usize) -> io::Result<CarrierBufferFrameCredit> {
        let bytes = peak_transmit(self.masked, bytes)?;
        let offered = self
            .ready
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .take();
        if let Some(offered) = offered {
            if offered.bytes() >= bytes {
                return Ok(offered);
            }
            drop(offered);
        }
        self.needed.store(bytes, Ordering::Release);
        self.transmit.try_reserve(bytes)
    }
    pub fn encode(&self, packet: &Transmit<'_>) -> io::Result<Vec<u8>> {
        match self
            .codec
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .as_mut()
        {
            Some(codec) => codec.encode(packet.contents),
            None => Ok(packet.contents.to_vec()),
        }
    }
    pub fn decode(&self, bytes: &[u8]) -> io::Result<Option<Vec<u8>>> {
        match self
            .codec
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .as_mut()
        {
            Some(codec) => codec.decode(bytes).map(Some),
            None => Ok(None),
        }
    }
}
impl Receive {
    /// The old hop only receives; it must not reserve a second transmit lane.
    pub fn prepare_unmasked(book: CarrierBufferBudget) -> io::Result<Self> {
        let mut working = book.try_working_set(CarrierBufferWorkingShape {
            receive_scratch_bytes: RECEIVE_BYTES,
            // The shared two-lane book requires a nonempty lane; one byte
            // is reserved, never a second datagram transmit working set.
            transmit_working_bytes: 1,
            receive_working_bytes: RECEIVE_BYTES,
        })?;
        let credit = working
            .take_receive_scratch()
            .expect("original receive scratch");
        let mut bytes = Vec::new();
        bytes
            .try_reserve_exact(RECEIVE_BYTES)
            .map_err(io::Error::other)?;
        bytes.resize(RECEIVE_BYTES, 0);
        Ok(Self {
            scratch: Scratch {
                bytes,
                _credit: credit,
            },
            shared: Arc::new(Shared {
                book,
                transmit: working.transmit(),
                receive: working.receive(),
                needed: AtomicUsize::new(1),
                ready: Mutex::new(None),
                codec: Mutex::new(None),
                masked: false,
                individual: std::sync::atomic::AtomicBool::new(false),
                max_transmit_datagram_bytes: MAX_DATAGRAM_BYTES,
            }),
        })
    }
    pub fn copy(
        &self,
        bytes: &[u8],
        peer: SocketAddr,
        metadata: Option<WireMetadata>,
    ) -> io::Result<Packet> {
        let credit = self
            .shared
            .receive
            .try_reserve(peak_receive(self.shared.masked, bytes.len())?)?;
        let bytes = bytes.to_vec();
        Ok(Packet {
            bytes,
            peer,
            metadata,
            _credit: Some(credit.into()),
        })
    }
}
