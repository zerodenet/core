use super::*;
use zero_platform_tokio::contract::component::{PacketDriverMetadataView, PacketMetadataVec};
use zero_traits::packet_metadata::{PacketMemoryError, PacketMetadataCredit, PacketMetadataLayout};
#[derive(Clone, Copy)]
pub(super) struct Number {
    pub(super) cid: [u8; 255],
    pub(super) length: u8,
    pub(super) largest: u64,
}
pub(super) struct Source {
    pub(super) memory: PacketDriverMetadataView,
    pub(super) header: storage::Rows<u8>,
    pub(super) numbers: PacketMetadataVec<Number>,
    pub(super) inflight: Option<InFlight>,
}
pub(super) struct InFlight {
    pub(super) input: [u8; 32],
    pub(super) length: usize,
    pub(super) plaintexts: PacketMetadataVec<Plain>,
    pub(super) next: usize,
    pub(super) offset: usize,
    pub(super) decrypt_complete: bool,
}
#[derive(Clone, Copy)]
enum Status {
    NotInitial,
    Pending,
    Hello,
    Rejected,
}
pub(super) struct SourceOwner {
    pub(super) value: Box<Source>,
    _credit: PacketMetadataCredit,
}
pub(super) struct Plain {
    pub(super) bytes: Box<[u8]>,
    pub(super) length: usize,
    _credit: PacketMetadataCredit,
}
impl Plain {
    pub(super) fn copy(
        memory: &PacketDriverMetadataView,
        bytes: &[u8],
    ) -> Result<Self, PacketMemoryError> {
        let credit = memory.reserve(PacketMetadataLayout::array::<u8>(bytes.len())?)?;
        Ok(Self {
            bytes: bytes.into(),
            length: bytes.len(),
            _credit: credit,
        })
    }
}
/// Borrowed classification bytes. They cannot retain/clone a plaintext buffer
/// outside this actual Source's already paid CRYPTO reassembly workspace.
#[derive(Debug, PartialEq, Eq)]
pub enum QuicInitialObservation<'a> {
    NotInitial,
    Pending,
    ClientHello(std::borrow::Cow<'a, [u8]>),
    Rejected,
}
impl QuicInitialSniffer {
    /// Explicit default library construction preserves the existing owned
    /// outcome; Source construction never falls back after metadata refusal.
    pub fn observe(
        &mut self,
        datagram: &[u8],
    ) -> Result<QuicInitialObservation<'_>, PacketMemoryError> {
        if self.source.is_some() {
            return self.observe_prepaid(datagram);
        }
        Ok(match self.ingest(datagram) {
            QuicInitialOutcome::NotInitial => QuicInitialObservation::NotInitial,
            QuicInitialOutcome::Pending => QuicInitialObservation::Pending,
            QuicInitialOutcome::Rejected => QuicInitialObservation::Rejected,
            QuicInitialOutcome::ClientHello(hello) => {
                QuicInitialObservation::ClientHello(std::borrow::Cow::Owned(hello))
            }
        })
    }

    pub fn with_metadata(memory: PacketDriverMetadataView) -> Result<Self, PacketMemoryError> {
        let credit = memory.reserve(PacketMetadataLayout::boxed::<Source>())?;
        Ok(Self::from_prepaid_source(memory, credit))
    }
    pub async fn prepare_metadata(
        memory: PacketDriverMetadataView,
        deadline: tokio::time::Instant,
    ) -> Result<Self, PacketMemoryError> {
        let layout = PacketMetadataLayout::boxed::<Source>();
        let credit = tokio::select! {
            result=memory.reserve_owned(layout)=>result?,
            _=tokio::time::sleep_until(deadline)=>return Err(PacketMemoryError::Busy),
        };
        Ok(Self::from_prepaid_source(memory, credit))
    }
    fn from_prepaid_source(memory: PacketDriverMetadataView, credit: PacketMetadataCredit) -> Self {
        let source = SourceOwner {
            value: Box::new(Source {
                header: storage::Rows::source(memory.clone()),
                memory: memory.clone(),
                numbers: Default::default(),
                inflight: None,
            }),
            _credit: credit,
        };
        Self {
            crypto: CryptoReassembler::source(memory),
            largest_packet_numbers: HashMap::new(),
            source: Some(source),
        }
    }
    /// Arm the existing Root notification before admission. Transient
    /// pressure keeps the same actual input/decrypted continuation; only the
    /// existing caller's classification deadline permits deterministic fallback.
    pub async fn observe_prepaid_until<'a>(
        &'a mut self,
        datagram: &[u8],
        deadline: tokio::time::Instant,
    ) -> Result<QuicInitialObservation<'a>, PacketMemoryError> {
        let memory = self
            .source
            .as_ref()
            .ok_or(PacketMemoryError::Stopped)?
            .value
            .memory
            .clone();
        loop {
            if tokio::time::Instant::now() >= deadline {
                return Ok(QuicInitialObservation::Rejected);
            }
            let changed = memory.changed_since(memory.snapshot());
            tokio::pin!(changed);
            let ready = std::future::poll_fn(|cx| {
                use std::future::Future;
                std::task::Poll::Ready(changed.as_mut().poll(cx).is_ready())
            })
            .await;
            match self.step(datagram) {
                Err(PacketMemoryError::Busy | PacketMemoryError::Paused) => {
                    if !ready {
                        tokio::select! { _=&mut changed=>{}, _=tokio::time::sleep_until(deadline)=>return Ok(QuicInitialObservation::Rejected) }
                    }
                }
                Err(error) => return Err(error),
                Ok(status) => return self.classification(status),
            }
        }
    }
    /// Strict synchronous Source entry; no library fallback on refusal.
    pub fn observe_prepaid(
        &mut self,
        datagram: &[u8],
    ) -> Result<QuicInitialObservation<'_>, PacketMemoryError> {
        let status = self.step(datagram)?;
        self.classification(status)
    }
    fn classification(
        &self,
        status: Status,
    ) -> Result<QuicInitialObservation<'_>, PacketMemoryError> {
        Ok(match status {
            Status::NotInitial => QuicInitialObservation::NotInitial,
            Status::Pending => QuicInitialObservation::Pending,
            Status::Rejected => QuicInitialObservation::Rejected,
            Status::Hello => match self.crypto.client_hello_ref() {
                Ok(Some(hello)) => {
                    QuicInitialObservation::ClientHello(std::borrow::Cow::Borrowed(hello))
                }
                _ => QuicInitialObservation::Rejected,
            },
        })
    }
    fn step(&mut self, datagram: &[u8]) -> Result<Status, PacketMemoryError> {
        let source = &mut self
            .source
            .as_mut()
            .ok_or(PacketMemoryError::Stopped)?
            .value;
        if !looks_like_client_initial(datagram) {
            return Ok(Status::NotInitial);
        }
        let digest = ring::digest::digest(&ring::digest::SHA256, datagram);
        let input: [u8; 32] = digest.as_ref().try_into().expect("same SHA256 width");
        if source.inflight.is_none() {
            source.inflight = Some(InFlight {
                input,
                length: datagram.len(),
                plaintexts: Default::default(),
                next: 0,
                offset: 0,
                decrypt_complete: false,
            });
        }
        if source
            .inflight
            .as_ref()
            .is_some_and(|inflight| inflight.input != input || inflight.length != datagram.len())
        {
            return Err(PacketMemoryError::Stopped);
        }
        if !source.inflight.as_ref().unwrap().decrypt_complete {
            match packet::fill_source_initial(datagram, source) {
                Ok(()) => {}
                Err(packet::SourceFailure::Memory(error)) => return Err(error),
                Err(packet::SourceFailure::Rejected) => return Ok(Status::Rejected),
            }
        }
        let inflight = source
            .inflight
            .as_mut()
            .expect("same actual decrypted input");
        if inflight.input != input || inflight.length != datagram.len() {
            return Err(PacketMemoryError::Stopped);
        }
        while let Some(plain) = inflight.plaintexts.get(inflight.next) {
            match frame::collect_crypto_frames_prepaid(
                &plain.bytes[..plain.length],
                &mut self.crypto,
            ) {
                Ok(()) => inflight.next += 1,
                Err(frame::FrameFailure::Memory(error)) => return Err(error),
                Err(frame::FrameFailure::Rejected) => return Ok(Status::Rejected),
            }
        }
        drop(source.inflight.take());
        Ok(match self.crypto.client_hello_ref() {
            Ok(Some(_)) => Status::Hello,
            Ok(None) => Status::Pending,
            Err(()) => Status::Rejected,
        })
    }
}

#[cfg(test)]
mod tests;
