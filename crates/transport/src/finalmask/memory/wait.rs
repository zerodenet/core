//! Existing mask writer waits on its exact byte lane and metadata Root.
use super::*;
use std::{
    future::Future,
    pin::Pin,
    task::{Context, Poll},
};
type ByteWait = Pin<Box<dyn Future<Output = io::Result<CarrierBufferFrameCredit>> + Send>>;
type MetaWait =
    Pin<Box<dyn Future<Output = Result<PacketMetadataCredit, PacketMemoryError>> + Send>>;
pub(in crate::finalmask) struct Wait {
    memory: Memory,
    bytes: Mutex<Option<ByteWait>>,
    byte_need: usize,
    metadata: Mutex<Option<MetaWait>>,
    meta_need: Option<PacketMetadataLayout>,
}
impl Wait {
    pub(in crate::finalmask) fn new(memory: Memory) -> Self {
        Self {
            memory,
            bytes: Mutex::new(None),
            byte_need: 0,
            metadata: Mutex::new(None),
            meta_need: None,
        }
    }
    pub(in crate::finalmask) fn poll(&mut self, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        let Some(actual) = &self.memory.0 else {
            return Poll::Ready(Ok(()));
        };
        let bytes = actual.needed_tx.load(Ordering::Acquire);
        if bytes > 0
            && actual
                .ready_tx
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .is_none()
        {
            let pending = self
                .bytes
                .get_mut()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            if pending.is_none() || self.byte_need != bytes {
                *pending = Some(Box::pin(actual.transmit.reserve(bytes)));
                self.byte_need = bytes;
            }
            let credit = std::task::ready!(pending.as_mut().unwrap().as_mut().poll(cx))?;
            *pending = None;
            let unused = {
                let mut ready = actual
                    .ready_tx
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner);
                if ready.is_none() {
                    *ready = Some(credit);
                    None
                } else {
                    Some(credit)
                }
            };
            drop(unused);
        }
        let layout = *actual
            .needed_metadata
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if let Some(layout) = layout {
            if actual
                .ready_metadata
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .is_none()
            {
                let pending = self
                    .metadata
                    .get_mut()
                    .unwrap_or_else(std::sync::PoisonError::into_inner);
                if pending.is_none() || self.meta_need != Some(layout) {
                    *pending = Some(Box::pin(actual.metadata.reserve_owned(layout)));
                    self.meta_need = Some(layout);
                }
                let credit = std::task::ready!(pending.as_mut().unwrap().as_mut().poll(cx))
                    .map_err(metadata_error)?;
                *pending = None;
                let unused = {
                    let mut ready = actual
                        .ready_metadata
                        .lock()
                        .unwrap_or_else(std::sync::PoisonError::into_inner);
                    if ready.is_none() {
                        *ready = Some(credit);
                        None
                    } else {
                        Some(credit)
                    }
                };
                drop(unused);
            }
        }
        Poll::Ready(Ok(()))
    }
}
