//! One actual original-opening future: its Box deallocates before its ticket.
use super::*;
use std::task::{Context, Poll};
use zero_platform_tokio::contract::component::PacketDriverMetadataView;
pub struct OwnedXhttpFactoryCall {
    // This exact field order also holds through cancellation and unwinding.
    inner: Option<Pin<Box<dyn Future<Output = Result<XhttpCarrier, crate::RuntimeError>> + Send>>>,
    refused: Option<io::ErrorKind>,
    _memory: Option<PacketMetadataCredit>,
}
impl OwnedXhttpFactoryCall {
    pub fn metadata_layout_for<F>(_: &F) -> PacketMetadataLayout {
        PacketMetadataLayout::boxed::<F>()
    }
    pub fn from_prepaid<F>(
        work: F,
        credit: PacketMetadataCredit,
    ) -> Result<Self, (PacketMemoryError, F, PacketMetadataCredit)>
    where
        F: Future<Output = Result<XhttpCarrier, crate::RuntimeError>> + Send + 'static,
    {
        let expected = Self::metadata_layout_for(&work);
        if credit.layout().allocations() < expected.allocations()
            || credit.layout().bytes() < expected.bytes()
        {
            return Err((PacketMemoryError::Capacity, work, credit));
        }
        Ok(Self {
            inner: Some(Box::pin(work)),
            refused: None,
            _memory: Some(credit),
        })
    }
    pub fn prepare<F>(memory: &PacketDriverMetadataView, work: F) -> Self
    where
        F: Future<Output = Result<XhttpCarrier, crate::RuntimeError>> + Send + 'static,
    {
        match memory.reserve(Self::metadata_layout_for(&work)) {
            Ok(credit) => Self::from_prepaid(work, credit)
                .unwrap_or_else(|_| panic!("exact concrete future promised")),
            Err(source) => {
                drop(work);
                Self {
                    inner: None,
                    refused: Some(match source {
                        PacketMemoryError::Capacity => io::ErrorKind::OutOfMemory,
                        PacketMemoryError::Busy | PacketMemoryError::Paused => {
                            io::ErrorKind::WouldBlock
                        }
                        PacketMemoryError::Overflow => io::ErrorKind::InvalidInput,
                        PacketMemoryError::Stopped => io::ErrorKind::NotConnected,
                    }),
                    _memory: None,
                }
            }
        }
    }
    pub(super) fn shared(
        work: Pin<Box<dyn Future<Output = Result<XhttpCarrier, crate::RuntimeError>> + Send>>,
    ) -> Self {
        Self {
            inner: Some(work),
            refused: None,
            _memory: None,
        }
    }
}
impl Future for OwnedXhttpFactoryCall {
    type Output = Result<XhttpCarrier, crate::RuntimeError>;
    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        if let Some(source) = self.refused.take() {
            return Poll::Ready(Err(io::Error::from(source).into()));
        }
        self.inner
            .as_mut()
            .expect("same factory operation is not repolled after refusal")
            .as_mut()
            .poll(cx)
    }
}
