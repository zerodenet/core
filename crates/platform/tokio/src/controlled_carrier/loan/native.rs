//! Taskless native custody keeps the original controller in its real owner.
use super::*;
use crate::{
    contract::{lifecycle::CloseReport, BoxFuture, ChannelError, CloseReason},
    controlled_carrier::{CarrierAdoption, CarrierIo, CarrierUserView},
};
use tokio::time::Instant;

pub(super) struct NativeControl(pub(super) CarrierAdoption);
impl DynChannelControl for NativeControl {
    fn cancel(&self, _: CloseReason) {
        // LoanOwner revokes the unique view. Retirement must wait for its
        // actual return before asking the original native control to close.
    }
    fn wait_closed(
        &mut self,
        deadline: Instant,
    ) -> BoxFuture<'_, Result<CloseReport, ChannelError>> {
        Box::pin(async move { self.0.retire(deadline).await })
    }
}
impl CarrierIo {
    /// Transfer the actual native view, its unique custody lease and original
    /// close authority. No worker, caller witness or FD is invented here.
    pub fn into_stream_loan(self, view: CarrierUserView) -> Result<StreamLoan, ChannelError> {
        let adoption = CarrierAdoption {
            slot: self.slot.clone(),
        };
        if !view.matches(&adoption) {
            return Err(super::super::error(
                self.slot.instance,
                crate::contract::Stage::Prepare,
                crate::contract::Reason::StalePlan,
                crate::contract::DeliveryState::Unknown,
            ));
        }
        let original = adoption.transferred_origin()?;
        let io = Custody {
            io: self,
            _view: view,
        };
        Ok(StreamLoan::new(
            Box::pin(io),
            Box::new(NativeControl(adoption)),
            original,
        ))
    }
}
struct Custody {
    io: CarrierIo,
    _view: CarrierUserView,
}
impl tokio::io::AsyncRead for Custody {
    fn poll_read(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &mut tokio::io::ReadBuf<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        std::pin::Pin::new(&mut self.io).poll_read(cx, buf)
    }
}
impl tokio::io::AsyncWrite for Custody {
    fn poll_write(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        bytes: &[u8],
    ) -> std::task::Poll<std::io::Result<usize>> {
        std::pin::Pin::new(&mut self.io).poll_write(cx, bytes)
    }
    fn poll_flush(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        std::pin::Pin::new(&mut self.io).poll_flush(cx)
    }
    fn poll_shutdown(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        std::pin::Pin::new(&mut self.io).poll_shutdown(cx)
    }
}
