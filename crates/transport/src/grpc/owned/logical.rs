//! Actual logical gRPC view/workers, independent of its pooled native original.
use super::{budget::Record, *};
use std::sync::Arc;
use zero_platform_tokio::contract::{
    BoxFuture, CloseReason,
    lifecycle::{CloseReport, DynChannelControl},
};

pub(super) struct WorkerReturn(pub(super) Arc<Record>, pub(super) bool);
impl Drop for WorkerReturn {
    fn drop(&mut self) {
        self.0.worker_returned(self.1);
    }
}
struct Control(Arc<Record>);
impl DynChannelControl for Control {
    fn try_close(&mut self) -> Result<CloseReport, ChannelError> {
        self.0.stop.cancel();
        Ok(if self.0.returned() {
            CloseReport::default()
        } else {
            CloseReport {
                unfinished: vec![zero_platform_tokio::contract::lifecycle::Unfinished::Io(
                    "actual gRPC logical stream/workers remain retained".into(),
                )],
                ..Default::default()
            }
        })
    }

    fn cancel(&self, _: CloseReason) {
        self.0.stop.cancel();
    }
    fn wait_closed(&mut self, end: Instant) -> BoxFuture<'_, Result<CloseReport, ChannelError>> {
        self.0.stop.cancel();
        Box::pin(self.0.wait_returned(end))
    }
}
impl Drop for Control {
    fn drop(&mut self) {
        self.0.stop.cancel();
    }
}
impl NativeGrpcStream {
    /// Move the one actual view and its actual logical controller to a protocol.
    /// Failure/cancellation of a later protocol handshake drops the controller,
    /// requesting this same worker record to stop while the pool retains it.
    pub fn into_controlled_parts(
        self,
    ) -> Result<
        (
            zero_platform_tokio::contract::stream::BoxStream,
            Box<dyn DynChannelControl>,
            zero_platform_tokio::controlled_carrier::CarrierOrigin,
        ),
        ChannelError,
    > {
        let physical = &self.life.physical;
        let witness = physical.witness.lock().unwrap().clone().ok_or_else(|| {
            error(
                physical.adoption.instance(),
                Stage::Prepare,
                Reason::StalePlan,
            )
        })?;
        let origin = physical.adoption.origin(&witness)?;
        let control = Box::new(Control(self.record()));
        Ok((Box::pin(self), control, origin))
    }
    /// This closes only that logical stream, never another pool user's carrier.
    pub fn into_stream_loan(
        self,
    ) -> Result<zero_platform_tokio::controlled_carrier::loan::StreamLoan, ChannelError> {
        let (io, control, origin) = self.into_controlled_parts()?;
        Ok(zero_platform_tokio::controlled_carrier::loan::StreamLoan::new(io, control, origin))
    }
}
