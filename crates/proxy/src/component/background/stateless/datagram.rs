//! One association's native transfer, sharing the existing Stream owner/N.
use super::*;
use std::pin::Pin;
use zero_core::{component::SessionId, request::DatagramTargets};
use zero_platform_tokio::{
    contract::{
        datagram::{
            BoxDatagramSession, DatagramOutbound, DatagramRequest, DatagramRx, DatagramSession,
            DatagramTx, DynDatagramOutbound,
        },
        lifecycle::{ChannelControl, Unfinished},
    },
    controlled_carrier::loan::DatagramLoan,
};
pub(super) struct Datagram {
    pub(super) owner: Outbound,
    pub(super) inner: Arc<dyn DynDatagramOutbound>,
}
pub(super) struct Association {
    pub(super) session: Option<SessionId>,
    pub(super) call: CarrierContext,
}
impl Datagram {
    async fn open_original(
        &self,
        request: DatagramRequest,
        mut context: OpenContext,
    ) -> Result<(BoxDatagramSession, Arc<Transfer>), ChannelError> {
        let session = context.session_identity()?;
        if context.upstream.binding().dial != self.owner.dial {
            return Err(crate::component::instance::invalid(
                Reason::StalePlan,
                "Datagram original dial changed",
            ));
        }
        let waiting = context.upstream.observed_pending_carrier().ok_or_else(|| {
            crate::component::instance::invalid(
                Reason::Unsupported,
                "Datagram association has no actual pending-original caller",
            )
        })?;
        self.owner.reap(&context).await?;
        let transfer = self.owner.reserve_transfer(None)?;
        let _opening = Opening(transfer.clone());
        waiting.retain(transfer.adoption.clone())?;
        let call = CarrierContext {
            session: None,
            deadline: context.deadline,
            cancel: context.cancel.clone(),
        };
        // This existing metadata API validates real Business admission before
        // reselecting a separate Service demand. It creates no native grant.
        let upstream = context
            .upstream
            .select_background_carrier_observed(
                Some(context.upstream.binding().clone()),
                call.clone(),
                waiting,
            )
            .await?;
        context.upstream = UpstreamTransport::new(
            upstream.binding().clone(),
            Arc::new(Opener {
                upstream,
                endpoint: self.owner.endpoint.clone(),
                process: self.owner.process,
                transfer: transfer.clone(),
                association: Some(Association { session, call }),
            }),
        );
        let channel = self.inner.open(request, context).await?;
        if transfer.grant.lock().unwrap().is_some() {
            return Err(crate::component::instance::invalid(
                Reason::InvalidConfig,
                "Datagram protocol did not consume its one actual stream grant",
            ));
        }
        transfer
            .published
            .store(true, std::sync::atomic::Ordering::Release);
        Ok((channel, transfer.clone()))
    }
}
impl DatagramOutbound for Datagram {
    type Tx = Pin<Box<dyn DatagramTx>>;
    type Rx = Pin<Box<dyn DatagramRx>>;
    type Control = Control;
    async fn open(
        &self,
        request: DatagramRequest,
        context: OpenContext,
    ) -> Result<DatagramSession<Self::Tx, Self::Rx, Self::Control>, ChannelError> {
        let (channel, transfer) = self.open_original(request, context).await?;
        Ok(DatagramSession {
            tx: channel.tx,
            rx: channel.rx,
            limits: channel.limits,
            control: Control {
                inner: channel.control,
                transfer,
                cleanup: Default::default(),
            },
        })
    }
    async fn open_carrier(
        &self,
        request: DatagramRequest,
        context: OpenContext,
    ) -> Result<DatagramLoan, ChannelError> {
        context.session_identity()?;
        if context.session_identity()?.is_some() {
            return Err(crate::component::instance::invalid(
                Reason::InvalidConfig,
                "Datagram carrier needs its actual Service caller",
            ));
        }
        let DatagramTargets::Fixed(target) = &request.targets else {
            return Err(crate::component::instance::invalid(
                Reason::Unsupported,
                "this packet stream is fixed-target",
            ));
        };
        let target = target.clone();
        let association = request.association;
        let (channel, transfer) = self.open_original(request, context).await?;
        let original = transfer
            .owner
            .lock()
            .await
            .as_ref()
            .map(|owner| owner.original().clone())
            .ok_or_else(|| {
                crate::component::instance::invalid(
                    Reason::InvalidConfig,
                    "actual Datagram donor control was not installed",
                )
            })?;
        let channel = DatagramSession {
            tx: channel.tx,
            rx: channel.rx,
            limits: channel.limits,
            control: Control {
                inner: channel.control,
                transfer,
                cleanup: Default::default(),
            },
        };
        Ok(DatagramLoan::new(
            zero_platform_tokio::contract::datagram::erase_session(channel),
            original,
            association,
            target,
        ))
    }
}
pub(super) struct Control {
    inner: Box<dyn DynChannelControl>,
    transfer: Arc<Transfer>,
    cleanup: Mutex<cleanup::Cleanup>,
}
mod cleanup;
impl ChannelControl for Control {
    fn revoked(&self) -> BoxFuture<'static, ()> {
        let inner = self.inner.revoked();
        let stop = self.transfer.stop.clone();
        Box::pin(async move {
            tokio::select! {_=inner=>{},_=stop.cancelled()=>{}}
        })
    }
    fn cancel(&self, reason: CloseReason) {
        self.inner.cancel(reason);
        self.transfer.stop.cancel();
        self.transfer.adoption.request_retirement();
    }
    fn try_close(&mut self) -> Result<CloseReport, ChannelError> {
        self.cancel(CloseReason::Shutdown);
        if !self.cleanup.lock().unwrap().completed(0) {
            let result = self.inner.try_close();
            let duplicate = { self.cleanup.lock().unwrap().accept(0, result) };
            drop(duplicate);
        }
        if !self.cleanup.lock().unwrap().completed(1) {
            let result = self.transfer.try_close();
            let duplicate = { self.cleanup.lock().unwrap().accept(1, result) };
            drop(duplicate);
        }
        self.cleanup
            .lock()
            .unwrap()
            .snapshot(self.transfer.finished())
    }
    async fn wait_closed(&mut self, end: Instant) -> Result<CloseReport, ChannelError> {
        self.cancel(CloseReason::Shutdown);
        // Both original cleanup futures remain on this same controller/caller.
        // A logical timeout/error must never skip native/borrowed retirement.
        let state = &self.cleanup;
        let logical = async {
            if state.lock().unwrap().completed(0) {
                return;
            }
            let result = self.inner.wait_closed(end).await;
            let duplicate = { state.lock().unwrap().accept(0, result) };
            drop(duplicate);
        };
        let physical = async {
            if state.lock().unwrap().completed(1) {
                return;
            }
            let result = self.transfer.close(end).await;
            let duplicate = { state.lock().unwrap().accept(1, result) };
            drop(duplicate);
        };
        tokio::join!(logical, physical);
        self.cleanup
            .lock()
            .unwrap()
            .snapshot(self.transfer.finished())
    }
}
impl Drop for Control {
    fn drop(&mut self) {
        self.cancel(CloseReason::Shutdown);
    }
}
