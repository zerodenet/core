//! Logical users of the same actual cached UDP socket and target FIFO.
use super::pool::Managed;
use crate::transport::{ShadowsocksUdpPendingResponse, ShadowsocksUdpSocketFlow};
use std::{
    pin::Pin,
    sync::{Arc, Mutex, Weak},
    task::{Context, Poll, Waker},
};
use tokio::time::Instant;
use zero_core::{
    component::{AssociationId, SessionId},
    packet::Datagram,
};
use zero_platform_tokio::{
    contract::{
        admission::SessionAdmission,
        context::OpenContext,
        datagram::{DatagramLimits, DatagramRx, DatagramSession, DatagramTx},
        lifecycle::{ChannelControl, CloseReport, Unfinished},
        BoxFuture, CancellationToken, ChannelError, CloseReason, DeliveryState, Reason,
        SendFailure, SendReceipt, Stage,
    },
    controlled_carrier::CarrierOrigin,
};
pub(super) struct Logical {
    root: Weak<Managed>,
    state: Mutex<State>,
    flow: ShadowsocksUdpSocketFlow,
    admission: Arc<SessionAdmission>,
    session: Option<SessionId>,
    observer: Option<Arc<dyn zero_platform_tokio::contract::context::ConnectObserver>>,
    stop: CancellationToken,
    caller: CancellationToken,
    changed: tokio::sync::Notify,
    original: CarrierOrigin,
}
struct State {
    views: usize,
    closed: bool,
    pending: Vec<ShadowsocksUdpPendingResponse>,
    receiver: Option<Waker>,
}
impl Logical {
    pub(super) fn new(
        root: &Arc<Managed>,
        flow: ShadowsocksUdpSocketFlow,
        original: CarrierOrigin,
        admission: Arc<SessionAdmission>,
        context: &OpenContext,
    ) -> Arc<Self> {
        Arc::new(Self {
            root: Arc::downgrade(root),
            state: Mutex::new(State {
                views: 2,
                closed: false,
                pending: vec![],
                receiver: None,
            }),
            flow,
            admission,
            session: context.identity.session(),
            observer: context.observer.clone(),
            stop: CancellationToken::new(),
            caller: context.cancel.clone(),
            changed: Default::default(),
            original,
        })
    }
    fn cancellation(&self) -> BoxFuture<'static, ()> {
        let (stop, caller, admission) = (
            self.stop.clone(),
            self.caller.clone(),
            self.admission.clone(),
        );
        Box::pin(async move {
            tokio::select! {_=stop.cancelled()=>{},_=caller.cancelled()=>{},_=admission.cancelled()=>{}}
        })
    }
    fn error(&self, stage: Stage, reason: Reason, delivery: DeliveryState) -> ChannelError {
        self.admission.error(self.session, stage, reason, delivery)
    }
    fn check(
        &self,
        cancel: &mut BoxFuture<'static, ()>,
        cx: &mut Context<'_>,
        stage: Stage,
    ) -> Result<(), ChannelError> {
        if cancel.as_mut().poll(cx).is_ready() {
            self.stop.cancel();
            return Err(self.error(stage, Reason::Cancelled, DeliveryState::NotSubmitted));
        }
        Ok(())
    }
    pub(super) fn settled(&self) -> bool {
        self.state.lock().unwrap().closed
    }
    fn try_close(&self) -> CloseReport {
        self.stop.cancel();
        let returned = {
            let mut state = self.state.lock().unwrap();
            if state.views != 0 {
                return CloseReport {
                    unfinished: vec![Unfinished::Resource(
                        "same SS UDP logical Tx/Rx views remain retained".into(),
                    )],
                    ..Default::default()
                };
            }
            state.closed = true;
            std::mem::take(&mut state.pending)
        };
        drop(returned);
        if let Some(root) = self.root.upgrade() {
            root.logical_returned();
        }
        self.changed.notify_waiters();
        CloseReport::default()
    }
    fn return_view(&self) {
        {
            let mut state = self.state.lock().unwrap();
            state.views = state
                .views
                .checked_sub(1)
                .expect("one exact Tx and Rx return");
        }
        if self.stop.is_cancelled() {
            let _ = self.try_close();
        }
        self.changed.notify_waiters();
    }
    pub(super) fn session(
        self: &Arc<Self>,
        association: AssociationId,
        limits: DatagramLimits,
    ) -> DatagramSession<tx::Tx, rx::Rx, Control> {
        DatagramSession {
            tx: tx::Tx::new(self.clone(), association, limits.clone()),
            rx: rx::Rx::new(self.clone(), association),
            control: Control(self.clone()),
            limits,
        }
    }
}
pub struct Control(Arc<Logical>);
impl Control {
    pub(super) fn original(&self) -> &CarrierOrigin {
        &self.0.original
    }
}
impl ChannelControl for Control {
    fn revoked(&self) -> BoxFuture<'static, ()> {
        self.0.cancellation()
    }
    fn cancel(&self, _: CloseReason) {
        self.0.stop.cancel();
    }
    fn try_close(&mut self) -> Result<CloseReport, ChannelError> {
        Ok(self.0.try_close())
    }
    async fn wait_closed(&mut self, end: Instant) -> Result<CloseReport, ChannelError> {
        self.cancel(CloseReason::Shutdown);
        loop {
            let changed = self.0.changed.notified();
            tokio::pin!(changed);
            changed.as_mut().enable();
            let report = self.0.try_close();
            if report.is_complete() {
                return Ok(report);
            }
            if tokio::time::timeout_at(end, changed).await.is_err() {
                return Ok(report);
            }
        }
    }
}
impl Drop for Control {
    fn drop(&mut self) {
        let _ = self.0.try_close();
    }
}
mod rx;
mod tx;
pub(super) use {rx::Rx, tx::Tx};
