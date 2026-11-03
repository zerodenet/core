use super::*;
use std::{sync::Mutex, task::Context};
use zero_core::component::SessionId;
use zero_platform_tokio::contract::{stream::BoxStream, BoxFuture, CancellationToken};

pub(super) struct Shared {
    pub state: Mutex<State>,
    pub owner: Option<tokio::sync::Mutex<zero_platform_tokio::controlled_carrier::loan::LoanOwner>>,
    pub original: Option<zero_platform_tokio::controlled_carrier::CarrierOrigin>,
    pub stop: CancellationToken,
    caller: CancellationToken,
    admission: Arc<zero_platform_tokio::contract::admission::SessionAdmission>,
    session: Option<SessionId>,
    pub changed: tokio::sync::Notify,
}
pub(super) struct State {
    pub stream: Option<BoxStream>,
    pub views: usize,
}
impl Shared {
    pub fn new(
        loan: zero_platform_tokio::controlled_carrier::loan::StreamLoan,
        admission: Arc<zero_platform_tokio::contract::admission::SessionAdmission>,
        session: Option<SessionId>,
        caller: CancellationToken,
    ) -> Arc<Self> {
        let original = loan.owner.original().clone();
        Arc::new(Self {
            original: Some(original),
            owner: Some(tokio::sync::Mutex::new(loan.owner)),
            state: Mutex::new(State {
                stream: Some(Box::pin(loan.io)),
                views: 2,
            }),
            stop: CancellationToken::new(),
            caller,
            admission,
            session,
            changed: tokio::sync::Notify::new(),
        })
    }
    #[cfg(test)]
    pub fn codec_fixture(
        stream: impl zero_platform_tokio::contract::stream::StreamIo + 'static,
        admission: Arc<zero_platform_tokio::contract::admission::SessionAdmission>,
        session: Option<SessionId>,
        caller: CancellationToken,
    ) -> Arc<Self> {
        Arc::new(Self {
            state: Mutex::new(State {
                stream: Some(Box::pin(stream)),
                views: 2,
            }),
            owner: None,
            original: None,
            stop: CancellationToken::new(),
            caller,
            admission,
            session,
            changed: Default::default(),
        })
    }
    pub fn cancellation(&self) -> BoxFuture<'static, ()> {
        let (stop, caller, admission) = (
            self.stop.clone(),
            self.caller.clone(),
            self.admission.clone(),
        );
        Box::pin(async move {
            tokio::select! {_=stop.cancelled()=>{},_=caller.cancelled()=>{},_=admission.cancelled()=>{}}
        })
    }
    pub fn check(
        &self,
        cancel: &mut BoxFuture<'static, ()>,
        cx: &mut Context<'_>,
        stage: Stage,
    ) -> Result<(), ChannelError> {
        if self.stop.is_cancelled()
            || self.caller.is_cancelled()
            || cancel.as_mut().poll(cx).is_ready()
        {
            self.stop.cancel();
            Err(self.error(stage, Reason::Cancelled, DeliveryState::NotSubmitted))
        } else {
            Ok(())
        }
    }
    pub fn error(&self, stage: Stage, reason: Reason, delivery: DeliveryState) -> ChannelError {
        self.admission.error(self.session, stage, reason, delivery)
    }
    pub fn io_error(&self, stage: Stage, source: std::io::Error) -> ChannelError {
        crate::client::NativeTcpStream::driver_failure(&source, self.session).unwrap_or_else(|| {
            self.error(stage, Reason::Io, DeliveryState::Unknown)
                .with_source(source)
        })
    }
    pub fn return_view(&self) {
        self.state.lock().unwrap().views -= 1;
        self.changed.notify_waiters();
    }
}
