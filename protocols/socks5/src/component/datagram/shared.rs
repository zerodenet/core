use alloc::boxed::Box;
use std::{
    sync::{Arc, Mutex},
    task::{Context, Poll, Wake, Waker},
};
use tokio::io::ReadBuf;
use zero_core::component::SessionId;
use zero_platform_tokio::contract::{
    admission::SessionAdmission, lifecycle::DynChannelControl, stream::BoxStream, ChannelError,
    CloseReason, DeliveryState, Reason, Stage,
};
pub(super) struct Shared {
    pub(super) state: Mutex<State>,
    pub(super) wake: Arc<Fanout>,
    admission: Arc<SessionAdmission>,
    session: Option<SessionId>,
}
pub(super) struct State {
    pub(super) tcp: Option<BoxStream>,
    pub(super) raw_control: Option<Box<dyn DynChannelControl>>,
    pub(super) stopped: bool,
    pub(super) tcp_eof: bool,
    pub(super) tcp_shutdown: bool,
}
#[derive(Default)]
pub(super) struct Fanout {
    readers: Mutex<[Option<Waker>; 2]>,
}
impl Fanout {
    pub(super) fn register(&self, index: usize, waker: &Waker) {
        let mut readers = self.readers.lock().expect("SOCKS5 wake state poisoned");
        if !readers[index]
            .as_ref()
            .is_some_and(|old| old.will_wake(waker))
        {
            readers[index] = Some(waker.clone());
        }
    }
}
impl Wake for Fanout {
    fn wake(self: Arc<Self>) {
        self.wake_by_ref();
    }
    fn wake_by_ref(self: &Arc<Self>) {
        let readers = self
            .readers
            .lock()
            .expect("SOCKS5 wake state poisoned")
            .clone();
        for reader in readers.into_iter().flatten() {
            reader.wake();
        }
    }
}
impl Shared {
    pub(super) fn new(
        tcp: BoxStream,
        raw_control: Box<dyn DynChannelControl>,
        admission: Arc<SessionAdmission>,
        session: Option<SessionId>,
    ) -> Arc<Self> {
        Arc::new(Self {
            state: Mutex::new(State {
                tcp: Some(tcp),
                raw_control: Some(raw_control),
                stopped: false,
                tcp_eof: false,
                tcp_shutdown: false,
            }),
            wake: Arc::new(Fanout::default()),
            admission,
            session,
        })
    }
    pub(super) fn error(
        &self,
        stage: Stage,
        reason: Reason,
        delivery: DeliveryState,
    ) -> ChannelError {
        self.admission.error(self.session, stage, reason, delivery)
    }
    pub(super) fn stop(&self, reason: CloseReason) {
        let mut state = self.state.lock().expect("SOCKS5 session poisoned");
        state.stopped = true;
        if let Some(control) = &state.raw_control {
            control.cancel(reason);
        }
        drop(state);
        self.wake.wake_by_ref();
    }
    pub(super) fn watch(
        &self,
        cx: &mut Context<'_>,
        reader: usize,
    ) -> Poll<Result<(), ChannelError>> {
        self.wake.register(reader, cx.waker());
        let waker = Waker::from(self.wake.clone());
        let mut watched = Context::from_waker(&waker);
        let mut state = self.state.lock().expect("SOCKS5 session poisoned");
        if state.stopped {
            return Poll::Ready(Err(self.error(
                Stage::Receive,
                Reason::Revoked,
                DeliveryState::NotSubmitted,
            )));
        }
        let mut byte = [0; 1];
        let mut read = ReadBuf::new(&mut byte);
        let result = state
            .tcp
            .as_mut()
            .expect("active control stream")
            .as_mut()
            .poll_read(&mut watched, &mut read);
        match result {
            Poll::Pending => Poll::Ready(Ok(())),
            Poll::Ready(Ok(())) if read.filled().is_empty() => {
                state.tcp_eof = true;
                state.stopped = true;
                if let Some(control) = &state.raw_control {
                    control.cancel(CloseReason::Completed);
                }
                drop(state);
                self.wake.wake_by_ref();
                Poll::Ready(Err(self.error(
                    Stage::Receive,
                    Reason::Revoked,
                    DeliveryState::NotSubmitted,
                )))
            }
            Poll::Ready(result) => {
                state.stopped = true;
                if let Some(control) = &state.raw_control {
                    control.cancel(CloseReason::ProtocolFailure);
                }
                drop(state);
                self.wake.wake_by_ref();
                Poll::Ready(Err(match result {
                    Err(source) => self
                        .error(Stage::Receive, Reason::Io, DeliveryState::Unknown)
                        .with_source(source),
                    Ok(()) => self
                        .error(Stage::Receive, Reason::Protocol, DeliveryState::Unknown)
                        .with_source(std::io::Error::other(
                            "unexpected SOCKS5 UDP ASSOCIATE control data",
                        )),
                }))
            }
        }
    }
}
