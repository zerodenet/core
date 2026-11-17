//! Return one real logical stream without claiming its donor's native closure.
use super::PooledStream;
use std::{
    io,
    pin::Pin,
    sync::{Arc, Mutex},
    task::{Context, Poll},
};
use tokio::{
    io::{AsyncRead, AsyncWrite, ReadBuf},
    time::Instant,
};
use zero_platform_tokio::{
    contract::{
        lifecycle::{CloseReport, DynChannelControl, Unfinished},
        stream::BoxStream,
        BoxFuture, CancellationToken, ChannelError, CloseReason,
    },
    controlled_carrier::{loan::StreamLoan, CarrierOrigin},
};
struct Shared {
    io: Mutex<Option<BoxStream>>,
    stop: CancellationToken,
}
struct View(Arc<Shared>);
struct Control {
    shared: Arc<Shared>,
    _original: CarrierOrigin,
    driver: zero_platform_tokio::contract::scope::TaskJoinWitness,
    done: bool,
}
impl PooledStream {
    pub fn into_stream_loan(self) -> Result<StreamLoan, ChannelError> {
        let (io, control, original) = self.into_controlled_parts()?;
        Ok(StreamLoan::new(io, control, original))
    }
    /// Transfer the same logical controller before protocol framing suspends.
    /// The donor's real original and driver remain in its installed pool.
    pub fn into_controlled_parts(
        self,
    ) -> Result<
        (
            BoxStream,
            zero_platform_tokio::contract::lifecycle::BoxChannelControl,
            CarrierOrigin,
        ),
        ChannelError,
    > {
        if self.origin.is_retired() || self.origin.driver().is_none() {
            return Err(super::failure(
                self.origin.instance(),
                zero_platform_tokio::contract::Stage::Prepare,
                zero_platform_tokio::contract::Reason::Revoked,
            ));
        }
        let driver = self
            .origin
            .driver()
            .expect("checked physical driver")
            .clone();
        self.controlled_with_driver(driver)
    }
    /// Actual accepted Source driver; its weak original and same-Scope witness
    /// remain distinct from native close and do not authorize a new listener.
    pub fn into_stream_loan_from_accepted_driver(
        self,
        driver: zero_platform_tokio::contract::scope::TaskJoinWitness,
    ) -> Result<StreamLoan, ChannelError> {
        if self.origin.is_retired()
            || driver.is_joined()
            || !(self
                .origin
                .accepted()
                .is_some_and(|source| source.scope_identity().same_scope(&driver.scope_identity()))
                || self.origin.listener().is_some_and(|source| {
                    source.owner().scope().same_scope(&driver.scope_identity())
                }))
        {
            return Err(super::failure(
                self.origin.instance(),
                zero_platform_tokio::contract::Stage::Prepare,
                zero_platform_tokio::contract::Reason::StalePlan,
            ));
        }
        let (io, control, original) = self.controlled_with_driver(driver)?;
        Ok(StreamLoan::new(io, control, original))
    }
    fn controlled_with_driver(
        self,
        driver: zero_platform_tokio::contract::scope::TaskJoinWitness,
    ) -> Result<
        (
            BoxStream,
            zero_platform_tokio::contract::lifecycle::BoxChannelControl,
            CarrierOrigin,
        ),
        ChannelError,
    > {
        let shared = Arc::new(Shared {
            io: Mutex::new(Some(self.io)),
            stop: CancellationToken::new(),
        });
        Ok((
            Box::pin(View(shared.clone())),
            Box::new(Control {
                shared,
                _original: self.origin.clone(),
                driver,
                done: false,
            })
            .into(),
            self.origin,
        ))
    }
}
impl AsyncRead for View {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        bytes: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        if self.0.stop.is_cancelled() {
            return Poll::Ready(Err(io::ErrorKind::BrokenPipe.into()));
        }
        self.0
            .io
            .lock()
            .unwrap()
            .as_mut()
            .expect("live logical pool view")
            .as_mut()
            .poll_read(cx, bytes)
    }
}
impl AsyncWrite for View {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        bytes: &[u8],
    ) -> Poll<io::Result<usize>> {
        if self.0.stop.is_cancelled() {
            return Poll::Ready(Err(io::ErrorKind::BrokenPipe.into()));
        }
        self.0
            .io
            .lock()
            .unwrap()
            .as_mut()
            .expect("live logical pool view")
            .as_mut()
            .poll_write(cx, bytes)
    }
    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        self.0
            .io
            .lock()
            .unwrap()
            .as_mut()
            .expect("live logical pool view")
            .as_mut()
            .poll_flush(cx)
    }
    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        self.0
            .io
            .lock()
            .unwrap()
            .as_mut()
            .expect("live logical pool view")
            .as_mut()
            .poll_shutdown(cx)
    }
}
impl DynChannelControl for Control {
    fn cancel(&self, _: CloseReason) {
        self.shared.stop.cancel();
    }
    fn wait_closed(
        &mut self,
        deadline: Instant,
    ) -> BoxFuture<'_, Result<CloseReport, ChannelError>> {
        Box::pin(async move {
            if self.done {
                return Ok(CloseReport::default());
            }
            self.shared.stop.cancel();
            // LoanOwner has already consumed the actual unique view return.
            // Poll the retained stream's own shutdown; do not cancel its pool.
            let shared = self.shared.clone();
            let close = std::future::poll_fn(move |cx| {
                shared
                    .io
                    .lock()
                    .unwrap()
                    .as_mut()
                    .expect("retained logical stream")
                    .as_mut()
                    .poll_shutdown(cx)
            });
            let outcome = tokio::time::timeout_at(deadline, close).await;
            let complete = match outcome {
                Ok(Ok(())) => true,
                Ok(Err(_)) => {
                    // A dead writer must be settled by the actual donor driver,
                    // never by a fabricated successful shutdown or dropped task.
                    tokio::time::timeout_at(deadline, self.driver.wait())
                        .await
                        .is_ok()
                }
                Err(_) => false,
            };
            if !complete {
                return Ok(CloseReport {
                    unfinished: vec![Unfinished::ReliableState(
                        "MUX original logical stream shutdown".into(),
                    )],
                    ..Default::default()
                });
            }
            let io = { self.shared.io.lock().unwrap().take() };
            drop(io);
            self.done = true;
            Ok(CloseReport {
                released_resources: vec!["MUX original logical stream returned to donor".into()],
                ..Default::default()
            })
        })
    }
}
impl Drop for Control {
    fn drop(&mut self) {
        self.shared.stop.cancel();
    }
}
