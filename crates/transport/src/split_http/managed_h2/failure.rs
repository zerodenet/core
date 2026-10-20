use super::evidence::{H2ConnectionFailure, IoOperation};
use std::{
    error::Error,
    future::Future,
    sync::{Arc, Mutex, OnceLock},
};
use tokio::sync::Notify;
use zero_core::component::ComponentInstanceId;
use zero_platform_tokio::contract::{
    scope::{HostedReceiptError, TaskJoinWitness, TaskJoined},
    ChannelError, Impact, Reason, Stage,
};

pub(super) struct Failure {
    instance: ComponentInstanceId,
    first: Mutex<Option<Arc<ChannelError>>>,
    changed: Notify,
    evidence: H2ConnectionFailure,
    witness: OnceLock<TaskJoinWitness>,
    received: Mutex<Option<TaskJoined>>,
}
impl Failure {
    pub(super) fn new(instance: ComponentInstanceId) -> Self {
        Self {
            instance,
            first: Mutex::new(None),
            changed: Notify::new(),
            evidence: H2ConnectionFailure::default(),
            witness: OnceLock::new(),
            received: Mutex::new(None),
        }
    }
    pub(super) fn install_witness(&self, witness: TaskJoinWitness) {
        // Called once immediately after the actual same-slot registration.
        let _ = self.witness.set(witness);
    }
    pub(super) fn received(&self) -> Option<TaskJoined> {
        self.received.lock().unwrap().clone()
    }
    pub(super) fn receive_joined(&self) -> Result<Option<TaskJoined>, HostedReceiptError> {
        let Some(witness) = self.witness.get() else {
            return Ok(None);
        };
        let Some(joined) = witness.try_joined() else {
            return Ok(None);
        };
        // Scope's aggregate error is distinct from the original connection
        // first-error Arc. Retain both exact objects, without substituting them.
        let received = self.received.lock().unwrap().get_or_insert(joined).clone();
        if received.result().is_err() {
            witness.acknowledge_hosted_failure(&received)?;
        }
        Ok(Some(received))
    }
    pub(super) async fn deliver_before_return(&self) {
        if let Some(witness) = self.witness.get() {
            if witness.is_hosted() {
                witness.wait().await;
            }
        }
        // Rejection retains the scope's credit and the original local receipt;
        // the explicit persistent receiver exposes the error for cleanup retry.
        let _ = self.receive_joined();
    }
    pub(super) fn get(&self) -> Option<Arc<ChannelError>> {
        self.first
            .lock()
            .unwrap_or_else(|source| source.into_inner())
            .clone()
    }
    pub(super) fn evidence(&self) -> &H2ConnectionFailure {
        &self.evidence
    }
    pub(super) fn observe_io(&self, operation: IoOperation, original: Arc<std::io::Error>) {
        self.evidence.record_io(operation, original);
        // Observation alone is not connection failure. Let the original Hyper
        // operation apply its own error handling, then publish only its actual
        // fatal handshake/outer error or ConnMapErr's connection-level error.
    }
    pub(super) fn observe_h2(&self, original: h2::Error) {
        let reason = if original.is_io() {
            Reason::Io
        } else {
            Reason::Protocol
        };
        self.evidence.record_h2(Arc::new(original));
        // Only ConnMapErr's connection-level error calls this observer. Normal
        // RST_STREAM is handled inside h2 and never publishes this failure.
        self.set(self.local(Stage::Receive, reason, self.evidence.clone()));
    }
    pub(super) fn connection_error(
        &self,
        stage: Stage,
        original: hyper::Error,
    ) -> Arc<ChannelError> {
        let reason = hyper_reason(&original);
        self.evidence.record_hyper(Arc::new(original));
        self.set(self.local(stage, reason, self.evidence.clone()))
    }
    pub(super) fn request_error(&self, stage: Stage, original: hyper::Error) -> Arc<ChannelError> {
        let reset = original
            .source()
            .and_then(|source| source.downcast_ref::<h2::Error>())
            .is_some_and(h2::Error::is_reset);
        let reason = hyper_reason(&original);
        let original = Arc::new(original);
        if let Some(first) = self.get() {
            if !reset {
                self.evidence.record_hyper(original);
            }
            return first;
        }
        let evidence = H2ConnectionFailure::default();
        evidence.record_hyper(original);
        let mut error = ChannelError::new(stage, reason, Impact::Session);
        error.component = Some(self.instance.component);
        Arc::new(error.with_source(evidence))
    }
    pub(super) fn local(
        &self,
        stage: Stage,
        reason: Reason,
        source: impl Error + Send + Sync + 'static,
    ) -> Arc<ChannelError> {
        let mut error = ChannelError::new(stage, reason, Impact::Component);
        error.component = Some(self.instance.component);
        Arc::new(error.with_source(source))
    }
    pub(super) fn set(&self, error: Arc<ChannelError>) -> Arc<ChannelError> {
        let mut first = self
            .first
            .lock()
            .unwrap_or_else(|source| source.into_inner());
        let error = first.get_or_insert(error).clone();
        drop(first);
        self.changed.notify_waiters();
        error
    }
    pub(super) async fn wait(&self) -> Arc<ChannelError> {
        loop {
            let changed = self.changed.notified();
            tokio::pin!(changed);
            changed.as_mut().enable();
            if let Some(error) = self.get() {
                return error;
            }
            changed.await;
        }
    }
    pub(super) async fn run<T, E>(
        &self,
        stage: Stage,
        operation: impl Future<Output = Result<T, E>>,
    ) -> Result<T, Arc<ChannelError>>
    where
        E: Error + Send + Sync + 'static,
    {
        let result = tokio::select! {biased;
            source = self.wait() => { self.deliver_before_return().await; return Err(source); },
            result = operation => result,
        };
        if let Some(error) = self.get() {
            self.deliver_before_return().await;
            return Err(error);
        }
        result.map_err(|source| self.local(stage, Reason::Io, source))
    }
    pub(super) async fn run_hyper<T>(
        &self,
        stage: Stage,
        operation: impl Future<Output = hyper::Result<T>>,
    ) -> Result<T, Arc<ChannelError>> {
        let result = tokio::select! {biased;
            source=self.wait()=>{ self.deliver_before_return().await; return Err(source); },
            result=operation=>result,
        };
        match result {
            Err(source) => {
                let source = self.request_error(stage, source);
                // A stream-local reset is not an aggregate failure.
                if self.get().is_some() {
                    self.deliver_before_return().await;
                }
                Err(source)
            }
            Ok(value) => match self.get() {
                Some(source) => {
                    self.deliver_before_return().await;
                    Err(source)
                }
                None => Ok(value),
            },
        }
    }
}

fn hyper_reason(source: &hyper::Error) -> Reason {
    if source.is_timeout() {
        return Reason::Timeout;
    }
    if let Some(cause) = source.source() {
        if let Some(h2) = cause.downcast_ref::<h2::Error>() {
            return if h2.is_io() {
                Reason::Io
            } else {
                Reason::Protocol
            };
        }
        if cause.is::<std::io::Error>() {
            return Reason::Io;
        }
    }
    Reason::Protocol
}

/// Preserve every decision field and explicitly expose the same original Arc.
#[derive(Debug)]
struct Shared(Arc<ChannelError>);
impl std::fmt::Display for Shared {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        std::fmt::Display::fmt(self.0.as_ref(), f)
    }
}
impl Error for Shared {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        Some(self.0.as_ref())
    }
}
pub(super) fn forwarded(source: Arc<ChannelError>) -> ChannelError {
    let mut error = ChannelError::new(source.stage, source.reason, source.impact);
    error.component = source.component;
    error.session = source.session;
    error.delivery = source.delivery;
    error.retry = source.retry;
    error.with_source(Shared(source))
}
