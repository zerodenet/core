//! Admission, dial-policy fencing, and 3-way cancellation/deadline helpers.

use std::{future::Future, sync::Arc};
use zero_core::{component::SessionId, request::Destination};
use zero_platform_tokio::{
    contract::{
        admission::SessionAdmission,
        context::{with_deadline, ConnectFact, ConnectObserver, OpenContext},
        network::CarrierContext,
        stream::BoxStream,
        CancellationToken, ChannelError, DeliveryState, Reason, Stage,
    },
    controlled_carrier::loan::StreamLoan,
};
use zero_traits::DialPolicy;

/// Lightweight facade around an optional `ConnectObserver` for one admitted call.
#[derive(Clone, Copy)]
pub struct FactRecorder<'a> {
    observer: Option<&'a Arc<dyn ConnectObserver>>,
    session: Option<SessionId>,
}

impl<'a> FactRecorder<'a> {
    pub fn record(&self, fact: ConnectFact) {
        if let Some(observer) = self.observer {
            observer.record(self.session, fact);
        }
    }

    pub fn protocol_traffic(&self, rx_bytes: u64, tx_bytes: u64) {
        if let Some(observer) = self.observer {
            observer.protocol_traffic(self.session, rx_bytes, tx_bytes);
        }
    }
}

/// Admitted outbound call guard that enforces component revocation, caller
/// cancellation, call deadline, dial policy, and consistent error attribution.
pub struct GuardedCall<'a> {
    admission: &'a Arc<SessionAdmission>,
    context: &'a OpenContext,
    session: Option<SessionId>,
}

impl<'a> GuardedCall<'a> {
    /// Admit a call and optionally enforce that the bound dial policy matches
    /// the component's compiled policy.
    pub fn enter(
        admission: &'a Arc<SessionAdmission>,
        context: &'a OpenContext,
        expected_dial: Option<&DialPolicy>,
    ) -> Result<Self, ChannelError> {
        let session = admission.check(context)?;
        if expected_dial.is_some_and(|dial| context.upstream.binding().dial != *dial) {
            return Err(admission.error(
                session,
                Stage::Prepare,
                Reason::StalePlan,
                DeliveryState::NotSubmitted,
            ));
        }
        Ok(Self {
            admission,
            context,
            session,
        })
    }

    /// Re-wrap an already admitted `(admission, context, session)` tuple for
    /// callers that take ownership of `OpenContext` after the handshake finishes.
    pub fn from_admitted(
        admission: &'a Arc<SessionAdmission>,
        context: &'a OpenContext,
        session: Option<SessionId>,
    ) -> Self {
        Self {
            admission,
            context,
            session,
        }
    }

    pub fn admission(&self) -> &'a Arc<SessionAdmission> {
        self.admission
    }

    pub fn context(&self) -> &'a OpenContext {
        self.context
    }

    pub fn session(&self) -> Option<SessionId> {
        self.session
    }

    pub fn facts(&self) -> FactRecorder<'a> {
        FactRecorder {
            observer: self.context.observer.as_ref(),
            session: self.session,
        }
    }

    pub fn error(&self, stage: Stage, reason: Reason, delivery: DeliveryState) -> ChannelError {
        self.admission.error(self.session, stage, reason, delivery)
    }

    pub fn attach(&self, error: ChannelError) -> ChannelError {
        self.admission.attach(error, self.session)
    }

    pub fn carrier_context(&self) -> CarrierContext {
        CarrierContext {
            session: self.session,
            deadline: self.context.deadline,
            cancel: self.context.cancel.clone(),
        }
    }

    /// Run `fut` under biased revocation, caller cancellation, and deadline racing.
    pub async fn run<T>(
        &self,
        stage: Stage,
        delivery: DeliveryState,
        fut: impl Future<Output = T>,
    ) -> Result<T, ChannelError> {
        tokio::select! {
            biased;
            _ = self.admission.cancelled() => Err(self.error(stage, Reason::Revoked, delivery)),
            _ = self.context.cancel.cancelled() => Err(self.error(stage, Reason::Cancelled, delivery)),
            result = with_deadline(self.context.deadline, fut) => result
                .map_err(|source| self.error(stage, Reason::Timeout, delivery).with_source(source)),
        }
    }

    /// Run `fut` under biased admission revocation, task-scope cancellation,
    /// caller cancellation, and call deadline racing.
    pub async fn run_scoped<T>(
        &self,
        scope_cancel: &CancellationToken,
        stage: Stage,
        delivery: DeliveryState,
        fut: impl Future<Output = T>,
    ) -> Result<T, ChannelError> {
        tokio::select! {
            biased;
            _ = self.admission.cancelled() => Err(self.error(stage, Reason::Revoked, delivery)),
            _ = scope_cancel.cancelled() => Err(self.error(stage, Reason::Revoked, delivery)),
            _ = self.context.cancel.cancelled() => Err(self.error(stage, Reason::Cancelled, delivery)),
            result = with_deadline(self.context.deadline, fut) => result
                .map_err(|source| self.error(stage, Reason::Timeout, delivery).with_source(source)),
        }
    }

    /// Run `fut` under biased revocation and caller cancellation when `fut`
    /// already enforces `context.deadline` internally.
    pub async fn run_cancellable<T>(
        &self,
        stage: Stage,
        delivery: DeliveryState,
        fut: impl Future<Output = T>,
    ) -> Result<T, ChannelError> {
        tokio::select! {
            biased;
            _ = self.admission.cancelled() => Err(self.error(stage, Reason::Revoked, delivery)),
            _ = self.context.cancel.cancelled() => Err(self.error(stage, Reason::Cancelled, delivery)),
            result = fut => Ok(result),
        }
    }

    /// Run a fallible I/O future under the same 3-way guard and normalize inner
    /// errors to `Reason::Io`.
    pub async fn run_io<T, E>(
        &self,
        stage: Stage,
        delivery: DeliveryState,
        fut: impl Future<Output = Result<T, E>>,
    ) -> Result<T, ChannelError>
    where
        E: std::error::Error + Send + Sync + 'static,
    {
        self.run(stage, delivery, fut)
            .await?
            .map_err(|source| self.error(stage, Reason::Io, delivery).with_source(source))
    }

    /// Open a standard upstream stream carrier under dial guard and error attribution.
    pub async fn open_stream(&self, endpoint: Destination) -> Result<BoxStream, ChannelError> {
        let opened = self
            .context
            .upstream
            .open_stream(endpoint, self.carrier_context());
        self.run(Stage::Dial, DeliveryState::Unknown, opened)
            .await?
            .map_err(|error| self.attach(error))
    }

    /// Borrow a standard preceding stream loan under dial guard and error attribution.
    pub async fn borrow_stream(&self, endpoint: Destination) -> Result<StreamLoan, ChannelError> {
        let opened = self
            .context
            .upstream
            .borrow_stream(endpoint, self.carrier_context());
        self.run(Stage::Dial, DeliveryState::Unknown, opened)
            .await?
            .map_err(|error| self.attach(error))
    }
}
