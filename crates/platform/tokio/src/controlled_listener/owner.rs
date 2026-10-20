//! Install this owner before calling bind; no native work occurs in `new`.
use super::*;
use crate::contract::{
    component::ResourceDeclaration,
    lifecycle::{ChannelControl, CloseReport},
    ChannelError, CloseReason, DeliveryState, Impact, Reason, RetryClass, Stage,
};
use std::{net::IpAddr, num::NonZeroUsize};
use tokio::time::Instant;
use zero_core::Address;
mod acquire;
mod create;

/// One exact declaration, bounded actual creation attempts, and one successful
/// listener. The host installs this roleless owner before lending bind access.
/// Failed attempts with real close confirmation may retry within the original
/// budget. Unknown native outcomes remain charged and forbid replacement.
pub struct ControlledTcpListenerOwner {
    address: SocketAddr,
    remaining: usize,
    acquired: bool,
    pub(super) control: ControlledTcpListenerControl,
    acquisition_source: Option<Arc<io::Error>>,
    pub(super) pending_close: CloseReport,
}
impl ControlledTcpListenerOwner {
    /// Pure validation/allocation. No DNS, native socket or runtime task.
    /// Caller selects the actual existing retry budget; there is no default.
    pub fn new(
        declaration: &ResourceDeclaration,
        max_bind_attempts: NonZeroUsize,
        parent: CancellationToken,
    ) -> Result<Self, ChannelError> {
        let ResourceDeclaration::TcpListener {
            destination,
            max_bind_attempts: ceiling,
        } = declaration
        else {
            return Err(denied(
                Reason::InvalidConfig,
                "not a TCP listener declaration",
            ));
        };
        if max_bind_attempts > *ceiling {
            return Err(denied(
                Reason::InvalidConfig,
                "TCP listener attempt budget exceeds declaration",
            ));
        }
        let ip = match &destination.address {
            Address::Ipv4(ip) => IpAddr::V4((*ip).into()),
            Address::Ipv6(ip) => IpAddr::V6((*ip).into()),
            Address::Domain(_) => {
                return Err(denied(
                    Reason::Unsupported,
                    "TCP listener requires an exact literal; DNS is not implied",
                ));
            }
        };
        let address = SocketAddr::new(ip, destination.port);
        let shared = Arc::new(Shared {
            accepted: None,
            state: Mutex::new(State {
                listener: None,
                raw: None,
                confirmed: 0,
                retired: false,
                closed: false,
                error: None,
            }),
            cancel: parent.child_token(),
            label: format!("native TCP listener {address}"),
        });
        Ok(Self {
            address,
            remaining: max_bind_attempts.get(),
            acquired: false,
            control: ControlledTcpListenerControl {
                shared,
                reported: false,
            },
            acquisition_source: None,
            pending_close: CloseReport::default(),
        })
    }

    /// Synchronous acquisition: same original enters the persistent ledger
    /// immediately after creation, before flags, bind, listen or registration.
    /// Success transfers only the unique accept view; close stays in this owner.
    pub fn bind_listener(
        &mut self,
        address: SocketAddr,
    ) -> Result<ControlledTcpListener, ChannelError> {
        if address != self.address {
            return Err(denied(
                Reason::InvalidConfig,
                "TCP listener address differs from declaration",
            ));
        }
        let shared = self.control.shared.clone();
        let mut state = shared
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if state.retired || state.closed || shared.cancel.is_cancelled() {
            return Err(denied(Reason::Revoked, "TCP listener owner is revoked"));
        }
        if let Some(source) = &state.error {
            return Err(failure(
                "previous native outcome is unconfirmed",
                source.clone(),
                true,
            ));
        }
        if self.acquired || self.remaining == 0 {
            return Err(denied(
                Reason::Capacity,
                "TCP listener acquisition budget exhausted",
            ));
        }
        // A missing runtime is known before native creation, and consumes no
        // actual creation attempt. A runtime without an IO driver may still
        // panic during registration; the persistent sentinel covers that case.
        tokio::runtime::Handle::try_current().map_err(|source| {
            denied(
                Reason::InvalidConfig,
                "TCP listener requires a Tokio runtime",
            )
            .with_source(source)
        })?;
        self.remaining -= 1;
        if let Err((stage, source)) = acquire::bind(&mut state, address) {
            let source = Arc::new(source);
            self.acquisition_source = Some(source.clone());
            // Successful explicit close is stored before returning the bind
            // error. Failed close keeps its independent original source.
            if state.raw.is_some() {
                state.close_original();
            } else if state.error.is_some() {
                // Consuming registration failed. Its original source becomes
                // the permanent Unknown, replacing only the pre-call sentinel.
                state.error = Some(source.clone());
            }
            return Err(failure(stage, source, state.error.is_some()));
        }
        self.acquired = true;
        Ok(ControlledTcpListener {
            cancelled: Box::pin(shared.cancel.clone().cancelled_owned()),
            shared: shared.clone(),
        })
    }

    /// The bounded last acquisition error is distinct from a close error.
    /// Errors returned to the caller reference this same original object.
    pub fn acquisition_source(&self) -> Option<Arc<io::Error>> {
        self.acquisition_source.clone()
    }
    pub fn remaining_bind_attempts(&self) -> usize {
        self.remaining
    }
    /// Observation only; the close report remains the owner's one-shot receipt.
    pub fn confirmed_close_count(&self) -> usize {
        self.control
            .shared
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .confirmed
    }
    pub fn native_unconfirmed(&self) -> bool {
        self.control
            .shared
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .error
            .is_some()
    }
    pub fn revoke(&self) {
        self.control.shared.cancel_admission();
    }
    pub async fn wait_closed(&mut self, deadline: Instant) -> Result<CloseReport, ChannelError> {
        if self.control.shared.accepted.is_some() {
            self.wait_owned_children(deadline).await
        } else {
            self.control.wait_closed(deadline).await
        }
    }
}
impl ChannelControl for ControlledTcpListenerOwner {
    fn cancel(&self, _: CloseReason) {
        self.revoke();
    }
    async fn wait_closed(&mut self, deadline: Instant) -> Result<CloseReport, ChannelError> {
        self.wait_closed(deadline).await
    }
}
fn denied(reason: Reason, message: &'static str) -> ChannelError {
    ChannelError::new(Stage::Prepare, reason, Impact::Component)
        .with_source(io::Error::other(message))
}
fn failure(stage: &'static str, source: Arc<io::Error>, unknown: bool) -> ChannelError {
    let mut error = ChannelError::new(Stage::Prepare, Reason::Io, Impact::Component)
        .with_source(AcquisitionFailure { stage, source });
    if unknown {
        error.delivery = DeliveryState::Unknown;
    } else {
        error.delivery = DeliveryState::NotSubmitted;
        error.retry = RetryClass::BeforeSubmission;
    }
    error
}
#[derive(Debug)]
struct AcquisitionFailure {
    stage: &'static str,
    source: Arc<io::Error>,
}
impl std::fmt::Display for AcquisitionFailure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "TCP listener {}: {}", self.stage, self.source)
    }
}
impl std::error::Error for AcquisitionFailure {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(self.source.as_ref())
    }
}
