//! One declared server socket; failed attempts never return its native budget.
use super::*;
use std::{io, num::NonZeroUsize};
use zero_platform_tokio::{
    contract::{
        lifecycle::{CloseReport, Unfinished},
        DeliveryState, RetryClass, Stage,
    },
    controlled_datagram::{
        listener::ControlledDatagramListenerControl, AcquisitionOutcome, ControlledDatagramOwner,
        DatagramOwnerReport,
    },
};

pub(in crate::component) struct DatagramGrant {
    owner: ControlledDatagramOwner,
    control: Option<ControlledDatagramListenerControl>,
    remaining: usize,
    max_pending_sends: NonZeroUsize,
    acquired: bool,
    confirmed: u64,
    reported: bool,
    acquisition_source: Option<Arc<io::Error>>,
}
impl DatagramGrant {
    pub(in crate::component) fn new(
        component: ComponentId,
        attempts: NonZeroUsize,
        sends: NonZeroUsize,
    ) -> Result<Self, ChannelError> {
        if sends.get() > tokio::sync::Semaphore::MAX_PERMITS {
            return Err(error(
                component,
                Reason::InvalidConfig,
                "UDP pending sends exceed semaphore capacity",
            ));
        }
        Ok(Self {
            // A DatagramListener declaration authorizes exactly one successful
            // listener, including its retiring/Unknown original, not a pool.
            owner: ControlledDatagramOwner::new(NonZeroUsize::new(1).unwrap()),
            control: None,
            remaining: attempts.get(),
            max_pending_sends: sends,
            acquired: false,
            confirmed: 0,
            reported: false,
            acquisition_source: None,
        })
    }
    fn snapshot(&mut self) -> DatagramOwnerReport {
        let report = self.owner.take_report();
        self.confirmed += report.confirmed_closed;
        report
    }
    pub(in crate::component) fn bind(
        &mut self,
        component: ComponentId,
        address: SocketAddr,
    ) -> Result<GrantedDatagramListener, ChannelError> {
        let report = self.snapshot();
        if let Some(unknown) = report.unconfirmed.first() {
            return Err(io_failure(
                component,
                Stage::Prepare,
                unknown.source.clone(),
                true,
            ));
        }
        if self.acquired || self.remaining == 0 {
            return Err(error(
                component,
                Reason::Capacity,
                "declared UDP listener attempt budget exhausted",
            ));
        }
        tokio::runtime::Handle::try_current().map_err(|source| {
            error(
                component,
                Reason::InvalidConfig,
                "UDP listener requires a Tokio runtime",
            )
            .with_source(source)
        })?;
        self.remaining -= 1;
        match self.owner.bind_listener(address, self.max_pending_sends) {
            Ok((rx, tx, control)) => {
                // Owner was installed before create. Install the sole control
                // before either view escapes, with no callback or await.
                self.control = Some(control);
                self.acquired = true;
                self.snapshot();
                Ok(GrantedDatagramListener { rx, tx })
            }
            Err(failure) => {
                self.acquisition_source = Some(failure.source.clone());
                let unknown = matches!(failure.outcome, AcquisitionOutcome::Unknown(_));
                self.snapshot(); // Persist actual failed-attempt close count.
                Err(io_failure(
                    component,
                    Stage::Prepare,
                    failure.source,
                    unknown,
                ))
            }
        }
    }
    pub(in crate::component) fn shutdown(
        &mut self,
        component: ComponentId,
        address: SocketAddr,
        deadline: Instant,
    ) -> Result<CloseReport, ChannelError> {
        if let Some(control) = &self.control {
            control.revoke();
        }
        let mut report = self.snapshot();
        if Instant::now() < deadline {
            if let Some(control) = &mut self.control {
                // Same listener gate fences the real RX and all bounded TX
                // clones. No receiver task, FD clone or Drop receipt is used.
                let _ = control.close();
                report = self.snapshot();
            } else {
                let closed = self.owner.close_all();
                self.confirmed += closed.confirmed_closed;
                report = closed;
            }
        }
        if let Some(unknown) = report.unconfirmed.first() {
            return Err(io_failure(
                component,
                Stage::Close,
                unknown.source.clone(),
                true,
            ));
        }
        if report.retained_live != 0 {
            return Ok(CloseReport {
                unfinished: vec![Unfinished::Resource(format!(
                    "component {} UDP listener {address} remains owned",
                    component.0
                ))],
                ..Default::default()
            });
        }
        let mut result = CloseReport::default();
        if !self.reported && self.confirmed != 0 {
            result.closed_io.push(format!(
                "component {} UDP listener {address}: {} originals explicitly closed",
                component.0, self.confirmed
            ));
            self.reported = true;
        }
        Ok(result)
    }
}
fn io_failure(
    component: ComponentId,
    stage: Stage,
    source: Arc<io::Error>,
    unknown: bool,
) -> ChannelError {
    let mut failure = error(
        component,
        Reason::Io,
        "native UDP listener operation failed",
    )
    .with_source(UdpFailure(source));
    failure.stage = stage;
    failure.delivery = if unknown {
        DeliveryState::Unknown
    } else {
        DeliveryState::NotSubmitted
    };
    if !unknown {
        failure.retry = RetryClass::BeforeSubmission;
    }
    failure
}
#[derive(Debug)]
struct UdpFailure(Arc<io::Error>);
impl std::fmt::Display for UdpFailure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.0.fmt(f)
    }
}
impl std::error::Error for UdpFailure {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(self.0.as_ref())
    }
}
