//! Reusable opening of a stream through an already prepared relay prefix.
use crate::{RuntimeError, TcpRelayStream};
use std::{future::Future, pin::Pin, sync::Arc};
use zero_platform_tokio::contract::{
    context::with_deadline,
    network::{CarrierContext, UpstreamBinding},
    ChannelError, DeliveryState, Impact, Reason, Stage,
};

pub type RelayConnectFuture =
    Pin<Box<dyn Future<Output = Result<TcpRelayStream, RuntimeError>> + Send>>;
pub type RelayConnectFn = Arc<dyn Fn(String, u16) -> RelayConnectFuture + Send + Sync>;
pub type ServiceRelayConnectFn =
    Arc<dyn Fn(String, u16, CarrierContext) -> RelayConnectFuture + Send + Sync>;

#[derive(Clone)]
enum Opener {
    Legacy(RelayConnectFn),
    Service {
        binding: UpstreamBinding,
        endpoint: (String, u16),
        open: ServiceRelayConnectFn,
    },
}

#[derive(Clone)]
pub struct RelayStreamConnector {
    identity: String,
    generation: u64,
    open: Opener,
    datagrams: Option<crate::OutboundDatagramSocketFactory>,
    observer: Option<Arc<dyn zero_traits::IoObserver>>,
}
impl RelayStreamConnector {
    pub fn new(identity: String, generation: u64, open: RelayConnectFn) -> Self {
        Self {
            identity,
            generation,
            open: Opener::Legacy(open),
            datagrams: None,
            observer: None,
        }
    }
    /// A prepared Service prefix with no implicit current demand. The callback
    /// must check its actual owner on every invocation; matching this immutable
    /// binding is not a resource grant or proof of physical closure.
    pub fn new_service_bound(
        identity: String,
        binding: UpstreamBinding,
        endpoint: (String, u16),
        open: ServiceRelayConnectFn,
    ) -> Self {
        Self {
            identity,
            generation: binding.network_generation.0,
            open: Opener::Service {
                binding,
                endpoint,
                open,
            },
            datagrams: None,
            observer: None,
        }
    }
    pub fn service_binding(&self) -> Option<&UpstreamBinding> {
        match &self.open {
            Opener::Service { binding, .. } => Some(binding),
            Opener::Legacy(_) => None,
        }
    }
    /// Every call supplies its own current budget. A legacy factory cannot
    /// silently accept this API and ignore caller cancellation or binding.
    pub fn connect_with_context(
        &self,
        binding: &UpstreamBinding,
        server: String,
        port: u16,
        context: CarrierContext,
    ) -> RelayConnectFuture {
        let opener = self.open.clone();
        let binding = binding.clone();
        let work = Box::pin(async move {
            let Opener::Service {
                binding: expected,
                endpoint,
                open,
            } = opener
            else {
                return Err(failure(
                    Reason::Unsupported,
                    "legacy relay has no per-demand context",
                ));
            };
            if binding != expected || endpoint != (server.clone(), port) {
                return Err(failure(
                    Reason::StalePlan,
                    "Service relay binding or endpoint changed",
                ));
            }
            if context.session.is_some() {
                return Err(failure(
                    Reason::InvalidConfig,
                    "Service relay cannot invent business request facts",
                ));
            }
            if context.cancel.is_cancelled() {
                return Err(failure(Reason::Cancelled, "Service relay demand cancelled"));
            }
            if context
                .deadline
                .is_some_and(|end| tokio::time::Instant::now() >= end)
            {
                return Err(failure(
                    Reason::Timeout,
                    "Service relay demand deadline elapsed",
                ));
            }
            let future = (open)(server, port, context.clone());
            tokio::select! { biased;
                _ = context.cancel.cancelled() => Err(RuntimeError::Io(std::io::Error::new(std::io::ErrorKind::Interrupted,
                    interrupted(Reason::Cancelled).with_source(std::io::Error::other("Service relay demand cancelled"))))),
                result = with_deadline(context.deadline, future) => result.map_err(|source| {
                    RuntimeError::Io(std::io::Error::new(std::io::ErrorKind::TimedOut,
                        interrupted(Reason::Timeout).with_source(source)))
                })?,
            }
        });
        observe(work, self.observer.clone())
    }

    pub fn with_observer(mut self, observer: Option<Arc<dyn zero_traits::IoObserver>>) -> Self {
        self.observer = observer;
        self
    }
    pub fn with_datagrams(mut self, factory: crate::OutboundDatagramSocketFactory) -> Self {
        self.datagrams = Some(factory);
        self
    }
    pub fn datagrams(&self) -> Option<crate::OutboundDatagramSocketFactory> {
        self.datagrams
            .clone()
            .map(|factory| factory.with_observer(self.observer.clone()))
    }
    pub fn identity(&self) -> &str {
        &self.identity
    }
    pub fn generation(&self) -> u64 {
        self.generation
    }
    pub fn connect(&self, server: String, port: u16) -> RelayConnectFuture {
        let open = match &self.open {
            Opener::Legacy(open) => (open)(server, port),
            Opener::Service { .. } => Box::pin(async {
                Err(failure(
                    Reason::Unsupported,
                    "Service relay requires a current connect demand",
                ))
            }),
        };
        observe(open, self.observer.clone())
    }
}
fn observe(
    open: RelayConnectFuture,
    observer: Option<Arc<dyn zero_traits::IoObserver>>,
) -> RelayConnectFuture {
    Box::pin(async move {
        let stream = open.await?;
        Ok(match observer {
            Some(observer) => {
                TcpRelayStream::new(crate::observed::ObservedStream::new(stream, observer))
            }
            None => stream,
        })
    })
}
fn interrupted(reason: Reason) -> ChannelError {
    // Once the callback has been invoked, a carrier may have submitted bytes.
    // Cancellation cannot establish an empty root or a safe transparent retry.
    ChannelError::new(Stage::Dial, reason, Impact::Session)
}

fn failure(reason: Reason, message: &'static str) -> RuntimeError {
    let mut error = ChannelError::new(Stage::Prepare, reason, Impact::Session)
        .with_source(std::io::Error::other(message));
    error.delivery = DeliveryState::NotSubmitted;
    let kind = match reason {
        Reason::Timeout => std::io::ErrorKind::TimedOut,
        Reason::Cancelled => std::io::ErrorKind::Interrupted,
        Reason::Unsupported => std::io::ErrorKind::Unsupported,
        Reason::StalePlan => std::io::ErrorKind::NotConnected,
        _ => std::io::ErrorKind::InvalidInput,
    };
    RuntimeError::Io(std::io::Error::new(kind, error))
}

#[cfg(test)]
#[path = "../tests/relay_connector/mod.rs"]
mod tests;
