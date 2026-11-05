//! BuildNew invocation custody in the same candidate map as the actual peers.
use super::{invocation::ResolvedLinkedInvocation, *};

pub(in crate::adapters::wireguard::lifecycle) enum RetainedInvocation {
    Prepared(Box<LinkedInvocation>),
    Resolved(Box<ResolvedLinkedInvocation>),
    Installed {
        binding: zero_platform_tokio::contract::network::UpstreamBinding,
    },
    Failed(Arc<EngineError>),
    Transitioning,
}
impl RetainedInvocation {
    /// Borrow the original recipe in place. Cancellation never drops its packet
    /// receiver or creates a fresh candidate with different ownership.
    pub(in crate::adapters::wireguard::lifecycle) fn resolve<'a>(
        &'a mut self,
        upstream: &'a UpstreamConnectServices,
        demand: PacketCarrierDemand,
    ) -> zero_platform_tokio::contract::BoxFuture<'a, Result<(), EngineError>> {
        Box::pin(async move {
            match self {
                Self::Prepared(invocation) => {
                    let request = invocation.resolve_request(upstream, demand).await?;
                    let Self::Prepared(invocation) = std::mem::replace(self, Self::Transitioning)
                    else {
                        unreachable!()
                    };
                    *self = Self::Resolved(Box::new((*invocation).into_resolved(request)));
                    Ok(())
                }
                Self::Installed { binding } => {
                    demand.validate()?;
                    if demand.binding != *binding
                        || !matches!(
                            demand.identity,
                            zero_platform_tokio::contract::context::RequestIdentity::Service(_)
                        )
                    {
                        return Err(invalid(
                            "reused linked demand differs from its actual root binding",
                        ));
                    }
                    Ok(())
                }
                Self::Resolved(invocation) => invocation.validate_retained(upstream, &demand),
                Self::Failed(source) => Err(retained_error(source.clone())),
                Self::Transitioning => Err(invalid("linked invocation transition is unfinished")),
            }
        })
    }
    pub(in crate::adapters::wireguard::lifecycle) fn install(
        &mut self,
        pending: &PendingLinkedEndpoint,
    ) -> Result<Arc<inbound::LinkedEndpoint>, EngineError> {
        match self {
            Self::Installed { .. } => pending
                .ready()
                .ok_or_else(|| invalid("installed linked original is missing")),
            Self::Resolved(_) => {
                let Self::Resolved(invocation) = std::mem::replace(self, Self::Transitioning)
                else {
                    unreachable!()
                };
                let binding = invocation.binding();
                match invocation.install() {
                    Ok(link) => {
                        *self = Self::Installed { binding };
                        Ok(link)
                    }
                    Err(source) => {
                        let source = Arc::new(source);
                        *self = Self::Failed(source.clone());
                        Err(retained_error(source))
                    }
                }
            }
            Self::Failed(source) => Err(retained_error(source.clone())),
            _ => Err(invalid(
                "actual linked invocation has not resolved before graph start",
            )),
        }
    }
}
fn retained_error(source: Arc<EngineError>) -> EngineError {
    EngineError::Io(std::io::Error::other(RetainedError(source)))
}
#[derive(Debug)]
struct RetainedError(Arc<EngineError>);
impl std::fmt::Display for RetainedError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.0.fmt(f)
    }
}
impl std::error::Error for RetainedError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(self.0.as_ref())
    }
}
