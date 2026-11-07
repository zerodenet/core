//! Borrow the same immutable Engine roots and finite actual device observer.
use super::*;
use crate::adapters::devices::EndpointDeviceRows;
use std::io;
use zero_engine::{EndpointControlObservation, EngineEndpointQueryGuard, EngineEndpointRows};
use zero_runtime::configuration::ControlBudgetPermit;
mod peers;
mod wire;
#[derive(Debug)]
pub enum ProxyEndpointQueryError<'a> {
    Missing(zero_engine::EndpointQueryError<'a>),
    Unsupported,
}
impl ProxyEndpointQueryError<'_> {
    pub fn code(&self) -> zero_api::ApiErrorCode {
        match self {
            Self::Missing(error) => error.code(),
            Self::Unsupported => zero_api::ApiErrorCode::Unsupported,
        }
    }
    pub fn message(&self) -> impl std::fmt::Display + '_ {
        Message(self)
    }
}
struct Message<'a>(&'a ProxyEndpointQueryError<'a>);
impl std::fmt::Display for Message<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self.0 {
            ProxyEndpointQueryError::Missing(error) => std::fmt::Display::fmt(&error.message(), f),
            ProxyEndpointQueryError::Unsupported => {
                f.write_str("endpoint protocol has no registered detail observer")
            }
        }
    }
}
pub struct ProxyEndpointQueryGuard<'a> {
    handle: &'a ProxyHandle,
    engine: EngineEndpointQueryGuard<'a>,
    details: bool,
}
pub struct ProxyEndpointQuery<'a> {
    rows: EngineEndpointRows<'a>,
    observations: Vec<Option<Observation>>,
    config: &'a zero_config::RuntimeConfig,
    details: bool,
}
struct Observation {
    facts: EndpointDeviceRows,
    projection: EndpointControlObservation,
}
impl ProxyHandle {
    pub fn control_endpoint_query<'a>(
        &'a self,
        request: &'a zero_api::QueryRequest,
    ) -> Option<Result<ProxyEndpointQueryGuard<'a>, ProxyEndpointQueryError<'a>>> {
        let details = matches!(request, zero_api::QueryRequest::EndpointDetails(_));
        let engine = match self.proxy.engine().control_endpoint_query(request)? {
            Ok(engine) => engine,
            Err(error) => return Some(Err(ProxyEndpointQueryError::Missing(error))),
        };
        if details
            && engine
                .bindings()
                .any(|binding| !self.proxy.protocols.endpoint_observer_present(binding))
        {
            return Some(Err(ProxyEndpointQueryError::Unsupported));
        }
        Some(Ok(ProxyEndpointQueryGuard {
            handle: self,
            engine,
            details,
        }))
    }
}
impl ProxyEndpointQueryGuard<'_> {
    pub fn freeze<'a>(
        &'a self,
        workspace: &mut ControlBudgetPermit,
    ) -> io::Result<ProxyEndpointQuery<'a>> {
        let rows = self.engine.freeze(workspace)?;
        let count = rows.rows().count();
        let bytes = count
            .checked_mul(std::mem::size_of::<Option<Observation>>())
            .ok_or_else(|| io::Error::other("endpoint observation workspace overflow"))?;
        workspace.grow(bytes)?;
        let mut observations = Vec::new();
        observations
            .try_reserve_exact(count)
            .map_err(io::Error::other)?;
        let config = self.engine.snapshot().config();
        for row in rows.rows() {
            let binding = row.binding();
            let facts = self
                .handle
                .proxy
                .protocols
                .control_endpoint_rows(binding, config, workspace)?;
            observations.push(facts.map(|facts| {
                Observation {
                    projection: EndpointControlObservation {
                        state: facts.state,
                        health: facts.health,
                        generation: None,
                        control: self
                            .handle
                            .proxy
                            .protocols
                            .endpoint_control_supported(binding),
                        live_contraction: self
                            .handle
                            .proxy
                            .protocols
                            .endpoint_live_direction_contraction(binding),
                    },
                    facts,
                }
            }));
        }
        Ok(ProxyEndpointQuery {
            rows,
            observations,
            config,
            details: self.details,
        })
    }
}
impl ProxyEndpointQuery<'_> {
    pub fn body(&self) -> impl Serialize + '_ {
        wire::Body(self)
    }
}
impl Serialize for ProxyEndpointQuery<'_> {
    fn serialize<S: Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
        let mut out = s.serialize_map(Some(1))?;
        out.serialize_entry(
            if self.details {
                "endpoint_details"
            } else if self.rows.is_list() {
                "endpoints"
            } else {
                "endpoint"
            },
            &wire::Body(self),
        )?;
        out.end()
    }
}
impl std::fmt::Debug for ProxyEndpointQueryGuard<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ProxyEndpointQueryGuard")
            .finish_non_exhaustive()
    }
}

#[cfg(all(test, feature = "wireguard"))]
mod tests;
