//! Projection only from the actual Engine B receipt and installed caller source.
#[cfg(test)]
use crate::protocol_registry::TcpExecutionServices;
#[cfg(test)]
use zero_core::{component::SessionId, Session};
#[cfg(test)]
use zero_engine::EngineError;
#[cfg(test)]
use zero_platform_tokio::contract::context::RequestIdentity;
use zero_platform_tokio::contract::scope::TaskJoinWitness;

pub(in crate::runtime::packet_flow) struct FlowCaller {
    pub(super) witness: TaskJoinWitness,
}
impl FlowCaller {
    #[cfg(test)]
    pub(in crate::runtime::packet_flow) fn from_tcp(
        services: &TcpExecutionServices,
        session: &Session,
    ) -> Result<Option<Self>, EngineError> {
        let Some(invocation) = &services.invocation else {
            return Ok(None);
        };
        let Some(admitted) = &invocation.admitted else {
            return Ok(None);
        };
        let RequestIdentity::Business(facts) = &invocation.identity else {
            return Err(
                std::io::Error::other("packet caller requires original business facts").into(),
            );
        };
        if facts.session != SessionId(session.id) {
            return Err(std::io::Error::other(
                "packet caller identity differs from the admitted session",
            )
            .into());
        }
        admitted.check_live(services.engine(), facts.session)?;
        if let Some(caller) = &invocation.caller_native {
            caller.check_open().map_err(std::io::Error::other)?;
            return caller
                .caller_witness()
                .map(|witness| Some(Self { witness }))
                .ok_or_else(|| {
                    std::io::Error::other("raw packet caller has no original tracked task").into()
                });
        }
        if let Some(caller) = &invocation.caller_accepted {
            caller.check_open().map_err(std::io::Error::other)?;
            return caller
                .task_join_witness()
                .map(|witness| Some(Self { witness }))
                .ok_or_else(|| {
                    std::io::Error::other("accepted packet caller has no original tracked task")
                        .into()
                });
        }
        Ok(None)
    }
}
