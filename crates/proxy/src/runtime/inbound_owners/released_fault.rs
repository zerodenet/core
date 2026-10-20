//! A private marker is issued only after real physical proof and publication.
use super::*;
use crate::component::instance::SharedError;
#[derive(Debug)]
struct ReleasedInboundFault {
    identity: ComponentInstanceId,
    source: SharedError,
}
impl std::fmt::Display for ReleasedInboundFault {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        std::fmt::Display::fmt(&self.source, f)
    }
}
impl std::error::Error for ReleasedInboundFault {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(self.source.as_ref())
    }
}
pub(crate) fn released_fault_identity(error: &EngineError) -> Option<ComponentInstanceId> {
    let EngineError::Io(error) = error else {
        return None;
    };
    error
        .get_ref()?
        .downcast_ref::<ReleasedInboundFault>()
        .map(|fault| fault.identity)
}
impl InboundOwnerRegistry {
    pub(crate) fn release_closed_reported(
        &self,
        receipt: ClosedInboundReceipt,
        publish: impl FnOnce(&InboundTerminal) -> Result<(), EngineError>,
    ) -> Result<(), EngineError> {
        let fault = self.release_closed(receipt, |terminal| {
            publish(terminal)?;
            Ok(terminal.failure().map(|source| ReleasedInboundFault {
                identity: terminal.identity(),
                source,
            }))
        })?;
        // No external constructor can claim this disposition by providing an
        // arbitrary ID, a string, an error or an empty CloseReport.
        fault.map_or(Ok(()), |source| Err(std::io::Error::other(source).into()))
    }
}
