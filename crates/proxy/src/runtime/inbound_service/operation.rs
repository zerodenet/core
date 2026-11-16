//! Actual factory/source invocation and cold task admission; no legacy worker SPI.
use crate::runtime::route_runtime::SharedIngressRuntimeServices;
use zero_engine::EngineError;
pub(crate) struct ServiceContext {
    pub(super) native: super::native::Installed,
    pub(super) retained: std::sync::Arc<super::native::Retained>,
    pub(super) max_tasks: std::num::NonZeroUsize,
    pub(super) max_resources: usize,
    pub(crate) ingress: SharedIngressRuntimeServices,
    pub(crate) tasks: zero_platform_tokio::contract::scope::WeakTaskRegistrar,
    pub(crate) cancel: zero_platform_tokio::contract::CancellationToken,
}
/// Admission backpressure happens before any new carrier preparation. This
/// delay is neither a readiness probe nor native-close/retirement evidence.
pub(super) async fn worker_launch(
    tasks: &zero_platform_tokio::contract::scope::WeakTaskRegistrar,
    cancel: &zero_platform_tokio::contract::CancellationToken,
    retained: &super::native::Retained,
) -> Result<Option<zero_platform_tokio::contract::scope::TaskLaunch>, EngineError> {
    use zero_platform_tokio::contract::scope::{SpawnError, TaskClass};
    loop {
        if cancel.is_cancelled() {
            return Ok(None);
        }
        retained.reap().await?;
        match tasks.reserve_launch(TaskClass::Auxiliary) {
            Ok(launch) => return Ok(Some(launch)),
            Err(SpawnError::Closing) => return Ok(None),
            Err(SpawnError::Capacity) => {
                tokio::select! {biased;
                    _=cancel.cancelled()=>return Ok(None),
                    _=tokio::time::sleep(std::time::Duration::from_millis(100))=>{},
                }
            }
            Err(source) => return Err(std::io::Error::other(source).into()),
        }
    }
}
