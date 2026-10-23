use crate::runtime::route_runtime::InboundRouteRuntime;

#[derive(Clone)]
pub(crate) struct InboundConnectionContext {
    pub(super) runtime: InboundRouteRuntime,
}

impl InboundConnectionContext {
    #[cfg(test)]
    pub(crate) fn accepted_tasks(&self) -> Option<zero_platform_tokio::AcceptedTcpTasks> {
        self.runtime.accepted_tasks()
    }

    pub(crate) fn new(runtime: InboundRouteRuntime) -> Self {
        Self { runtime }
    }
}
