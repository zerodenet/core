use crate::runtime::route_runtime::InboundRouteRuntime;

#[derive(Clone)]
pub(crate) struct InboundConnectionContext {
    pub(super) runtime: InboundRouteRuntime,
}

impl InboundConnectionContext {
    pub(crate) fn new(runtime: InboundRouteRuntime) -> Self {
        Self { runtime }
    }
}
