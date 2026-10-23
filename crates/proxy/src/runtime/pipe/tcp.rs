use zero_core::Session;
use zero_engine::EngineError;

use crate::runtime::tcp_ingress::TcpIngressRuntime;
use crate::transport::TcpRouteResult;

use super::contract::KernelPipe;

/// TCP connection pipe.
pub(crate) struct TcpPipe<'a> {
    runtime: &'a TcpIngressRuntime,
}

impl<'a> TcpPipe<'a> {
    pub(crate) fn new(runtime: &'a TcpIngressRuntime) -> Self {
        Self { runtime }
    }
}

/// Input for one TCP connection dispatch.
pub(crate) struct TcpPipeInput<'a> {
    pub(crate) session: &'a mut Session,
    pub(crate) admitted: Option<crate::component::business_demand::AdmittedTcpDemand>,
}

impl KernelPipe for TcpPipe<'_> {
    type Input<'a> = TcpPipeInput<'a>;
    type Output = TcpRouteResult;
    type Error = EngineError;

    async fn dispatch(&mut self, input: Self::Input<'_>) -> Result<Self::Output, Self::Error> {
        let services = self.runtime.runtime_services();
        let work = Box::pin(crate::runtime::tcp_dispatch::dispatch_tcp(
            self.runtime,
            input.session,
            input.admitted,
        ));
        services.protocols().drive_component_supervision(work).await
    }
}
