use crate::runtime::inbound_operation::PreparedInboundListenerOperation;
use std::{future::Future, pin::Pin};
use zero_engine::EngineError;

pub(super) fn prepare(
    request: ::vmess::transport::PreparedVmessInboundListenerRequest,
) -> Box<dyn PreparedInboundListenerOperation> {
    Box::new(Staged {
        request: Some(request),
        installed: None,
    })
}
struct Staged {
    request: Option<::vmess::transport::PreparedVmessInboundListenerRequest>,
    installed: Option<Box<dyn PreparedInboundListenerOperation>>,
}
impl PreparedInboundListenerOperation for Staged {
    fn internal_tasks(&self) -> usize {
        self.installed
            .as_ref()
            .map_or(0, |operation| operation.internal_tasks())
    }
    fn install_resources(
        &mut self,
        context: &crate::runtime::inbound_owners::InboundInstallContext,
    ) -> Result<(), EngineError> {
        if let Some(operation) = self.installed.as_mut() {
            return operation.install_resources(context);
        }
        let request = self.request.take().ok_or_else(|| {
            std::io::Error::other("VMess staged listener activation already failed")
        })?;
        // This is the host's accepted installation point, after the previous
        // actual listener owner retired. Preparation never publishes users.
        let request = request.activate().map_err(EngineError::from)?;
        let mut operation = active(request);
        operation.install_resources(context)?;
        self.installed = Some(operation);
        Ok(())
    }
    fn execute(
        mut self: Box<Self>,
        runtime: crate::runtime::route_runtime::InboundListenerRuntime,
        bound: crate::protocol_registry::BoundInbound,
        shutdown: tokio::sync::watch::Receiver<bool>,
    ) -> Pin<Box<dyn Future<Output = Result<(), EngineError>> + Send + 'static>> {
        match self.installed.take() {
            Some(operation) => operation.execute(runtime, bound, shutdown),
            None => Box::pin(async {
                Err(
                    std::io::Error::other("VMess inbound users were not installed by the host")
                        .into(),
                )
            }),
        }
    }
}

use ::vmess::transport::{VmessInboundAcceptedRoute, VmessInboundListenerRequest};

use crate::runtime::inbound_operation::TcpInboundListenerOperation;
use crate::runtime::inbound_route::NoClientMuxRouteDefaults;

fn active(
    request: VmessInboundListenerRequest,
) -> Box<dyn crate::runtime::inbound_operation::PreparedInboundListenerOperation> {
    Box::new(TcpInboundListenerOperation {
        protocol_name: request.protocol_name(),
        error_protocol_name: request.error_protocol_name(),
        request,
        dispatch:
            |request: VmessInboundListenerRequest,
             socket,
             context: crate::runtime::inbound_operation::InboundConnectionContext| async move {
                let defaults = NoClientMuxRouteDefaults {
                    udp_protocol: VmessInboundListenerRequest::UDP_PROTOCOL,
                    mux_protocol: VmessInboundListenerRequest::MUX_PROTOCOL,
                    panic_message: VmessInboundListenerRequest::PANIC_MESSAGE,
                    abort_on_end: VmessInboundListenerRequest::ABORT_ON_END,
                    read_error_log: VmessInboundListenerRequest::READ_ERROR_LOG,
                };
                match request.accept_standard_route(socket).await? {
                    VmessInboundAcceptedRoute::Tcp {
                        destination,
                        auth,
                        stream,
                    } => {
                        context
                            .serve_standard_stream(
                                ::vmess::inbound::VmessInbound.protocol(),
                                destination,
                                Some(auth),
                                None,
                                move |facts| {
                                    ::vmess::component::inbound::TcpReply::new(*stream, facts)
                                        .map(zero_platform_tokio::contract::inbound::accept_stream)
                                },
                            )
                            .await
                    }
                    VmessInboundAcceptedRoute::Legacy(route) => {
                        context.dispatch_no_client_mux_route(*route, defaults).await
                    }
                }
            },
    })
}
