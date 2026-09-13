use super::{PreparedInboundServiceOperation, ServiceContext};
use crate::{
    protocol_registry::TcpRuntimeServices, runtime::route_runtime::SharedIngressRuntimeServices,
};
use tokio::{sync::watch, task::JoinSet};
use zero_engine::EngineError;

#[derive(Default)]
pub(crate) struct InboundServices {
    tasks: JoinSet<Result<(), EngineError>>,
}
impl InboundServices {
    pub(crate) fn is_empty(&self) -> bool {
        self.tasks.is_empty()
    }
    pub(crate) async fn join_next(
        &mut self,
    ) -> Option<Result<Result<(), EngineError>, tokio::task::JoinError>> {
        self.tasks.join_next().await
    }
    pub(crate) async fn replace(
        &mut self,
        prepared: Vec<Box<dyn PreparedInboundServiceOperation>>,
        services: TcpRuntimeServices,
        shutdown: watch::Receiver<bool>,
    ) {
        // Abort and join old owners before starting replacements. Worker JoinSet
        // drop aborts child routes and drops protocol-held connections/status.
        self.tasks.abort_all();
        while self.tasks.join_next().await.is_some() {}
        for operation in prepared {
            let context = ServiceContext {
                upstream: services.upstream(),
                ingress: SharedIngressRuntimeServices::new(services.clone()),
            };
            let mut shutdown = shutdown.clone();
            self.tasks.spawn(async move {
                if *shutdown.borrow_and_update() {
                    return Ok(());
                }
                tokio::select! {
                    result = operation.run(context) => result,
                    _ = shutdown.changed() => Ok(()),
                }
            });
        }
    }
}
