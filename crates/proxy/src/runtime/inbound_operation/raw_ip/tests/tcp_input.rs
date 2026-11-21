//! Real host TCP accept/admission/children over the standalone software stack.
//! This fixture acquires no TUN descriptor and claims no hardware close receipt.
use crate::runtime::{
    inbound_owners::{InboundOwner, InboundOwnerRegistry, InboundTaskAccess},
    route_runtime::{InboundRouteRuntimeFactory, SharedIngressRuntimeServices},
    Proxy,
};
use std::{num::NonZeroUsize, sync::Arc, time::Duration};
use tokio::{sync::watch, task::JoinHandle, time::Instant};
use zero_engine::EngineError;
use zero_stack::UserTcpStack;

pub(crate) struct Input {
    owner: Arc<InboundOwner>,
    registry: Arc<InboundOwnerRegistry>,
    host: InboundTaskAccess,
    monitor: JoinHandle<Result<(), EngineError>>,
    proxy: Proxy,
    tag: String,
}
pub(crate) async fn start(proxy: &Proxy, tcp: Arc<UserTcpStack>, tag: &str) -> Input {
    let registry = Arc::new(InboundOwnerRegistry::default());
    let (stop, mut shutdown) = watch::channel(false);
    // One original accept driver plus its single actual TCP child.
    let installed = registry.install(tag, &proxy.protocols, 2, stop).unwrap();
    let group = installed.child_tasks(NonZeroUsize::MIN).unwrap();
    let native = super::native_obligations::NativeObligations::new(1, 0, 0);
    native
        .bind_source(installed.source_registrar().scope_identity())
        .unwrap();
    installed.register_after_scope(native.resource()).unwrap();
    let owner = installed.owner();
    let host = installed.tasks();
    let accept_host = host.clone();
    let cancellation = host.cancellation();
    let runtime = InboundRouteRuntimeFactory::new(
        SharedIngressRuntimeServices::new(proxy.tcp_runtime_services()),
        tag.into(),
    );
    owner
        .spawn_driver(async move {
            tokio::select! { biased;
                _ = cancellation.cancelled() => Ok(()),
                _ = shutdown.changed() => Ok(()),
                result = super::tcp::accept(
                    tcp, runtime, group, accept_host, native, None,
                    super::tcp::TcpSourceTasks::fixture(),
                ) => result,
            }
        })
        .unwrap();
    let observed = owner.clone();
    let monitor = tokio::spawn(async move { observed.observe_listener().await });
    Input {
        owner,
        registry,
        host,
        monitor,
        proxy: proxy.clone(),
        tag: tag.into(),
    }
}
impl Input {
    pub(crate) async fn shutdown(self) {
        self.host.request_stop();
        self.proxy
            .protocols
            .drive_component_supervision(async {
                tokio::time::timeout(Duration::from_secs(3), self.monitor)
                    .await
                    .unwrap()
                    .unwrap()
                    .unwrap();
                let receipt = self
                    .registry
                    .confirm_shutdown(&self.tag, Instant::now() + Duration::from_secs(3))
                    .await
                    .unwrap()
                    .expect("the same software input owner");
                let terminal = self
                    .owner
                    .terminal_for_test()
                    .expect("actual original ingress Scope consumed");
                assert!(terminal.failure().is_none() && terminal.task_failures().is_empty());
                assert!(terminal.joined_tasks() >= 1);
                self.registry
                    .release_closed_reported(receipt, |_| Ok(()))
                    .unwrap();
            })
            .await;
    }
}
