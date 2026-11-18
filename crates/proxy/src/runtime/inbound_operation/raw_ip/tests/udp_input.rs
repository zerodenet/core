//! Stack input uses a real host, caller groups and original Direct/DNS providers.
//! The in-memory packet input itself makes no native-device close claim.
use crate::runtime::inbound_owners::{InboundOwner, InboundOwnerRegistry, InboundTaskAccess};
use crate::{
    inbound::tun::udp::{OwnedTunUdpTasks, UdpScratch},
    runtime::{
        inbound_operation::raw_ip::native_obligations::NativeObligations,
        udp_ingress::UdpIngressRuntime, Proxy,
    },
    RunningProxy,
};
use std::{
    sync::{atomic::AtomicU64, Arc},
    time::Duration,
};
use tokio::{sync::watch, task::JoinHandle, time::Instant};

pub(crate) struct Input {
    owner: Arc<InboundOwner>,
    host: InboundTaskAccess,
    registry: Arc<InboundOwnerRegistry>,
    monitor: JoinHandle<Result<(), zero_engine::EngineError>>,
    running: RunningProxy,
    tag: String,
    // Only packet-working storage custody, not a fabricated device provider.
    _read: zero_tun::packet_memory::NativeReadGrant,
}

pub(crate) async fn start(
    proxy: Proxy,
    stack: Arc<zero_stack::UserUdpStack>,
    tag: String,
    dns_hijack: bool,
    dns_hijacked_queries: Arc<AtomicU64>,
) -> Input {
    let running = proxy.spawn();
    running
        .wait_ready(Instant::now() + Duration::from_secs(3))
        .await
        .unwrap();
    let registry = proxy.inbound_owners.clone();
    let (stop, mut shutdown) = watch::channel(false);
    let installed = registry.install(&tag, &proxy.protocols, 2, stop).unwrap();
    let owner = installed.owner();
    owner.prepare_packet_source_pause();
    let host = installed.tasks();
    let native = NativeObligations::new(0, 8, 8);
    native
        .bind_source(installed.source_registrar().scope_identity())
        .unwrap();
    installed.register_after_scope(native.resource()).unwrap();
    let tasks = OwnedTunUdpTasks::new(native, 8, 8);
    tasks.install(&installed).unwrap();
    let read = proxy
        .protocols
        .runtime_host()
        .original_budgets()
        .packet
        .native_grant(65_535)
        .unwrap();
    let scratch = UdpScratch::prepare(&read.memory_view().unwrap()).unwrap();
    let runtime = UdpIngressRuntime::new(proxy.tcp_runtime_services().for_inbound(tag.clone()));
    let cancel = host.cancellation().clone();
    let driver_tag = tag.clone();
    owner
        .spawn_driver(async move {
            tokio::select! { biased;
                _ = cancel.cancelled() => Ok(()),
                _ = shutdown.changed() => Ok(()),
                result = crate::inbound::tun::udp::tests::fixtures::run_owned(
                    runtime, stack, driver_tag, tasks, scratch,
                    dns_hijack, dns_hijacked_queries,
                ) => result,
            }
        })
        .unwrap();
    let monitored = owner.clone();
    let monitor = tokio::spawn(async move { monitored.observe_listener().await });
    Input {
        owner,
        host,
        registry,
        monitor,
        running,
        tag,
        _read: read,
    }
}

impl Input {
    pub(crate) async fn shutdown(self) {
        // Same order as production: stop the original peer group, consume its
        // full observer and confirm original resources and Scope before removal.
        self.host.request_stop();
        tokio::time::timeout(Duration::from_secs(5), self.monitor)
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        let receipt = self
            .registry
            .confirm_shutdown(&self.tag, Instant::now() + Duration::from_secs(3))
            .await
            .unwrap()
            .expect("same original input owner");
        assert!(self.owner.terminal_for_test().is_some());
        self.registry.release_closed(receipt, |_| Ok(())).unwrap();
        drop(self._read);
        self.running.shutdown().await.unwrap();
    }
}
