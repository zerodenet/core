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
    input: tokio::sync::mpsc::Sender<(Vec<u8>, tokio::sync::oneshot::Sender<()>)>,
    wire: zero_platform_tokio::contract::scope::TaskJoinWitness,
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
    let limits = super::limits::RawIpSourceLimits {
        tcp: 0,
        udp: 8,
        dns: 8,
        icmp: 0,
        tcp_dns: 0,
    };
    let installed = registry
        .install(
            &tag,
            &proxy.protocols,
            limits.task_limit(false).unwrap(),
            stop,
        )
        .unwrap();
    let owner = installed.owner();
    let pause = owner.prepare_packet_source_pause();
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
    let (begin, started) = tokio::sync::oneshot::channel();
    let (input, mut packets) =
        tokio::sync::mpsc::channel::<(Vec<u8>, tokio::sync::oneshot::Sender<()>)>(1);
    let wire_stack = stack.clone();
    owner
        .spawn_driver(async move {
            started.await.map_err(std::io::Error::other)?;
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
    let wire_cancel = host.cancellation().clone();
    let (bind, bound) = tokio::sync::oneshot::channel::<
        zero_platform_tokio::contract::packet::PacketSourceCheckpoint,
    >();
    let (_, wire) = installed
        .source_registrar()
        .spawn_tracked(
            zero_platform_tokio::contract::scope::TaskClass::Critical,
            async move {
                let checkpoint = bound.await.map_err(|source| {
                    zero_platform_tokio::contract::ChannelError::new(
                        zero_platform_tokio::contract::Stage::Prepare,
                        zero_platform_tokio::contract::Reason::Revoked,
                        zero_platform_tokio::contract::Impact::Component,
                    )
                    .with_source(source)
                })?;
                // This is the real packet producer used by feed(), not an unrelated
                // pending task standing in for a Wire registration or native receipt.
                loop {
                    tokio::select! { biased;
                        _ = wire_cancel.cancelled() => return Ok(()),
                        request = checkpoint.requested() => {
                            checkpoint.acknowledge(request, packets.is_empty());
                            checkpoint.resumed(request).await;
                        },
                        frame = packets.recv() => match frame {
                            Some((frame, delivered)) => {
                                use zero_traits::UdpStack;
                                wire_stack.feed(&frame).await;
                                let _ = delivered.send(());
                            },
                            None => return Ok(()),
                        },
                    }
                }
            },
        )
        .unwrap();
    let checkpoint = pause
        .bind(
            zero_platform_tokio::contract::packet::PacketPauseParticipant::Wire,
            &wire,
        )
        .unwrap();
    bind.send(checkpoint)
        .unwrap_or_else(|_| panic!("actual wire still owns its checkpoint receiver"));
    begin.send(()).unwrap();
    let monitored = owner.clone();
    let monitor = tokio::spawn(async move { monitored.observe_listener().await });
    Input {
        owner,
        host,
        registry,
        monitor,
        running,
        tag,
        input,
        wire,
        _read: read,
    }
}

impl Input {
    pub(crate) fn feed<'a>(
        &'a self,
        packet: &'a [u8],
    ) -> impl std::future::Future<Output = ()> + Send + 'a {
        // The worker borrows its queue, not every retained host/registry owner
        // through &Input and their unrelated recursive Sync obligations.
        let input = &self.input;
        async move {
            let (delivered, received) = tokio::sync::oneshot::channel();
            input.send((packet.to_vec(), delivered)).await.unwrap();
            received.await.unwrap();
        }
    }
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
        let joined = tokio::time::timeout(
            Duration::from_secs(3),
            self.wire.wait_registration_released(),
        )
        .await
        .unwrap();
        assert!(self.wire.matches_joined(&joined));
        assert!(joined.registration_released());
        joined.result().unwrap();
        assert!(self.owner.terminal_for_test().is_some());
        self.registry.release_closed(receipt, |_| Ok(())).unwrap();
        drop(self._read);
        self.running.shutdown().await.unwrap();
    }
}
