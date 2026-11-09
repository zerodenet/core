//! The existing one-task host driver consumes one actual granted native view.
use super::*;
use std::{
    net::IpAddr,
    pin::Pin,
    task::{Context, Poll},
};
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};
use zero_core::component::ComponentInstanceId;
use zero_platform_tokio::contract::{
    component::{GrantedPacketDevice, PacketDeviceIo},
    scope::{TaskJoinWitness, TaskRegistrar},
    ChannelError, DeliveryState, Impact, Reason, Stage,
};

pub(crate) struct PendingScopedDevice {
    device: Arc<HostPacketDevice>,
    instance: ComponentInstanceId,
    scope: zero_platform_tokio::contract::scope::TaskScopeIdentity,
    input: oneshot::Sender<GrantedPacketDevice>,
    witness: TaskJoinWitness,
    pending: oneshot::Receiver<()>,
}
impl PendingScopedDevice {
    pub(crate) async fn wait_input_pending(
        &mut self,
        deadline: tokio::time::Instant,
    ) -> Result<(), ChannelError> {
        tokio::time::timeout_at(deadline, &mut self.pending)
            .await
            .map_err(|source| {
                failure(
                    Reason::Timeout,
                    "original Packet driver has not polled its input",
                )
                .with_source(source)
            })?
            .map_err(|source| {
                failure(Reason::Io, "original Packet driver ended before its input")
                    .with_source(source)
            })
    }
    pub(crate) fn witness(&self) -> TaskJoinWitness {
        self.witness.clone()
    }
    pub(crate) fn install(self, grant: GrantedPacketDevice) -> Result<(), ChannelError> {
        if grant.identity.instance() != self.instance
            || !grant.identity.scope().same_scope(&self.scope)
            || !grant.original.device().same_provider(&grant.identity)
            || !grant.original.driver().same_registration(&self.witness)
        {
            return Err(failure(
                Reason::StalePlan,
                "unique Packet view differs from its actual native provider",
            ));
        }
        self.device
            .native_provider
            .set(grant.original.clone())
            .map_err(|_| {
                failure(
                    Reason::StalePlan,
                    "Packet native provider was already installed",
                )
            })?;
        self.input.send(grant).map_err(|_returned| {
            failure(
                Reason::Revoked,
                "original driver ended before taking its unique native view",
            )
        })
    }
}
impl HostPacketDevice {
    /// The lifecycle stores the returned device before calling native acquire.
    /// Registration precedes acquisition, and there is no local Tokio fallback.
    pub(crate) fn prepare_scoped(
        config: zero_config::DirectPacketDeviceConfig,
        mtu: u16,
        instance: ComponentInstanceId,
        tasks: &TaskRegistrar,
        options: &crate::runtime::raw_ip::RawIpStackOptions,
    ) -> Result<(Arc<Self>, PendingScopedDevice), ChannelError> {
        let (writes, rx) = mpsc::channel::<Write>(config.write_queue.get());
        let returns = Arc::new(PacketReturns::new_for_options(options, true, None).map_err(
            |source| {
                failure(
                    Reason::Capacity,
                    "actual Packet return limits exceed retained originals",
                )
                .with_source(source)
            },
        )?);
        let sessions = session::Sessions::new(
            returns.clone(),
            config.router_addresses.clone(),
            usize::from(mtu),
        );
        let (stop, stopped) = watch::channel(false);
        let (activate, activated) = watch::channel(false);
        let (view_ready, ready) = watch::channel(false);
        let (input, native) = oneshot::channel::<GrantedPacketDevice>();
        let (pending, input_pending) = oneshot::channel();
        let packets = returns.clone();
        let logical = sessions.clone();
        let (read_pause, checkpoint) = super::pause::ReadPause::new(tasks.scope_identity());
        let worker = task::DriverTask::scoped(tasks, async move {
            let mut pending = Some(pending);
            let mut native = Box::pin(native);
            let grant = std::future::poll_fn(|cx| {
                use std::future::Future;
                let result = native.as_mut().poll(cx);
                if result.is_pending() {
                    if let Some(pending) = pending.take() {
                        let _ = pending.send(());
                    }
                }
                result
            })
            .await;
            let Ok(grant) = grant else {
                // The real driver attempt acquired no I/O view. Its actual
                // native acquisition error remains in lifecycle/start and the
                // original runtime provider; no synthetic task fault is minted.
                return task::DriverResult {
                    failure: None,
                    native: None,
                };
            };
            let view = DeviceView {
                io: grant.io,
                name: grant.interface,
            };
            view_ready.send_replace(true);
            let (returned, failure) = driver::run(
                view,
                driver::NativeReadInput {
                    grant: grant.native_read,
                    pause: Some(super::pause::ReadCheckpoint {
                        checkpoint,
                        #[cfg(windows)]
                        sdk: grant.sdk_read_control,
                    }),
                },
                rx,
                packets,
                logical,
                stopped,
                activated,
            )
            .await
            .parts();
            drop(returned); // Returns only the unique view, never native close.
            task::DriverResult {
                failure,
                native: None,
            }
        })
        .map_err(|source| {
            failure(
                Reason::Capacity,
                "same Packet driver Scope rejected registration",
            )
            .with_source(source)
        })?;
        let witness = worker.witness().expect("same scoped original registration");
        read_pause.bind(&witness)?;
        sessions
            .driver
            .set(witness.clone())
            .map_err(|_| failure(Reason::StalePlan, "same Packet driver already installed"))?;
        let device = Arc::new(Self {
            read_pause: Some(read_pause),
            config,
            writes,
            returns,
            sessions,
            stop,
            worker,
            mtu: usize::from(mtu),
            activate,
            ready,
            native_provider: std::sync::OnceLock::new(),
        });
        let pending = PendingScopedDevice {
            device: device.clone(),
            instance,
            scope: tasks.scope_identity(),
            input,
            witness,
            pending: input_pending,
        };
        Ok((device, pending))
    }
    pub(crate) async fn wait_native_view(&self) -> io::Result<()> {
        let mut ready = self.ready.clone();
        while !*ready.borrow_and_update() {
            ready
                .changed()
                .await
                .map_err(|source| io::Error::other(source))?;
        }
        if self.native_provider.get().is_none() {
            return Err(io::Error::other(
                "scoped driver has no exact native provider binding",
            ));
        }
        Ok(())
    }
    pub(crate) fn open_packet(
        self: &Arc<Self>,
        request: zero_platform_tokio::contract::packet::PacketRequest,
        context: zero_platform_tokio::contract::context::OpenContext,
    ) -> Result<zero_platform_tokio::contract::packet::BoxPacketSession, ChannelError> {
        let _acquiring = self
            .read_pause
            .as_ref()
            .map(|pause| pause.try_enter())
            .transpose()?;
        if !self.usable()
            || self
                .native_provider
                .get()
                .is_none_or(|original| original.is_retired())
        {
            return Err(failure(
                Reason::Revoked,
                "standard native Packet component is not published",
            ));
        }
        if context.upstream.binding().dial != zero_traits::DialPolicy::default() {
            return Err(failure(
                Reason::Unsupported,
                "native Packet cannot enforce socket dial constraints",
            ));
        }
        let caller = context.upstream.packet_caller(
            &context.identity,
            &zero_platform_tokio::contract::network::CarrierContext {
                session: None,
                deadline: context.deadline,
                cancel: context.cancel.clone(),
            },
        )?;
        self.sessions
            .open(self.writes.clone(), request, context, caller)
            .map(zero_platform_tokio::contract::packet::erase_session)
    }
    pub(crate) fn logical_sessions_closed(&self) -> bool {
        self.sessions.logical_closed()
    }
    pub(crate) async fn join_scoped_driver(&self) -> io::Result<()> {
        self.close();
        let joined = self.worker.join().await.map(|_| ());
        self.sessions.finish_driver();
        joined
    }
}
fn failure(reason: Reason, message: &str) -> ChannelError {
    let mut error = ChannelError::new(Stage::Prepare, reason, Impact::Component)
        .with_source(io::Error::other(message.to_owned()));
    error.delivery = DeliveryState::NotSubmitted;
    error
}
struct DeviceView {
    io: PacketDeviceIo,
    name: Arc<str>,
}
impl AsyncRead for DeviceView {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buffer: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        self.get_mut().io.as_mut().poll_read(cx, buffer)
    }
}
impl AsyncWrite for DeviceView {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        bytes: &[u8],
    ) -> Poll<io::Result<usize>> {
        self.get_mut().io.as_mut().poll_write(cx, bytes)
    }
    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        self.get_mut().io.as_mut().poll_flush(cx)
    }
    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        self.get_mut().io.as_mut().poll_shutdown(cx)
    }
}
impl zero_tun::TunDevice for DeviceView {
    fn configure(&self, _: IpAddr, _: IpAddr, _: u16) -> io::Result<()> {
        Err(io::Error::new(
            io::ErrorKind::Unsupported,
            "borrowed native Packet configuration is read-only",
        ))
    }
    fn name(&self) -> &str {
        &self.name
    }
}
