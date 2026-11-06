//! The existing receive loop is the one actual owning Scope driver.
use super::*;
use zero_platform_tokio::{
    contract::{
        lifecycle::TaskClass, scope::TaskJoinWitness, CancellationToken, ChannelError, Impact,
        Reason, Stage,
    },
    controlled_carrier::{CarrierAdoption, CarrierDatagram, CarrierDriver, CarrierUserView},
};
pub(crate) struct NativePreparation {
    socket: Socket,
    endpoint: SocketAddr,
    codec: Arc<dyn DatagramCodec<Address, Error = zero_core::Error>>,
    responses: Arc<Responses>,
    subscribers: broadcast::Sender<ShadowsocksUdpResponse>,
    plugin: Option<Arc<super::super::plugin::outbound::PluginLease>>,
}
struct Active {
    native: Arc<socket::Native>,
    _original_view: CarrierUserView,
    responses: Arc<Responses>,
}
impl Drop for Active {
    fn drop(&mut self) {
        self.responses.close_pending();
        self.native.retire();
    }
}
impl ShadowsocksUdpSocketFlow {
    pub(crate) fn prepare_native(
        io: CarrierDatagram,
        codec: Arc<dyn DatagramCodec<Address, Error = zero_core::Error>>,
        plugin: Option<Arc<super::super::plugin::outbound::PluginLease>>,
        pending: usize,
        budget: Arc<responses::budget::PendingBudget>,
        stop: CancellationToken,
    ) -> (Self, CarrierDriver<NativePreparation>) {
        // split preserves the sealed original tag while the exact same view is
        // retained by the driver and its revocable sender cell.
        io.into_driver().split(|io| {
            let endpoint = io.peer();
            let native = Arc::new(socket::Native::new(Arc::new(io), stop));
            let socket = Socket::Native(native);
            let responses = Arc::new(Responses::with_budget(pending, budget));
            let (subscribers, _) = broadcast::channel(32);
            let flow = Self {
                plugin: plugin.clone(),
                socket: socket.clone(),
                endpoint,
                codec: codec.clone(),
                recv_tx: subscribers.downgrade(),
                responses: responses.clone(),
                _receiver: None,
            };
            (
                flow,
                NativePreparation {
                    socket,
                    endpoint,
                    codec,
                    responses,
                    subscribers,
                    plugin,
                },
            )
        })
    }
    pub(crate) fn retire_native(&self) {
        if let Socket::Native(native) = &self.socket {
            native.retire();
        }
    }
    pub(crate) fn native_unavailable(&self) -> bool {
        matches!(&self.socket,Socket::Native(native) if native.unavailable())
    }
}
pub(crate) fn start(
    preparation: CarrierDriver<NativePreparation>,
    adoption: &CarrierAdoption,
    tasks: &zero_platform_tokio::contract::scope::TaskRegistrar,
    view: CarrierUserView,
    stop: CancellationToken,
) -> Result<TaskJoinWitness, ChannelError> {
    let component = adoption.instance().component;
    let scope_stop = tasks.cancellation();
    let (_,witness)=adoption.spawn_driver(tasks,TaskClass::Critical,preparation,move |prepared| {
        let Socket::Native(native)=&prepared.socket else {unreachable!("only a native preparation carries this original tag")};
        let active=Active {native:native.clone(),_original_view:view,responses:prepared.responses.clone()};
        async move {
            let _active=active;
            tokio::select! {biased;
                _=stop.cancelled()=>Ok(()),
                _=scope_stop.cancelled()=>Ok(()),
                outcome=receive(prepared)=>outcome.map_err(|source| {
                    let mut error=ChannelError::new(Stage::Receive,Reason::Io,Impact::Component).with_source(source);
                    error.component=Some(component);error
                }),
            }
        }
    })?;
    Ok(witness)
}
async fn receive(prepared: NativePreparation) -> std::io::Result<()> {
    let mut bytes = vec![0; 65535];
    loop {
        let (size, peer) = prepared.socket.receive(&mut bytes).await?;
        if peer != prepared.endpoint {
            continue;
        }
        if let Some(plugin) = &prepared.plugin {
            plugin.check()?;
        }
        let Some(response) = prepared.codec.decode(&bytes[..size]) else {
            continue;
        };
        // A cached physical connection remains available between logical users;
        // lack of a current subscriber never fabricates native closure.
        let _ = prepared.responses.deliver(response, &prepared.subscribers);
    }
}
