//! Real native TCP authority; this fixture never manufactures a carrier/control pair.
use super::*;
use zero_platform_tokio::{
    controlled_carrier::{CarrierGrant, NativeCarrierOpener, PreparedCarrier},
    EgressInterfaceControl,
};

pub(in crate::component) fn controlled_context(
    limit: usize,
    peer: std::net::SocketAddr,
    observations: Arc<Loopback>,
) -> (TaskScope, BuildContext) {
    let (scope, mut context) = context_with(limit, observations.clone());
    let egress = Arc::new(EgressInterfaceControl::default());
    let binding = UpstreamBinding {
        network_generation: NetworkGeneration(egress.generation()),
        ..binding()
    };
    let native = NativeCarrierOpener::new(
        context.instance(),
        binding.clone(),
        peer,
        egress,
        Some(observations.traffic.clone()),
    )
    .unwrap();
    context.background_carrier = Some(UpstreamTransport::new(
        binding,
        Arc::new(ControlledLoopback {
            native,
            observations,
        }),
    ));
    (scope, context)
}

struct ControlledLoopback {
    native: NativeCarrierOpener,
    observations: Arc<Loopback>,
}
impl UpstreamOpener for ControlledLoopback {
    fn prepare_controlled_stream(
        &self,
        binding: &UpstreamBinding,
        destination: Destination,
        context: CarrierContext,
        grant: CarrierGrant,
    ) -> Result<PreparedCarrier, ChannelError> {
        assert_eq!(context.session, None);
        assert!(!context.cancel.is_cancelled());
        let observed = (destination.clone(), context.deadline);
        let prepared =
            self.native
                .prepare_controlled_stream(binding, destination, context, grant)?;
        self.observations.opens.fetch_add(1, Ordering::SeqCst);
        self.observations
            .destinations
            .lock()
            .unwrap()
            .push(observed);
        Ok(prepared)
    }
    fn open_stream(
        &self,
        _: &UpstreamBinding,
        _: Destination,
        _: CarrierContext,
    ) -> BoxFuture<'_, Result<BoxStream, ChannelError>> {
        panic!("controlled Mieru must never call the old BoxStream opener")
    }
    fn open_datagram(
        &self,
        _: &UpstreamBinding,
        _: DatagramRequest,
        _: CarrierContext,
    ) -> BoxFuture<'_, Result<BoxDatagramSession, ChannelError>> {
        denied()
    }
}
