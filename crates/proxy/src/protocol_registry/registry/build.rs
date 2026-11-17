use std::sync::Arc;

use super::ProtocolRegistry;
use crate::protocol_registry::ProtocolSupportCapability;

#[cfg(feature = "udp-runtime")]
type PacketPathProjectionFn<T> =
    fn(
        &T,
        &zero_config::OutboundConfig,
    ) -> Result<Option<super::ColdPacketPathProjection>, zero_engine::EngineError>;

impl ProtocolRegistry {
    pub(crate) fn register_standard_protocol<T>(&mut self, adapter: Arc<T>)
    where
        T: ProtocolSupportCapability + 'static,
    {
        assert!(
            !adapter.has_inbound() || self.inbounds.has_factory(adapter.name()),
            "standard inbound requires its actual component factory"
        );
        self.push_registered_entry(super::RegisteredProtocolEntry {
            support: adapter,
            #[cfg(feature = "udp-runtime")]
            packet_projection: None,
        });
    }
    /// Existing optional cold projection, separate from executable claims.
    /// Calling it cannot construct TCP/UDP operations or select another pool.
    #[cfg(feature = "udp-runtime")]
    pub(crate) fn register_standard_packet_carrier<T>(
        &mut self,
        adapter: Arc<T>,
        projection: PacketPathProjectionFn<T>,
    ) where
        T: ProtocolSupportCapability + 'static,
    {
        self.register_standard_protocol(adapter.clone());
        self.entries
            .last_mut()
            .expect("same registered protocol")
            .packet_projection = Some(Arc::new(move |input| projection(adapter.as_ref(), input)));
    }
    // Endpoint hooks are kept neutral even in builds without a registered
    // endpoint protocol. WireGuard is the current opt-in implementation.
    #[allow(dead_code, reason = "some builds register no endpoint controller")]
    pub(crate) fn register_endpoint_controller<
        T: crate::protocol_registry::EndpointControlCapability + 'static,
    >(
        &mut self,
        adapter: Arc<T>,
    ) {
        self.endpoint_controllers.push(adapter);
    }

    #[allow(dead_code, reason = "some builds register no endpoint observer")]
    pub(crate) fn register_endpoint_observer<
        T: crate::protocol_registry::EndpointObservationCapability + 'static,
    >(
        &mut self,
        adapter: Arc<T>,
    ) {
        self.endpoint_observers.push(adapter);
    }

    fn push_registered_entry(&mut self, entry: super::RegisteredProtocolEntry) {
        let name = entry.support.name();
        assert!(
            !self
                .entries
                .iter()
                .any(|registered| registered.support.name() == name),
            "protocol capability `{name}` registered more than once"
        );
        self.entries.push(entry);
    }
}
