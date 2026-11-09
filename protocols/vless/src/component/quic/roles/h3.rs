//! Same accepted XMUX pool; the original H3 driver owns every HTTP work item.
use super::*;
use zero_platform_tokio::contract::lifecycle::{ChannelControl, DynChannelControl};
use zero_traits::packet_metadata::PacketMetadataLayout;
use zero_transport::split_http::{OwnedLogicalReturn, XhttpCarrier};
struct Logical(OwnedLogicalReturn);
impl DynChannelControl for Logical {
    fn cancel(&self, _: zero_platform_tokio::contract::CloseReason) {
        self.0.request_return();
    }
    fn wait_closed(
        &mut self,
        end: tokio::time::Instant,
    ) -> zero_platform_tokio::contract::BoxFuture<
        '_,
        Result<zero_platform_tokio::contract::lifecycle::CloseReport, ChannelError>,
    > {
        Box::pin(ChannelControl::wait_closed(self, end))
    }
}
impl ChannelControl for Logical {
    fn cancel(&self, _: zero_platform_tokio::contract::CloseReason) {
        self.0.request_return();
    }
    async fn wait_closed(
        &mut self,
        _: tokio::time::Instant,
    ) -> Result<zero_platform_tokio::contract::lifecycle::CloseReport, ChannelError> {
        self.0.request_return();
        self.0.wait_returned().await;
        Ok(Default::default())
    }
}
impl Outbound {
    pub(super) async fn acquire_h3(
        &self,
        context: &OpenContext,
    ) -> Result<
        (
            zero_platform_tokio::TcpRelayStream,
            BoxChannelControl,
            CarrierOrigin,
        ),
        ChannelError,
    > {
        let options = &self.generation.options;
        let (pool, profile) = options.h3.as_ref().expect("accepted H3 recipe");
        let credit = self
            .generation
            .memory(PacketMetadataLayout::boxed::<Logical>())?;
        // Per attempt the current real caller supplies the establishing facts.
        // Group retains its physical connection, never the first caller's factory.
        let generation = self.generation.clone();
        let current = context.clone();
        let factory: zero_transport::split_http::XhttpCarrierFactory = Arc::new(move || {
            let generation = generation.clone();
            let mut current = current.clone();
            Box::pin(async move {
                if let Some(receiver) =
                    super::super::pending::for_current(&generation, current.upstream.binding())
                        .map_err(std::io::Error::other)?
                {
                    current.upstream = receiver;
                }
                let original = entry::Generation::acquire(&generation, &current)
                    .await
                    .map_err(std::io::Error::other)?;
                Ok(XhttpCarrier::OwnedHttp3(original.into_h3()))
            })
        });
        let (stream, origin) = pool
            .connect_owned(
                factory,
                profile,
                None,
                zero_transport::split_http::OwnedXhttpContext {
                    instance: self.generation.instance,
                    scope: self.generation.tasks.scope_identity(),
                    binding: context.upstream.binding(),
                    memory: self.generation.metadata_memory.clone(),
                },
            )
            .await
            .map_err(|source| error(Stage::Handshake, Reason::Io).with_source(source))?;
        let returned = stream
            .owned_return()
            .ok_or_else(|| error(Stage::Prepare, Reason::StalePlan))?;
        let control = BoxChannelControl::from_prepaid(Logical(returned), credit).map_err(
            |(source, _, _)| error(Stage::Prepare, Reason::Capacity).with_source(source),
        )?;
        let encrypted = options
            .plan
            .encrypt(zero_platform_tokio::TcpRelayStream::new(stream));
        let io = zero_platform_tokio::contract::context::with_deadline(context.deadline, encrypted)
            .await
            .map_err(|source| error(Stage::Handshake, Reason::Timeout).with_source(source))?
            .map_err(|source| error(Stage::Handshake, Reason::Io).with_source(source))?;
        context.upstream.observe_carrier_origin(&origin);
        Ok((io, control, origin))
    }
}
