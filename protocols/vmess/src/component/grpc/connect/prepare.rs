//! The original TLS/H2 futures become one registered transport driver.
use super::*;
use crate::component::grpc::state::Body;
use zero_platform_tokio::{
    contract::scope::TaskClass,
    controlled_carrier::{CarrierIo, CarrierUserView},
};
pub(super) type Prepared = zero_platform_tokio::controlled_carrier::CarrierDriver<
    zero_transport::grpc::PreparedSingleClient,
>;
impl Managed {
    pub(super) async fn prepare_transport(
        &self,
        io: CarrierIo,
        prepared_tls: Option<zero_transport::tls::OwnedPreparedClient>,
        context: &OpenContext,
    ) -> Result<Prepared, ChannelError> {
        let owner = self;
        let session = context.session_identity()?;
        let preparing = io.prepare_driver(|io| async {
            let io = zero_transport::TcpRelayStream::new(crate::component::carrier::Carrier::new(
                Box::pin(io),
            ));
            let prepared_tls = if let Some(material) = &owner.options.base.material {
                Some(material.prepare(owner.ech_resolver.as_deref()).await?)
            } else {
                prepared_tls
            };
            let io = if let Some(prepared) = prepared_tls {
                prepared.connect(io).await?
            } else if let Some(tls) = &owner.options.base.tls {
                zero_transport::tls::connect_tls_stream(io, tls, None, &owner.options.base.server)
                    .await?
            } else {
                io
            };
            let settings = io
                .application_settings()
                .filter(|settings| settings.protocol == b"h2")
                .map(|settings| settings.peer.clone());
            zero_transport::grpc::PreparedSingleClient::prepare(
                io,
                &owner.options.grpc,
                &owner.options.authority,
                settings.as_deref(),
            )
            .await
        });
        let tag = owner
            .wait(context, Stage::Handshake, async {
                preparing.await.map_err(|source| {
                    owner
                        .admission
                        .error(
                            session,
                            Stage::Handshake,
                            Reason::Io,
                            DeliveryState::NotSubmitted,
                        )
                        .with_source(source)
                })
            })
            .await?;
        Ok(tag)
    }
    pub(super) fn finish_stream(
        &self,
        tag: Prepared,
        record: Arc<Record>,
        context: &OpenContext,
        user_view: Option<CarrierUserView>,
        epoch: (
            zero_core::component::NetworkGeneration,
            Option<zero_platform_tokio::contract::dns::DnsGeneration>,
        ),
    ) -> Result<zero_transport::TcpRelayStream, ChannelError> {
        let owner = self;
        let session = context.session_identity()?;
        let (io, driver) = tag.split(zero_transport::grpc::PreparedSingleClient::into_parts);
        let body = Body(record.clone());
        let stop = record.stop.clone();
        let scope_stop = owner.tasks.cancellation();
        let instance = owner.instance;
        let (_,witness) = record.adoption.spawn_driver(&owner.tasks,TaskClass::Auxiliary,driver,|driver|async move {
            let _body = body;
            tokio::select! { biased;
                _=stop.cancelled()=>Ok(()),
                _=scope_stop.cancelled()=>Ok(()),
                result=driver=>result.map_err(|source|{
                    let mut error = error(instance,Stage::Receive,Reason::Io).with_source(source);
                    error.delivery = DeliveryState::Unknown; error.session = session; error
                }),
            }
        }).map_err(|source|owner.admission.attach(source,session))?;
        record.install_witness(witness);
        owner
            .check()
            .map_err(|source| owner.admission.attach(source, session))?;
        if owner.options.node()
            && (
                context.upstream.binding().network_generation,
                matches!(
                    owner.options.base.endpoint.address,
                    zero_core::Address::Domain(_)
                )
                .then(|| context.upstream.carrier_dns_generation())
                .flatten(),
            ) != epoch
        {
            record.stop.cancel();
            return Err(owner.admission.error(
                session,
                Stage::Handshake,
                Reason::StalePlan,
                DeliveryState::NotSubmitted,
            ));
        }
        let io = zero_transport::TcpRelayStream::new(super::super::wire::Stream::new(
            io,
            record.clone(),
            user_view,
        ));
        Ok(io)
    }
}
