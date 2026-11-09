//! One setup result stays in the actual Entry, even if its first waiter drops.
use super::*;
use zero_platform_tokio::{
    contract::RetainedError,
    controlled_carrier::{CarrierAdoption, CarrierDriver, CarrierPreparation, CarrierUserView},
    TcpRelayStream,
};
pub(in crate::component::quic) struct Setup {
    pub adoption: CarrierAdoption,
    pub preparation: CarrierPreparation,
    pub outcome: Outcome,
}
pub(in crate::component::quic) enum Outcome {
    Ready(CarrierDriver<(TcpRelayStream, CarrierUserView)>),
    Rejected {
        source: RetainedError,
        // This is the actual raw CarrierIo input tag, not a made-up witness.
        // A staged REALITY job already owns that same IO. The tag and unique
        // user view move into its real driver only after the setup Host ACK.
        input: Option<CarrierDriver<CarrierUserView>>,
        staged_job: bool,
    },
}
impl Setup {
    pub(super) async fn initialize(
        generation: &TcpGeneration,
        entry: &entry::Entry,
        grant: zero_platform_tokio::controlled_carrier::CarrierGrant,
        view: CarrierUserView,
        preparation: CarrierPreparation,
        upstream: zero_platform_tokio::contract::network::UpstreamTransport,
        call: zero_platform_tokio::contract::network::CarrierContext,
    ) -> Self {
        let adoption = grant.adoption();
        let endpoint = generation.options.protocol.endpoint.clone();
        let masks = { entry.owners.state.lock().await.tcp_masks.take() };
        let opening = async {
            let masks = masks.ok_or_else(|| error(Stage::Prepare, Reason::Revoked))?;
            let opened = if upstream.binding().prefix.is_empty() {
                upstream
                    .acquire_controlled_stream(endpoint, call.clone(), grant)
                    .await
            } else {
                let loan = upstream
                    .borrow_stream(endpoint.clone(), call.clone())
                    .await?;
                // The actual lower LoanOwner is strongly installed before the
                // same Slot checks this bound endpoint/prefix/dial declaration.
                grant.adopt_stream_loan(upstream.binding(), endpoint, &call, loan)
            }?;
            Ok::<_, ChannelError>((opened, masks))
        };
        let opened = zero_platform_tokio::contract::context::with_deadline(call.deadline, opening)
            .await
            .map_err(|source| error(Stage::Dial, Reason::Timeout).with_source(source))
            .and_then(|result| result);
        let outcome = match opened {
            Err(source) => Outcome::Rejected {
                source: entry.owners.receive_setup(source),
                input: None,
                staged_job: false,
            },
            Ok((io, masks)) => {
                let (io, tag) = io.into_driver().split(|io| (io, ()));
                let prepared = io
                    .prepare_driver(|io| {
                        handshake::authenticate(generation, io, preparation.clone(), masks)
                    })
                    .await;
                match prepared {
                    Ok(input) => Outcome::Ready(
                        input
                            .try_map(|io| Ok::<_, std::convert::Infallible>((io, view)))
                            .unwrap(),
                    ),
                    Err(source) => Outcome::Rejected {
                        source: entry.owners.receive_setup(source.source),
                        staged_job: source.staged_job,
                        input: Some(
                            tag.try_map(|()| Ok::<_, std::convert::Infallible>(view))
                                .unwrap(),
                        ),
                    },
                }
            }
        };
        Self {
            adoption,
            preparation,
            outcome,
        }
    }
}
