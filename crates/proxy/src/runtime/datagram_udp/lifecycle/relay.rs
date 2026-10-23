use zero_core::{DatagramUdpResponder, InboundDatagramUdpRelay, SessionAuth};
use zero_engine::EngineError;

use super::response::finish_dispatch;
#[cfg(feature = "upstream-association-runtime")]
use super::with_upstream::run_loop;
#[cfg(not(feature = "upstream-association-runtime"))]
use super::without_upstream::run_loop;
use crate::runtime::datagram_udp::contract::DatagramUdpRelayRequest;
use crate::runtime::udp_ingress::UdpIngressRuntime;

pub(super) struct DatagramUdpLoopContext<'a> {
    pub(super) runtime: &'a UdpIngressRuntime,
    pub(super) auth: Option<&'a SessionAuth>,
}

pub(crate) async fn run_protocol_datagram_udp_relay<S, R>(
    runtime: UdpIngressRuntime,
    source: S,
    relay: R,
    inbound_tag: &str,
    poll_upstream: bool,
) -> Result<(), EngineError>
where
    S: Send,
    R: InboundDatagramUdpRelay<S>,
{
    run_ready_protocol_datagram_udp_relay(runtime, source, relay, inbound_tag, poll_upstream, None)
        .await
}

pub(crate) async fn run_ready_protocol_datagram_udp_relay<S, R>(
    runtime: UdpIngressRuntime,
    source: S,
    relay: R,
    inbound_tag: &str,
    poll_upstream: bool,
    ready: Option<crate::runtime::inbound_owners::InboundReady>,
) -> Result<(), EngineError>
where
    S: Send,
    R: InboundDatagramUdpRelay<S>,
{
    let (responder, auth) = relay.into_datagram_udp_parts();
    run_datagram_udp_relay(
        runtime,
        DatagramUdpRelayRequest {
            source,
            responder,
            inbound_tag,
            poll_upstream,
            auth,
        },
        ready,
    )
    .await
}

async fn run_datagram_udp_relay<S, R>(
    runtime: UdpIngressRuntime,
    request: DatagramUdpRelayRequest<'_, S, R>,
    ready: Option<crate::runtime::inbound_owners::InboundReady>,
) -> Result<(), EngineError>
where
    S: Send,
    R: DatagramUdpResponder<S>,
{
    let DatagramUdpRelayRequest {
        source,
        mut responder,
        inbound_tag,
        poll_upstream,
        auth,
    } = request;
    let mut dispatch = runtime.new_dispatch(inbound_tag).await?;
    let mut direct_buf = vec![0_u8; 64 * 1024];
    #[cfg(feature = "upstream-association-runtime")]
    let mut upstream_buf = vec![0_u8; 64 * 1024];

    let context = DatagramUdpLoopContext {
        runtime: &runtime,
        auth: auth.as_ref(),
    };

    let result = crate::runtime::inbound_owners::InboundReady::poll_optional(ready, async {
        #[cfg(feature = "upstream-association-runtime")]
        {
            if poll_upstream {
                run_loop(
                    &context,
                    &source,
                    &mut responder,
                    &mut dispatch,
                    direct_buf.as_mut_slice(),
                    upstream_buf.as_mut_slice(),
                )
                .await?;
            } else {
                super::without_upstream::run_loop(
                    &context,
                    &source,
                    &mut responder,
                    &mut dispatch,
                    direct_buf.as_mut_slice(),
                )
                .await?;
            }
        }

        #[cfg(not(feature = "upstream-association-runtime"))]
        {
            let _ = poll_upstream;
            run_loop(
                &context,
                &source,
                &mut responder,
                &mut dispatch,
                direct_buf.as_mut_slice(),
            )
            .await?;
        }

        Ok::<(), EngineError>(())
    })
    .await;

    finish_dispatch(dispatch);

    result
}
