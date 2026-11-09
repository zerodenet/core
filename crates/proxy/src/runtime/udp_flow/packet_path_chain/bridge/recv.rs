use crate::protocol_registry::packet_memory::business::BusinessMemory;
use std::sync::Arc;
use zero_traits::packet_metadata::PacketMetadataArc;

use tracing::{debug, warn};
use zero_core::Address;

use super::waiter::Waiters;
use crate::runtime::udp_flow::packet_path::{DatagramCodec, PacketPathCarrier};

pub(in crate::runtime::udp_flow::packet_path_chain) async fn recv_loop(
    path: Arc<dyn PacketPathCarrier>,
    waiters: PacketMetadataArc<Waiters>,
    memory: BusinessMemory,
    codec: Arc<dyn DatagramCodec<Address, Error = zero_core::Error>>,
    mut scratch: super::super::receiver::ReceiveScratch,
) -> Result<(), zero_platform_tokio::contract::ChannelError> {
    let buf = &mut scratch.bytes;
    loop {
        let read = match path.recv_from(buf).await {
            Ok(read) => read,
            Err(error) => {
                warn!(error = %error, "packet path recv loop stopped");
                return Err(zero_platform_tokio::contract::ChannelError::new(
                    zero_platform_tokio::contract::Stage::Receive,
                    zero_platform_tokio::contract::Reason::Io,
                    zero_platform_tokio::contract::Impact::Session,
                )
                .with_source(error));
            }
        };
        let decoded = match memory.decode(codec.as_ref(), &buf[..read]) {
            Ok(Some(d)) => d,
            Ok(None) => {
                warn!(bytes = read, "failed to decode inner datagram response");
                continue;
            }
            Err(source)
                if crate::protocol_registry::packet_memory::business::is_pressure(&source) =>
            {
                warn!(%source,"original Host cannot admit this new UDP decode workspace");
                tokio::task::consume_budget().await;
                continue;
            }
            Err(source) => {
                return Err(zero_platform_tokio::contract::ChannelError::new(
                    zero_platform_tokio::contract::Stage::Receive,
                    zero_platform_tokio::contract::Reason::Capacity,
                    zero_platform_tokio::contract::Impact::Session,
                )
                .with_source(source))
            }
        };
        debug!(
            target = ?decoded.0,
            port = decoded.1,
            bytes = decoded.2.len(),
            "decoded packet path datagram response"
        );
        if let Some(waiter) = waiters.take_matching(&decoded.0, decoded.1) {
            let (target, port, payload) = decoded;
            if let Err(source) = waiter
                .reply
                .send((target, port, payload, Some(waiter.session)))
            {
                warn!(%source,"packet path response owner cannot admit new output");
            }
        } else {
            warn!(
                target = ?decoded.0,
                port = decoded.1,
                "no waiter for packet path datagram response"
            );
        }
    }
}
