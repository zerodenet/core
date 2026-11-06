use super::*;
use zero_platform_tokio::contract::{DeliveryState, Reason, SendFailure, SendReceipt};
pub(super) fn send_failure(
    reason: Reason,
    message: &str,
    delivery: DeliveryState,
    source: Option<Arc<io::Error>>,
) -> SendFailure {
    let error = zero_platform_tokio::contract::ChannelError::new(
        zero_platform_tokio::contract::Stage::Send,
        reason,
        zero_platform_tokio::contract::Impact::Packet,
    );
    let error = match source {
        Some(source) => error.with_source(SharedIo(source)),
        None => error.with_source(io::Error::other(message.to_owned())),
    };
    SendFailure::new(error, delivery)
}
pub(super) async fn owned_write<W: tokio::io::AsyncWrite + Unpin>(
    mut write: session::OwnedWrite,
    writer: &mut W,
    stopped: &mut watch::Receiver<bool>,
    failure: &mut Option<Arc<io::Error>>,
) -> bool {
    if write.is_cancelled() {
        write.finish(Err(send_failure(
            Reason::Revoked,
            "Packet session closed before native write",
            DeliveryState::NotSubmitted,
            None,
        )));
        return false;
    }
    let hop = packet::ip_hop_limit(write.packet.payload());
    if !packet::advance_ip_hop(write.packet.payload_mut()) {
        write.finish(Err(send_failure(
            Reason::Protocol,
            "invalid native Packet hop",
            DeliveryState::NotSubmitted,
            None,
        )));
        return false;
    }
    let mtu = write.mtu();
    let mut fragments = if write.bytes().len() > mtu {
        packet::fragment_forwarded_packet(write.bytes(), mtu)
            .into_iter()
            .map(Into::into)
            .collect::<Vec<zero_traits::PacketBuffer>>()
    } else {
        Vec::new()
    };
    if write.bytes().len() > mtu && fragments.is_empty() {
        if let Some(hop) = hop {
            packet::restore_ip_hop(write.packet.payload_mut(), hop);
        }
        write.finish(Err(send_failure(
            Reason::Protocol,
            "native Packet cannot be fragmented",
            DeliveryState::NotSubmitted,
            None,
        )));
        return false;
    }
    // Fragmentation is an actual L3 transformation. The queued original stays
    // owned by this same physical driver until all its writes finish.
    let original_length = write.bytes().len();
    let count = fragments.len().max(1);
    for index in 0..count {
        let bytes = if fragments.is_empty() {
            write.bytes()
        } else {
            fragments[index].as_ref()
        };
        let result = write_one(&write, writer, bytes, stopped).await;
        match result {
            Ok(bytes) => write.observed_sent(bytes),
            Err(source) => {
                let source = Arc::new(source);
                if failure.is_none() {
                    *failure = Some(source.clone());
                }
                write.observed_error();
                write.finish(Err(send_failure(
                    Reason::Io,
                    "native Packet write did not confirm local acceptance",
                    DeliveryState::Unknown,
                    Some(source),
                )));
                // Unknown ends this original driver; no logical reopen may
                // resume native IO before its actual Scope/native cleanup.
                return true;
            }
        }
    }
    fragments.clear();
    write.finish(Ok(SendReceipt::accepted(original_length)));
    false
}
async fn write_one<W: tokio::io::AsyncWrite + Unpin>(
    write: &session::OwnedWrite,
    writer: &mut W,
    bytes: &[u8],
    stopped: &mut watch::Receiver<bool>,
) -> io::Result<usize> {
    let cancellation = write.cancellation();
    tokio::select! { biased;
        _=stopped.changed()=>Err(io::Error::new(io::ErrorKind::Interrupted,"native Packet device stopped during original write")),
        _=cancellation.cancelled()=>Err(io::Error::new(io::ErrorKind::Interrupted,"Packet session stopped during original write")),
        result=writer.write(bytes)=>result.and_then(|size| if size==bytes.len(){Ok(size)}else{Err(io::Error::new(io::ErrorKind::WriteZero,"partial native Packet write"))}),
    }
}
