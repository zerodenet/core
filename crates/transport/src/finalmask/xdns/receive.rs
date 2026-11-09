//! One actual native scratch, then paid complete DNS originals through parsing.
use super::*;
use quinn::udp::RecvMeta;
use std::io::IoSliceMut;
pub(super) async fn receive(
    socket: &dyn AsyncUdpSocket,
    scratch: &mut super::super::memory::Scratch,
    memory: &super::super::memory::Memory,
) -> io::Result<Vec<Packet>> {
    let mut meta = [RecvMeta::default()];
    let count = std::future::poll_fn(|cx| {
        socket.poll_recv(cx, &mut [IoSliceMut::new(&mut scratch.bytes)], &mut meta)
    })
    .await?;
    if count == 0 {
        return Ok(Vec::new());
    }
    let meta = meta[0];
    if meta.len > scratch.bytes.len()
        || meta.stride == 0
        || meta.len.div_ceil(meta.stride) > socket.max_receive_segments().max(1)
    {
        return Err(wire::invalid());
    }
    let mut packets = Vec::new();
    for bytes in scratch.bytes[..meta.len].chunks(meta.stride) {
        let Ok(peak) = wire::storage_peak(bytes) else {
            continue;
        };
        // Drop-new on actual UDP pressure; existing pending replies, security
        // state and accepted output retain their original custody.
        let credit = match memory.receive(peak) {
            Ok(credit) => credit,
            Err(source) if source.kind() == io::ErrorKind::WouldBlock => continue,
            Err(source) => return Err(source),
        };
        packets.push(Packet {
            bytes: bytes.to_vec(),
            peer: meta.addr,
            metadata: None,
            _credit: credit.map(Into::into),
        });
    }
    Ok(packets)
}
