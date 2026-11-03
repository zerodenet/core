//! Actual packet I/O preserves native ancillary facts without another worker.
use super::*;
pub(crate) async fn receive(socket: &dyn AsyncUdpSocket) -> io::Result<Vec<Packet>> {
    receive_with_metadata(socket, false).await
}
pub(crate) async fn receive_native(socket: &dyn AsyncUdpSocket) -> io::Result<Vec<Packet>> {
    receive_with_metadata(socket, true).await
}
async fn receive_with_metadata(
    socket: &dyn AsyncUdpSocket,
    native: bool,
) -> io::Result<Vec<Packet>> {
    let mut bytes = vec![0; 65536];
    let mut meta = [RecvMeta::default()];
    let count = std::future::poll_fn(|cx| {
        socket.poll_recv(cx, &mut [IoSliceMut::new(&mut bytes)], &mut meta)
    })
    .await?;
    if count == 0 {
        return Ok(Vec::new());
    }
    if native && meta[0].len == 0 {
        return Ok(vec![Packet {
            bytes: Vec::new(),
            peer: meta[0].addr,
            metadata: Some(WireMetadata {
                ecn: meta[0].ecn,
                source_ip: None,
                destination_ip: meta[0].dst_ip,
                segment_size: None,
            }),
        }]);
    }
    if native
        && (meta[0].stride == 0
            || meta[0].len > bytes.len()
            || meta[0].len.div_ceil(meta[0].stride) > socket.max_receive_segments().max(1))
    {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "original UDP receive metadata exceeds actual buffer/segment capabilities",
        ));
    }
    if meta[0].stride == 0 || meta[0].len > bytes.len() {
        return Ok(Vec::new());
    }
    Ok(bytes[..meta[0].len]
        .chunks(meta[0].stride)
        .map(|p| Packet {
            bytes: p.to_vec(),
            peer: meta[0].addr,
            metadata: native.then_some(WireMetadata {
                ecn: meta[0].ecn,
                source_ip: None,
                destination_ip: meta[0].dst_ip,
                segment_size: None,
            }),
        })
        .collect())
}
pub(crate) async fn send(socket: &Arc<dyn AsyncUdpSocket>, packet: Packet) -> io::Result<()> {
    if let Some(segment) = packet.metadata.and_then(|metadata| metadata.segment_size) {
        if packet.bytes.len().div_ceil(segment) > socket.max_transmit_segments().max(1)
            || packet.bytes.len() > 65507
        {
            for bytes in packet.bytes.chunks(segment) {
                send_one(socket, &packet, bytes, None).await?;
            }
            return Ok(());
        }
    }
    send_one(
        socket,
        &packet,
        &packet.bytes,
        packet.metadata.and_then(|metadata| metadata.segment_size),
    )
    .await
}
async fn send_one(
    socket: &Arc<dyn AsyncUdpSocket>,
    packet: &Packet,
    bytes: &[u8],
    segment_size: Option<usize>,
) -> io::Result<()> {
    let transmit = Transmit {
        contents: bytes,
        destination: packet.peer,
        ecn: packet.metadata.and_then(|metadata| metadata.ecn),
        segment_size,
        src_ip: packet.metadata.and_then(|metadata| metadata.source_ip),
    };
    let mut poller = socket.clone().create_io_poller();
    loop {
        match socket.try_send(&transmit) {
            Err(error) if error.kind() == io::ErrorKind::WouldBlock => {
                std::future::poll_fn(|cx| poller.as_mut().poll_writable(cx)).await?
            }
            result => return result,
        }
    }
}
