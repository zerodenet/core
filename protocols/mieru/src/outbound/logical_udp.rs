use super::*;
/// UDP ASSOCIATE on an already established, independently closable session.
pub(crate) async fn establish<S>(mut stream: S) -> Result<MieruUdpFlowConnection, Error>
where
    S: AsyncRead + AsyncWrite + Send + Unpin + 'static,
{
    crate::tunnel::request_udp_associate(&mut stream).await?;
    let (send_tx, mut send_rx) = mpsc::channel::<zero_core::UdpFlowPacket>(32);
    let (responses, _) = broadcast::channel(32);
    let output = responses.clone();
    tokio::spawn(async move {
        let (mut read, mut write) = tokio::io::split(stream);
        let read_loop = async {
            loop {
                let mut header = [0; 3];
                read.read_exact(&mut header).await?;
                if header[0] != 0 {
                    return Err::<(), io::Error>(io::ErrorKind::InvalidData.into());
                }
                let size = u16::from_be_bytes([header[1], header[2]]) as usize;
                let mut frame = vec![0; size + 4];
                frame[..3].copy_from_slice(&header);
                read.read_exact(&mut frame[3..]).await?;
                let packet =
                    crate::udp::decode_udp_flow_packet(&frame).map_err(io::Error::other)?;
                let _ = output.send(packet.into_parts());
            }
        };
        let write_loop = async {
            while let Some(packet) = send_rx.recv().await {
                let frame = crate::udp::encode_udp_flow_packet(
                    &packet.target,
                    packet.port,
                    &packet.payload,
                )
                .map_err(io::Error::other)?;
                write.write_all(&frame).await?;
                write.flush().await?;
            }
            Ok::<(), io::Error>(())
        };
        tokio::select! {_=read_loop=>{},_=write_loop=>{}}
    });
    Ok(MieruUdpFlowConnection::new(MieruUdpFlowSession::new(
        MieruUdpFlowHandle {
            sender: MieruUdpFlowSender { send_tx },
            responses,
        },
    )))
}
