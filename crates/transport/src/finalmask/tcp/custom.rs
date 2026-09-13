use super::*;
async fn send(stream: &mut TcpRelayStream, sequence: &[Item]) -> io::Result<()> {
    let mut bytes = Vec::new();
    for item in sequence {
        if item.delay_ms.maximum > 0 {
            if !bytes.is_empty() {
                stream.write_all(&bytes).await?;
                stream.flush().await?;
                bytes.clear();
            }
            tokio::time::sleep(Duration::from_millis(item.delay_ms.sample())).await;
        }
        item.content.append(&mut bytes);
    }
    if !bytes.is_empty() {
        stream.write_all(&bytes).await?;
        stream.flush().await?;
    }
    Ok(())
}
async fn receive(stream: &mut TcpRelayStream, sequence: &[Item]) -> io::Result<()> {
    for item in sequence {
        let mut bytes = vec![0; item.content.length()];
        stream.read_exact(&mut bytes).await?;
        if let super::super::udp::Item::Bytes(expected) = &item.content {
            if &bytes != expected {
                return Err(invalid("FinalMask TCP header mismatch"));
            }
        }
    }
    Ok(())
}
pub(super) async fn handshake(
    stream: &mut TcpRelayStream,
    config: &Custom,
    server: bool,
) -> io::Result<()> {
    for (i, client) in config.clients.iter().enumerate() {
        if server {
            if let Err(error) = receive(stream, client).await {
                if let Some(response) = config.errors.get(i) {
                    let _ = send(stream, response).await;
                }
                return Err(error);
            }
            if let Some(response) = config.servers.get(i) {
                send(stream, response).await?;
            }
        } else {
            send(stream, client).await?;
            if let Some(response) = config.servers.get(i) {
                receive(stream, response).await?;
            }
        }
    }
    for response in config.servers.iter().skip(config.clients.len()) {
        if server {
            send(stream, response).await?;
        } else {
            receive(stream, response).await?;
        }
    }
    Ok(())
}
