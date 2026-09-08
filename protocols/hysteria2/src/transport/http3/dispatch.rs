//! Distinguish the HY2 TCP frame from ordinary HTTP/3 streams before H3 parsing.
use super::super::Hysteria2Stream;
use bytes::Bytes;
use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc,
};
use tokio::{sync::mpsc, task::JoinSet};

type WebStream = Result<(quinn::SendStream, quinn::RecvStream, Bytes), quinn::ConnectionError>;
pub(super) async fn run(
    connection: quinn::Connection,
    authenticated: Arc<AtomicBool>,
    web: mpsc::Sender<WebStream>,
    tcp: mpsc::Sender<(zero_core::Session, Hysteria2Stream)>,
) {
    let mut streams = JoinSet::new();
    loop {
        tokio::select! {
            accepted = connection.accept_bi(), if streams.len() < 128 => {
                let (mut send, mut recv) = match accepted { Ok(pair) => pair, Err(error) => { let _ = web.send(Err(error)).await; break; } };
                let authenticated = authenticated.clone();
                let web = web.clone(); let tcp = tcp.clone();
                streams.spawn(async move {
                    let mut prefix = [0u8; 8];
                    let read = async {
                        recv.read_exact(&mut prefix[..1]).await?;
                        let size = 1usize << (prefix[0] >> 6);
                        if size > 1 { recv.read_exact(&mut prefix[1..size]).await?; }
                        Ok::<_, quinn::ReadExactError>(size)
                    };
                    let size = match tokio::time::timeout(std::time::Duration::from_secs(10), read).await {
                        Ok(Ok(size)) => size,
                        _ => { let _ = recv.stop(0x10cu32.into()); let _ = send.reset(0x10cu32.into()); return; }
                    };
                    let frame = crate::shared::decode_varint(&prefix[..size]).map(|(v, _)| v).unwrap_or(0);
                    let prefix = Bytes::copy_from_slice(&prefix[..size]);
                    if frame == 0x401 {
                        if authenticated.load(Ordering::Acquire) {
                            let mut stream = Hysteria2Stream::with_prefix(send, recv, prefix);
                            let acceptor = crate::inbound::Hysteria2InboundTcpAcceptor::new();
                            if let Ok(Ok(session)) = tokio::time::timeout(std::time::Duration::from_secs(10), acceptor.accept_stream(&mut stream)).await {
                                let _ = tcp.send((session, stream)).await;
                            }
                        } else { let _ = recv.stop(0x10bu32.into()); let _ = send.reset(0x10bu32.into()); }
                    } else { let _ = web.send(Ok((send, recv, prefix))).await; }
                });
            }
            _ = streams.join_next(), if !streams.is_empty() => {}
            _ = connection.closed() => break,
        }
    }
}
