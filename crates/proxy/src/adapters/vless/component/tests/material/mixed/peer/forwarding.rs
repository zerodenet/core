//! This test peer parses the actual plain VLESS header and forwards real IO.
use super::*;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
pub(super) async fn serve(
    source: zero_transport::split_http::XhttpIncoming,
    stop: CancellationToken,
) {
    let mut sessions = tokio::task::JoinSet::new();
    loop {
        tokio::select! {biased;
            _=stop.cancelled()=>break,
            accepted=source.accept()=> {
                let Some(io)=accepted else {break};
                sessions.spawn(forward(io));
            }
            joined=sessions.join_next(),if !sessions.is_empty()=>{joined.unwrap().unwrap();}
        }
    }
    source.close();
    sessions.abort_all();
    while let Some(joined) = sessions.join_next().await {
        assert!(joined.is_ok() || joined.unwrap_err().is_cancelled());
    }
}
async fn forward(mut io: zero_transport::split_http::XhttpStream) {
    let mut header = [0u8; 26];
    io.read_exact(&mut header).await.unwrap();
    assert_eq!(header[0], 0);
    assert_eq!(
        &header[1..17],
        &[
            0x11, 0x11, 0x11, 0x11, 0x22, 0x22, 0x33, 0x33, 0x44, 0x44, 0x55, 0x55, 0x55, 0x55,
            0x55, 0x55
        ]
    );
    assert_eq!(header[17], 0, "test has no Vision/addons");
    assert_eq!(header[21], 1);
    let target = SocketAddr::from((
        [header[22], header[23], header[24], header[25]],
        u16::from_be_bytes([header[19], header[20]]),
    ));
    io.write_all(&[0, 0]).await.unwrap();
    io.flush().await.unwrap();
    if header[18] == 1 {
        let mut peer = TcpStream::connect(target).await.unwrap();
        // An intentionally abandoned first logical is allowed to reset this
        // independent library peer's half. This is not any Kernel close proof.
        let _ = tokio::io::copy_bidirectional(&mut io, &mut peer).await;
    } else {
        assert_eq!(header[18], 2);
        let peer = tokio::net::UdpSocket::bind("127.0.0.1:0").await.unwrap();
        peer.connect(target).await.unwrap();
        loop {
            let length = match io.read_u16().await {
                Ok(length) => length as usize,
                Err(source)
                    if matches!(
                        source.kind(),
                        std::io::ErrorKind::UnexpectedEof | std::io::ErrorKind::ConnectionReset
                    ) =>
                {
                    break
                }
                Err(source) => panic!("real UDP stream header: {source}"),
            };
            let mut bytes = vec![0; length];
            io.read_exact(&mut bytes).await.unwrap();
            assert_eq!(peer.send(&bytes).await.unwrap(), length);
            let mut reply = vec![0; 65535];
            let received = peer.recv(&mut reply).await.unwrap();
            io.write_u16(received as u16).await.unwrap();
            io.write_all(&reply[..received]).await.unwrap();
            io.flush().await.unwrap();
        }
    }
}
