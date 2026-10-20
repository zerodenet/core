use super::*;
use zero_core::{request::Destination, Address};
#[derive(Debug)]
struct RichSource {
    original: io::Error,
    marker: Arc<()>,
}
impl fmt::Display for RichSource {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "real carrier write: {}", self.original)
    }
}
impl Error for RichSource {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        Some(&self.original)
    }
}
struct RichStream {
    inner: tokio::io::DuplexStream,
    marker: Arc<()>,
}
impl AsyncRead for RichStream {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        Pin::new(&mut self.inner).poll_read(cx, buf)
    }
}
impl AsyncWrite for RichStream {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        match Pin::new(&mut self.inner).poll_write(cx, buf) {
            Poll::Ready(Err(original)) => Poll::Ready(Err(io::Error::new(
                original.kind(),
                RichSource {
                    original,
                    marker: self.marker.clone(),
                },
            ))),
            other => other,
        }
    }
    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.inner).poll_flush(cx)
    }
    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.inner).poll_shutdown(cx)
    }
}
#[tokio::test]
async fn actual_broken_pipe_preserves_the_original_private_error_payload_across_protocol_error_mapping(
) {
    let (io, peer) = tokio::io::duplex(4096);
    drop(peer);
    let marker = Arc::new(());
    let (mut io, observed) = Carrier::new(Box::pin(RichStream {
        inner: io,
        marker: marker.clone(),
    }));
    let state = crate::transport::ShadowsocksTransportState::default();
    let prepared = state
        .prepare_tcp(crate::component::TcpPreparation {
            server: "127.0.0.1",
            port: 443,
            protocol: crate::transport::ShadowsocksOutboundOptionsRef {
                cipher: "aes-128-gcm",
                password: "password",
            },
            replay_policy: Default::default(),
            limits: Default::default(),
            plugin: None,
            dial: Default::default(),
        })
        .unwrap();
    let result = prepared
        .config
        .establish_tcp_to(
            &mut io,
            &Destination::new(Address::Domain("native.example".into()), 443),
        )
        .await;
    assert!(result.is_err());
    assert_eq!(observed.written(), 0);
    let captured = observed.error().unwrap();
    let original = captured
        .source()
        .unwrap()
        .downcast_ref::<io::Error>()
        .unwrap();
    assert_eq!(original.kind(), io::ErrorKind::BrokenPipe);
    let rich = original
        .get_ref()
        .unwrap()
        .downcast_ref::<RichSource>()
        .unwrap();
    assert!(Arc::ptr_eq(&rich.marker, &marker));
    assert_eq!(rich.original.kind(), io::ErrorKind::BrokenPipe);
}
