//! Protocol EOF must leave the carrier write half usable until reverse EOF.
use super::*;
use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc,
};
struct Observed<S> {
    inner: S,
    shutdown: Arc<AtomicBool>,
}
impl<S: AsyncRead + AsyncWrite + Unpin + Send + Sync> AsyncSocket for Observed<S> {
    type Error = io::Error;
    async fn read(&mut self, bytes: &mut [u8]) -> io::Result<usize> {
        AsyncReadExt::read(&mut self.inner, bytes).await
    }
    async fn write_all(&mut self, bytes: &[u8]) -> io::Result<()> {
        AsyncWriteExt::write_all(&mut self.inner, bytes).await?;
        AsyncWriteExt::flush(&mut self.inner).await
    }
    async fn shutdown(&mut self) -> io::Result<()> {
        AsyncWriteExt::shutdown(self).await
    }
}
impl<S: AsyncRead + Unpin> AsyncRead for Observed<S> {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        Pin::new(&mut self.inner).poll_read(cx, buf)
    }
}
impl<S: AsyncWrite + Unpin> AsyncWrite for Observed<S> {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        Pin::new(&mut self.inner).poll_write(cx, buf)
    }
    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.inner).poll_flush(cx)
    }
    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        self.shutdown.store(true, Ordering::Release);
        Pin::new(&mut self.inner).poll_shutdown(cx)
    }
}
#[tokio::test]
async fn vmess_framed_upload_end_flushes_without_closing_the_reverse_carrier() {
    tokio::time::timeout(std::time::Duration::from_secs(8), async {
        for cipher in [VmessCipher::Aes128Gcm, VmessCipher::Chacha20Poly1305] {
            let (client, server) = tokio::io::duplex(1024);
            let shutdown = Arc::new(AtomicBool::new(false));
            let server_observation = shutdown.clone();
            let upload = build_payload(262144);
            let mut response = upload.clone();
            response.extend(build_payload(1025));
            let expected = response.clone();
            let peer = async move {
                let profile = VmessInboundProfile::from_config_users([(
                    "11111111-2222-3333-4444-555555555555".to_owned(),
                    cipher.name().to_owned(),
                    None::<String>,
                    None::<u64>,
                    None::<u64>,
                    None::<u32>,
                    None::<u64>,
                    None::<u64>,
                )])
                .unwrap();
                let (_, mut io) = profile
                    .accept_tcp_stream(VmessInbound, TestSocket(server))
                    .await
                    .unwrap();
                let mut bytes = Vec::new();
                AsyncReadExt::read_to_end(&mut io, &mut bytes)
                    .await
                    .unwrap();
                assert!(
                    bytes == build_payload(262144),
                    "complete upload bytes={} of 262144",
                    bytes.len()
                );
                assert!(
                    !server_observation.load(Ordering::Acquire),
                    "a genuine VMess EOF arrived while the native write half is still open"
                );
                AsyncWriteExt::write_all(&mut io, &expected).await.unwrap();
                AsyncWriteExt::shutdown(&mut io).await.unwrap();
            };
            let caller = async move {
                let request = PreparedVmessOutboundRequestBundle::from_config(
                    "11111111-2222-3333-4444-555555555555",
                    cipher.name(),
                    None,
                )
                .unwrap();
                let target = Session::new(
                    0,
                    Address::Ipv4([127, 0, 0, 1]),
                    443,
                    Network::Tcp,
                    ProtocolType::new("vmess"),
                );
                let (mut io, _) = request
                    .establish_tcp_outbound_stream(
                        Observed {
                            inner: client,
                            shutdown: shutdown.clone(),
                        },
                        &target,
                    )
                    .await
                    .unwrap();
                AsyncWriteExt::write_all(&mut io, &upload).await.unwrap();
                AsyncWriteExt::shutdown(&mut io).await.unwrap();
                assert!(!shutdown.load(Ordering::Acquire));
                assert_eq!(
                    AsyncWriteExt::write_all(&mut io, b"after upload EOF")
                        .await
                        .unwrap_err()
                        .kind(),
                    io::ErrorKind::BrokenPipe
                );
                let mut bytes = Vec::new();
                AsyncReadExt::read_to_end(&mut io, &mut bytes)
                    .await
                    .unwrap();
                assert!(
                    bytes == response,
                    "complete reverse bytes={} of {} first difference {:?}",
                    bytes.len(),
                    response.len(),
                    bytes.iter().zip(&response).position(|(a, b)| a != b)
                );
                assert!(
                    shutdown.load(Ordering::Acquire),
                    "both logical ends must actually drive the same native shutdown"
                );
            };
            tokio::join!(caller, peer);
        }
    })
    .await
    .expect("original 8s complete reverse tail deadline");
}

#[tokio::test]
async fn vmess_framed_reverse_end_does_not_close_a_still_active_upload() {
    tokio::time::timeout(std::time::Duration::from_secs(8), async {
        let (client, server) = tokio::io::duplex(1024);
        let shutdown = Arc::new(AtomicBool::new(false));
        let observed = shutdown.clone();
        let peer = async move {
            let profile = VmessInboundProfile::from_config_users([(
                "11111111-2222-3333-4444-555555555555".to_owned(),
                "aes-128-gcm".to_owned(),
                None::<String>,
                None::<u64>,
                None::<u64>,
                None::<u32>,
                None::<u64>,
                None::<u64>,
            )])
            .unwrap();
            let (_, mut io) = profile
                .accept_tcp_stream(
                    VmessInbound,
                    Observed {
                        inner: server,
                        shutdown: observed.clone(),
                    },
                )
                .await
                .unwrap();
            AsyncWriteExt::write_all(&mut io, b"reverse before upload")
                .await
                .unwrap();
            AsyncWriteExt::shutdown(&mut io).await.unwrap();
            assert!(!observed.load(Ordering::Acquire));
            let mut upload = Vec::new();
            AsyncReadExt::read_to_end(&mut io, &mut upload)
                .await
                .unwrap();
            assert!(
                upload == build_payload(262144),
                "complete still-active upload bytes={} of 262144",
                upload.len()
            );
            assert!(observed.load(Ordering::Acquire));
        };
        let caller = async move {
            let request = PreparedVmessOutboundRequestBundle::from_config(
                "11111111-2222-3333-4444-555555555555",
                "aes-128-gcm",
                None,
            )
            .unwrap();
            let target = Session::new(
                0,
                Address::Ipv4([127, 0, 0, 1]),
                443,
                Network::Tcp,
                ProtocolType::new("vmess"),
            );
            let (mut io, _) = request
                .establish_tcp_outbound_stream(TestSocket(client), &target)
                .await
                .unwrap();
            let mut reverse = Vec::new();
            AsyncReadExt::read_to_end(&mut io, &mut reverse)
                .await
                .unwrap();
            assert_eq!(reverse, b"reverse before upload");
            assert!(!shutdown.load(Ordering::Acquire));
            AsyncWriteExt::write_all(&mut io, &build_payload(262144))
                .await
                .unwrap();
            AsyncWriteExt::shutdown(&mut io).await.unwrap();
        };
        tokio::join!(caller, peer);
    })
    .await
    .expect("original 8s reverse-first halfclose deadline");
}
