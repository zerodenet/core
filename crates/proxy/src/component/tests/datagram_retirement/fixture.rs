//! A real TCP control lifetime fixture, not a simulated protocol peer.
use std::sync::Arc;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    sync::Notify,
    time::{timeout_at, Instant},
};
use zero_platform_tokio::contract::{
    lifecycle::{erase_control, ChannelControl, CloseReport, DynChannelControl, Unfinished},
    CancellationToken, ChannelError, CloseReason, Impact, Reason, Stage,
};
pub(super) fn deadline() -> Instant {
    Instant::now() + std::time::Duration::from_secs(3)
}
struct TcpControl {
    socket: Option<TcpStream>,
    shutdown: bool,
    cancel: CancellationToken,
}
impl ChannelControl for TcpControl {
    fn cancel(&self, _: CloseReason) {
        self.cancel.cancel();
    }
    async fn wait_closed(&mut self, deadline: Instant) -> Result<CloseReport, ChannelError> {
        self.cancel(CloseReason::Shutdown);
        let close = async {
            let socket = self.socket.as_mut().expect("owned real TCP fixture");
            if !self.shutdown {
                socket.shutdown().await.map_err(|source| {
                    ChannelError::new(Stage::Close, Reason::Io, Impact::Session).with_source(source)
                })?;
                self.shutdown = true;
            }
            let mut byte = [0; 1];
            let size = socket.read(&mut byte).await.map_err(|source| {
                ChannelError::new(Stage::Close, Reason::Io, Impact::Session).with_source(source)
            })?;
            if size != 0 {
                return Err(
                    ChannelError::new(Stage::Close, Reason::Protocol, Impact::Session).with_source(
                        std::io::Error::new(
                            std::io::ErrorKind::InvalidData,
                            format!("unexpected real TCP control byte {}", byte[0]),
                        ),
                    ),
                );
            }
            self.socket.take();
            Ok(CloseReport {
                closed_io: vec!["actual TCP fixture logical directions".into()],
                ..Default::default()
            })
        };
        timeout_at(deadline, close).await.unwrap_or_else(|_| {
            Ok(CloseReport {
                unfinished: vec![Unfinished::Io("actual TCP fixture peer EOF".into())],
                ..Default::default()
            })
        })
    }
}
pub(super) async fn tcp_control(
    unexpected: bool,
) -> (
    Box<dyn DynChannelControl>,
    Arc<Notify>,
    Arc<Notify>,
    tokio::task::JoinHandle<()>,
) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let socket = TcpStream::connect(listener.local_addr().unwrap())
        .await
        .unwrap();
    let eof = Arc::new(Notify::new());
    let release = Arc::new(Notify::new());
    let (ended, released) = (eof.clone(), release.clone());
    let task = tokio::spawn(async move {
        let (mut peer, _) = listener.accept().await.unwrap();
        let mut bytes = Vec::new();
        peer.read_to_end(&mut bytes).await.unwrap();
        assert!(bytes.is_empty());
        if unexpected {
            peer.write_all(&[37]).await.unwrap();
        }
        ended.notify_one();
        released.notified().await;
        peer.shutdown().await.unwrap();
    });
    (
        erase_control(TcpControl {
            socket: Some(socket),
            shutdown: false,
            cancel: CancellationToken::new(),
        }),
        eof,
        release,
        task,
    )
}
