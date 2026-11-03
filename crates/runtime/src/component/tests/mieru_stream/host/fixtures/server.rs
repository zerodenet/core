use super::*;
pub(super) struct Observed {
    pub inner: Box<dyn zero_platform_tokio::contract::lifecycle::DynResourceLifecycle>,
    pub closes: Arc<AtomicUsize>,
}
impl zero_platform_tokio::contract::lifecycle::ResourceLifecycle for Observed {
    async fn start(&mut self, deadline: Instant) -> Result<(), BuildError> {
        self.inner.start(deadline).await
    }
    fn revoke(&self, reason: zero_platform_tokio::contract::CloseReason) {
        self.inner.revoke(reason);
    }
    async fn shutdown(
        &mut self,
        deadline: Instant,
    ) -> Result<zero_platform_tokio::contract::lifecycle::CloseReport, BuildError> {
        let report = self.inner.shutdown(deadline).await?;
        self.closes
            .fetch_add(report.closed_io.len(), Ordering::SeqCst);
        Ok(report)
    }
}
pub struct Server {
    pub address: SocketAddr,
    pub accepts: Arc<AtomicUsize>,
    stop: CancellationToken,
    task: tokio::task::JoinHandle<()>,
}
impl Server {
    pub async fn packet() -> Self {
        let socket = Arc::new(tokio::net::UdpSocket::bind("127.0.0.1:0").await.unwrap());
        let address = socket.local_addr().unwrap();
        let stop = CancellationToken::new();
        let cancel = stop.clone();
        let accepts = Arc::new(AtomicUsize::new(0));
        let count = accepts.clone();
        let task = tokio::spawn(async move {
            let mut peers =
                std::collections::HashMap::<SocketAddr, tokio::sync::mpsc::Sender<Vec<u8>>>::new();
            let mut connections = tokio::task::JoinSet::new();
            let mut buffer = vec![0; 65536];
            loop {
                let (size, peer) = tokio::select! { biased;
                    _ = cancel.cancelled() => break,
                    result = socket.recv_from(&mut buffer) => result.unwrap(),
                };
                let sender = peers.entry(peer).or_insert_with(|| {
                    count.fetch_add(1, Ordering::SeqCst);
                    let (sender, receiver) = tokio::sync::mpsc::channel(64);
                    let socket = socket.clone();
                    let cancel = cancel.clone();
                    connections.spawn(async move {
                        let profile = mieru::inbound::MieruInboundProfile::from_config(vec![("u".into(),"p".into())]);
                        let connection = tokio::select! { biased;
                            _ = cancel.cancelled() => return,
                            result = profile.accept_packet_peer(socket, peer, receiver) => result.unwrap(),
                        };
                        let mut streams = tokio::task::JoinSet::new();
                        loop {
                            let stream = tokio::select! { biased;
                                _ = cancel.cancelled() => break,
                                result = connection.accept_next() => result.unwrap(),
                            };
                            let Some(mut stream) = stream else { break; };
                            let cancel = cancel.clone();
                            streams.spawn(async move {
                                tokio::select! { biased;
                                    _ = cancel.cancelled() => {},
                                    _ = async {
                                        let mut request = [0; 10];
                                        if stream.read_exact(&mut request).await.is_err() { return; }
                                        assert!(request == [5,1,0,1,203,0,113,1,1,187] || request == [5,3,0,1,0,0,0,0,0,0]);
                                        stream.write_all(&[5,0,0,1,0,0,0,0,0,0]).await.unwrap();
                                        stream.flush().await.unwrap();
                                        let (mut read, mut write) = tokio::io::split(stream);
                                        let _ = tokio::io::copy(&mut read, &mut write).await;
                                    } => {},
                                }
                            });
                        }
                        connection.close("fixture shutdown");
                        while let Some(result) = streams.join_next().await { result.unwrap(); }
                    });
                    sender
                });
                let _ = sender.send(buffer[..size].to_vec()).await;
            }
            while let Some(result) = connections.join_next().await {
                result.unwrap();
            }
        });
        Self {
            address,
            accepts,
            stop,
            task,
        }
    }
    pub async fn new() -> Self {
        Self::tcp("127.0.0.1:0").await
    }
    pub async fn closing_logical() -> Self {
        Self::tcp_mode("127.0.0.1:0", true).await
    }
    pub async fn tcp(address: &str) -> Self {
        Self::tcp_mode(address, false).await
    }
    async fn tcp_mode(address: &str, close_logical: bool) -> Self {
        let listener = TcpListener::bind(address).await.unwrap();
        let address = listener.local_addr().unwrap();
        let stop = CancellationToken::new();
        let cancel = stop.clone();
        let accepts = Arc::new(AtomicUsize::new(0));
        let count = accepts.clone();
        let task = tokio::spawn(async move {
            let mut connections = tokio::task::JoinSet::new();
            loop {
                tokio::select! { biased;
                    _=cancel.cancelled()=>break,
                    result=listener.accept()=>{
                        let (socket,_) = result.unwrap(); count.fetch_add(1,Ordering::SeqCst);
                        connections.spawn(async move {
                            let connection = mieru::inbound::MieruInboundProfile::from_config(vec![("u".into(),"p".into())]).accept_multiplexer(TcpRelayStream::new(socket)).await.unwrap();
                            let mut streams = tokio::task::JoinSet::new();
                            while let Ok(Some(mut stream)) = connection.accept_next().await {
                                streams.spawn(async move {
                                    let mut request=[0;10]; if stream.read_exact(&mut request).await.is_err() { return; }
                                    assert!(request==[5,1,0,1,203,0,113,1,1,187] || request==[5,3,0,1,0,0,0,0,0,0]);
                                    stream.write_all(&[5,0,0,1,0,0,0,0,0,0]).await.unwrap(); stream.flush().await.unwrap();
                                    if close_logical {
                                        let mut bytes=[0;4];stream.read_exact(&mut bytes).await.unwrap();stream.write_all(&bytes).await.unwrap();stream.shutdown().await.unwrap();
                                    } else {
                                        let (mut read,mut write)=tokio::io::split(stream); let _=tokio::io::copy(&mut read,&mut write).await;
                                    }
                                });
                            }
                            while let Some(result)=streams.join_next().await { result.unwrap(); }
                        });
                    }
                }
            }
            while let Some(result) = connections.join_next().await {
                result.unwrap();
            }
        });
        Self {
            address,
            accepts,
            stop,
            task,
        }
    }
    pub async fn close(self) {
        self.stop.cancel();
        self.task.await.unwrap();
    }
}
