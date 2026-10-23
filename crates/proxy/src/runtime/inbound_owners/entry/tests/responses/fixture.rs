use super::*;
use crate::runtime::inbound_owners::InboundPeerTasks;
pub(super) struct Live {
    pub(super) proxy: Proxy,
    pub(super) owner: Arc<InboundOwner>,
    pub(super) group: Arc<InboundPeerTasks>,
    pub(super) tasks: InboundTaskAccess,
}
impl Live {
    pub(super) fn new() -> Self {
        let proxy = Proxy::new(
            zero_config::RuntimeConfig::parse(r#"{"route":{"final":{"type":"direct"}}}"#).unwrap(),
        )
        .unwrap();
        let (stop, mut stopping) = watch::channel(false);
        let installed = proxy
            .inbound_owners
            .install(
                "response-peer",
                &proxy.protocols,
                crate::runtime::inbound_owners::MAX_DATAGRAM_PEERS,
                stop,
            )
            .unwrap();
        let owner = installed.owner();
        let group = installed.peer_tasks();
        let tasks = installed.tasks();
        owner
            .spawn_driver(async move {
                let _ = stopping.wait_for(|stop| *stop).await;
                Ok(())
            })
            .unwrap();
        Self {
            proxy,
            owner,
            group,
            tasks,
        }
    }
    pub(super) async fn record_peer(&self, binding: &HostPeerBinding) {
        let task = binding.wait_bound().await.unwrap();
        let exit = timeout(
            Duration::from_secs(2),
            self.owner.scope.lock().await.next_exit(),
        )
        .await
        .unwrap()
        .unwrap();
        assert_eq!(exit.id, task);
        assert_eq!(exit.class, TaskClass::Critical);
        assert!(exit.result.is_ok());
        self.owner.record(exit);
        assert_eq!(binding.observed_join().unwrap().task(), task);
    }
}
pub(super) async fn pair() -> (tokio::net::TcpStream, tokio::net::TcpStream) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let (client, server) = tokio::join!(
        tokio::net::TcpStream::connect(listener.local_addr().unwrap()),
        listener.accept()
    );
    (client.unwrap(), server.unwrap().0)
}
pub(super) async fn pending(future: impl Future) {
    tokio::pin!(future);
    std::future::poll_fn(|cx| {
        assert!(future.as_mut().poll(cx).is_pending());
        Poll::Ready(())
    })
    .await;
}
pub(super) async fn read_pending(io: &mut tokio::net::TcpStream, entered: &Notify) {
    let read = io.read_u8();
    tokio::pin!(read);
    std::future::poll_fn(|cx| {
        let result = read.as_mut().poll(cx);
        if result.is_pending() {
            entered.notify_one();
        }
        result
    })
    .await
    .unwrap();
}
pub(super) fn deadline() -> Instant {
    Instant::now() + Duration::from_secs(2)
}
