use super::*;
use std::{
    future::Future,
    pin::Pin,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc, Mutex,
    },
    task::Poll,
};
use tokio::{sync::Notify, task::JoinSet};

struct Owner {
    calls: AtomicUsize,
    accept: usize,
    tasks: Mutex<JoinSet<()>>,
    pending: Arc<AtomicUsize>,
    changed: Arc<Notify>,
}
impl Owner {
    fn new(accept: usize) -> Arc<Self> {
        Arc::new(Self {
            calls: AtomicUsize::new(0),
            accept,
            tasks: Mutex::new(JoinSet::new()),
            pending: Arc::new(AtomicUsize::new(0)),
            changed: Arc::new(Notify::new()),
        })
    }
    async fn abort_and_join(&self, expected: usize) {
        let mut tasks = std::mem::take(&mut *self.tasks.lock().unwrap());
        assert_eq!(tasks.len(), expected);
        tasks.abort_all();
        let mut joined = 0;
        while let Some(result) = tasks.join_next().await {
            assert!(result.unwrap_err().is_cancelled());
            joined += 1;
        }
        assert_eq!(joined, expected);
    }
}
impl crate::TcpTaskOwner for Owner {
    fn spawn(&self, mut worker: Pin<Box<dyn Future<Output = ()> + Send + 'static>>) -> bool {
        if self.calls.fetch_add(1, Ordering::SeqCst) >= self.accept {
            return false;
        }
        let pending = self.pending.clone();
        let changed = self.changed.clone();
        self.tasks.lock().unwrap().spawn(async move {
            let mut observed = false;
            std::future::poll_fn(|cx| match worker.as_mut().poll(cx) {
                Poll::Pending => {
                    if !observed {
                        observed = true;
                        pending.fetch_add(1, Ordering::SeqCst);
                        changed.notify_one();
                    }
                    Poll::Pending
                }
                ready => ready,
            })
            .await;
        });
        true
    }
}
fn client(
    owner: Arc<Owner>,
) -> (
    ClientTcpStack,
    mpsc::Receiver<crate::packet_output::ObservedPacket>,
) {
    let (sender, receiver) = mpsc::channel(8);
    let client = ClientTcpStack::new_observed_with_task_owner(
        vec!["10.0.0.2".parse().unwrap()],
        sender,
        1420,
        owner,
    )
    .unwrap();
    (client, receiver)
}

#[tokio::test]
async fn cold_client_registers_original_pending_workers_only_on_actual_connect() {
    let owner = Owner::new(2);
    let (client, mut packets) = client(owner.clone());
    assert_eq!(owner.calls.load(Ordering::SeqCst), 0);
    assert_eq!(owner.tasks.lock().unwrap().len(), 0);
    let connecting = client.connect("10.0.0.2".parse().unwrap(), "10.0.0.1:443".parse().unwrap());
    tokio::pin!(connecting);
    let syn = tokio::select! {
        result = &mut connecting => panic!("connection completed without a peer: {}", result.is_ok()),
        packet = packets.recv() => packet.unwrap(),
    };
    assert!(packet::parse_tcp(&syn.packet).unwrap().syn);
    timeout(Duration::from_secs(2), async {
        while owner.pending.load(Ordering::SeqCst) != 2 {
            owner.changed.notified().await;
        }
    })
    .await
    .unwrap();
    assert_eq!(owner.calls.load(Ordering::SeqCst), 2);
    owner.abort_and_join(2).await;
}

#[tokio::test]
async fn rejected_client_control_worker_emits_no_syn_and_has_no_connection_record() {
    let owner = Owner::new(0);
    let (client, mut packets) = client(owner.clone());
    assert!(matches!(
        client
            .connect("10.0.0.2".parse().unwrap(), "10.0.0.1:443".parse().unwrap())
            .await,
        Err(ClientTcpStackError::OutboundClosed)
    ));
    assert_eq!(owner.calls.load(Ordering::SeqCst), 1);
    assert!(client.connections.lock().await.is_empty());
    assert!(packets.try_recv().is_err());
    owner.abort_and_join(0).await;
}

#[tokio::test]
async fn rejected_client_retransmission_worker_removes_half_open_before_syn_submission() {
    let owner = Owner::new(1);
    let (client, mut packets) = client(owner.clone());
    assert!(matches!(
        client
            .connect("10.0.0.2".parse().unwrap(), "10.0.0.1:443".parse().unwrap())
            .await,
        Err(ClientTcpStackError::OutboundClosed)
    ));
    assert_eq!(owner.calls.load(Ordering::SeqCst), 2);
    assert!(client.connections.lock().await.is_empty());
    assert!(packets.try_recv().is_err());
    owner.abort_and_join(1).await;
}

#[tokio::test]
async fn shutdown_fails_the_actual_half_open_waiter_without_registering_replacement_workers() {
    let owner = Owner::new(2);
    let (client, mut packets) = client(owner.clone());
    let mut connecting =
        Box::pin(client.connect("10.0.0.2".parse().unwrap(), "10.0.0.1:443".parse().unwrap()));
    std::future::poll_fn(|cx| {
        assert!(connecting.as_mut().poll(cx).is_pending());
        Poll::Ready(())
    })
    .await;
    assert!(
        packet::parse_tcp(&packets.recv().await.unwrap().packet)
            .unwrap()
            .syn
    );
    client.revoke();
    assert!(matches!(
        client
            .connect("10.0.0.2".parse().unwrap(), "10.0.0.1:443".parse().unwrap())
            .await,
        Err(ClientTcpStackError::OutboundClosed)
    ));
    client.shutdown().await;
    assert!(matches!(
        connecting.await,
        Err(ClientTcpStackError::OutboundClosed)
    ));
    assert!(client.connections.lock().await.is_empty());
    assert_eq!(owner.calls.load(Ordering::SeqCst), 2);
    let mut tasks = std::mem::take(&mut *owner.tasks.lock().unwrap());
    assert_eq!(tasks.len(), 2);
    tasks.abort_all();
    while let Some(result) = tasks.join_next().await {
        if let Err(error) = result {
            assert!(error.is_cancelled());
        }
    }
}

#[tokio::test]
async fn shutdown_wakes_original_established_stream_read_and_write_as_transport_closed() {
    let owner = Owner::new(2);
    let (client, mut packets) = client(owner.clone());
    let local = "10.0.0.2".parse().unwrap();
    let remote = "10.0.0.1:443".parse::<SocketAddr>().unwrap();
    let mut connecting = Box::pin(client.connect(local, remote));
    std::future::poll_fn(|cx| {
        assert!(connecting.as_mut().poll(cx).is_pending());
        Poll::Ready(())
    })
    .await;
    let syn = packets.recv().await.unwrap();
    let syn = packet::parse_tcp(&syn.packet).unwrap();
    let syn_ack = packet::build_tcp_with_window(
        remote.ip(),
        local,
        remote.port(),
        syn.src.port,
        100,
        syn.seq.wrapping_add(1),
        packet::tcp_flags::SYN | packet::tcp_flags::ACK,
        65535,
        &[],
    );
    client.feed(&syn_ack).await;
    let mut stream = connecting.await.unwrap();
    let mut buffer = [0; 1];
    let mut reading = Box::pin(stream.read(&mut buffer));
    std::future::poll_fn(|cx| {
        assert!(reading.as_mut().poll(cx).is_pending());
        Poll::Ready(())
    })
    .await;
    client.shutdown().await;
    assert_eq!(
        reading.await.unwrap_err().kind(),
        std::io::ErrorKind::BrokenPipe
    );
    assert_eq!(
        stream.write(b"retired").await.unwrap_err().kind(),
        std::io::ErrorKind::BrokenPipe
    );
    let mut tasks = std::mem::take(&mut *owner.tasks.lock().unwrap());
    assert_eq!(tasks.len(), 2);
    tasks.abort_all();
    while let Some(result) = tasks.join_next().await {
        if let Err(error) = result {
            assert!(error.is_cancelled());
        }
    }
}
