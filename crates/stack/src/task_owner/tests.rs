use super::*;
use std::{
    sync::{
        atomic::{AtomicUsize, Ordering},
        Mutex,
    },
    task::Poll,
    time::Duration,
};
use tokio::{
    sync::{mpsc, Notify},
    task::JoinSet,
};
use zero_traits::TcpStack;

#[derive(Default)]
struct ActualOwner {
    tasks: Mutex<JoinSet<()>>,
    pending: Arc<AtomicUsize>,
    changed: Arc<Notify>,
}
impl TcpTaskOwner for ActualOwner {
    fn spawn(&self, mut worker: Pin<Box<dyn Future<Output = ()> + Send + 'static>>) -> bool {
        let pending = self.pending.clone();
        let changed = self.changed.clone();
        self.tasks.lock().unwrap().spawn(async move {
            let mut witnessed = false;
            std::future::poll_fn(|cx| match worker.as_mut().poll(cx) {
                Poll::Pending => {
                    if !witnessed {
                        witnessed = true;
                        pending.fetch_add(1, Ordering::SeqCst);
                        changed.notify_one();
                    }
                    Poll::Pending
                }
                Poll::Ready(()) => Poll::Ready(()),
            })
            .await;
        });
        true
    }
}
fn syn() -> Vec<u8> {
    crate::packet::build_tcp(
        "10.0.0.2".parse().unwrap(),
        "10.0.0.1".parse().unwrap(),
        40000,
        443,
        1000,
        0,
        crate::packet::tcp_flags::SYN,
        &[],
    )
}

#[tokio::test]
async fn owned_stack_registers_original_pending_control_and_retransmission_workers() {
    let owner = Arc::new(ActualOwner::default());
    let (outbound, mut packets) = mpsc::channel::<Vec<u8>>(8);
    let stack = crate::UserNetworkStack::new_with_packet_output_and_task_owner(
        outbound.into(),
        1360,
        owner.clone(),
    );
    let (tcp, _) = stack.into_parts();
    assert_eq!(owner.tasks.lock().unwrap().len(), 0); // Cold constructor.
    tcp.feed(&syn()).await;
    assert!(packets.try_recv().is_ok()); // The original state machine emitted SYN-ACK.
    tokio::time::timeout(Duration::from_secs(2), async {
        while owner.pending.load(Ordering::SeqCst) != 2 {
            owner.changed.notified().await;
        }
    })
    .await
    .unwrap();
    let mut tasks = std::mem::take(&mut *owner.tasks.lock().unwrap());
    assert_eq!(tasks.len(), 2);
    tasks.abort_all();
    let mut actual_joins = 0;
    while let Some(joined) = tasks.join_next().await {
        assert!(joined.unwrap_err().is_cancelled());
        actual_joins += 1;
    }
    assert_eq!(actual_joins, 2);
}
struct Denied;
impl TcpTaskOwner for Denied {
    fn spawn(&self, _: Pin<Box<dyn Future<Output = ()> + Send + 'static>>) -> bool {
        false
    }
}
#[tokio::test]
async fn denied_owned_worker_never_uses_the_legacy_executor_or_emits_syn_ack() {
    let (outbound, mut packets) = mpsc::channel::<Vec<u8>>(8);
    let stack = crate::UserNetworkStack::new_with_packet_output_and_task_owner(
        outbound.into(),
        1360,
        Arc::new(Denied),
    );
    let (tcp, _) = stack.into_parts();
    tcp.feed(&syn()).await;
    assert!(packets.try_recv().is_err());
}
