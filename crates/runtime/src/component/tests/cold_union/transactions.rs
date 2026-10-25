//! Actual SS replay worker and its two dependent protocol roles, not a model graph.
use super::*;
use std::{future::Future, task::Poll};
use tokio::{
    io::AsyncReadExt,
    net::{TcpListener, TcpStream},
    time::{Duration, Instant},
};
use zero_core::{
    component::NetworkGeneration,
    request::{Destination, ServiceOrigin, ServicePurpose, ServiceRequestFacts},
    Address,
};
use zero_platform_tokio::contract::{
    context::{OpenContext, RequestIdentity},
    datagram::{BoxDatagramSession, DatagramRequest},
    network::{CarrierContext, UpstreamBinding, UpstreamOpener, UpstreamTransport},
    stream::{BoxStream, ConnectRequest},
    BoxFuture, CancellationToken,
};

pub(super) fn deadline() -> Instant {
    Instant::now() + Duration::from_secs(3)
}
struct Root {
    listener: TcpListener,
    peers: Mutex<Vec<TcpStream>>,
}
impl UpstreamOpener for Root {
    fn open_stream(
        &self,
        _: &UpstreamBinding,
        _: Destination,
        _: CarrierContext,
    ) -> BoxFuture<'_, Result<BoxStream, ChannelError>> {
        Box::pin(async {
            let (client, accepted) = tokio::join!(
                TcpStream::connect(self.listener.local_addr().unwrap()),
                self.listener.accept()
            );
            self.peers.lock().unwrap().push(accepted.unwrap().0);
            Ok(Box::pin(client.unwrap()) as BoxStream)
        })
    }
    fn open_datagram(
        &self,
        _: &UpstreamBinding,
        _: DatagramRequest,
        _: CarrierContext,
    ) -> BoxFuture<'_, Result<BoxDatagramSession, ChannelError>> {
        Box::pin(async { panic!("Stream fixture must not open UDP") })
    }
}
async fn open(slot: &ComponentSlot, root: &Arc<Root>) -> BoxStream {
    slot.stream(deadline())
        .await
        .unwrap()
        .connect(
            ConnectRequest {
                destination: Destination::new(Address::Domain("target.invalid".into()), 80),
            },
            OpenContext {
                identity: RequestIdentity::Service(Arc::new(ServiceRequestFacts {
                    origin: ServiceOrigin::Runtime,
                    purpose: ServicePurpose::Bootstrap,
                })),
                deadline: Some(deadline()),
                cancel: CancellationToken::new(),
                observer: None,
                upstream: UpstreamTransport::new(
                    UpstreamBinding {
                        prefix: vec![],
                        dial: slot.dial.clone(),
                        network_generation: NetworkGeneration(1),
                    },
                    root.clone(),
                ),
            },
        )
        .await
        .unwrap()
        .io
}

#[tokio::test]
async fn cancelled_shared_replacement_cannot_start_until_the_actual_transferred_user_exits() {
    let controls = Arc::new(Controls::default());
    let catalog = Arc::new(catalog(&controls));
    let original = config();
    catalog.start(original.clone(), deadline()).await.unwrap();
    catalog.commit(&original, || Ok(())).unwrap();
    let old = catalog.current(&original).unwrap();
    let shared = resource(&old);
    let unaffected = old.configured("separate").unwrap();
    let root = Arc::new(Root {
        listener: TcpListener::bind("127.0.0.1:0").await.unwrap(),
        peers: Mutex::new(vec![]),
    });
    let mut io = open(&old.configured("a").unwrap(), &root).await;
    let pending = Arc::new(tokio::sync::Notify::new());
    let revoked = Arc::new(tokio::sync::Notify::new());
    let release = Arc::new(tokio::sync::Notify::new());
    let reader = tokio::spawn({
        let pending = pending.clone();
        let revoked = revoked.clone();
        let release = release.clone();
        async move {
            let mut read = Box::pin(io.read_u8());
            let result = std::future::poll_fn(|cx| match read.as_mut().poll(cx) {
                Poll::Pending => {
                    pending.notify_one();
                    Poll::Pending
                }
                Poll::Ready(result) => Poll::Ready(result),
            })
            .await;
            drop(read);
            revoked.notify_one();
            release.notified().await;
            drop(io);
            result
        }
    });
    pending.notified().await;
    assert!(
        !reader.is_finished(),
        "the actual protocol read is pending before retirement"
    );
    controls.extra_task.store(true, Ordering::SeqCst);
    let candidate = Arc::new(original.as_ref().clone());
    let next = catalog.prepare(candidate.clone()).unwrap();
    let replacing = tokio::spawn({
        let catalog = catalog.clone();
        let candidate = candidate.clone();
        async move { catalog.start(candidate, deadline()).await }
    });
    tokio::time::timeout_at(deadline(), revoked.notified())
        .await
        .unwrap();
    assert!(!replacing.is_finished());
    assert_eq!(
        controls.builds.load(Ordering::SeqCst),
        3,
        "no candidate worker or consumer may build while its old user is held"
    );
    assert!(!old.confirmed(&std::collections::BTreeSet::from([shared.instance_id()])));
    replacing.abort();
    assert!(replacing.await.unwrap_err().is_cancelled());
    assert!(Arc::ptr_eq(
        &catalog
            .state
            .lock()
            .unwrap()
            .replacement
            .as_ref()
            .unwrap()
            .candidate,
        &next
    ));
    assert!(Arc::ptr_eq(&catalog.current(&original).unwrap(), &old));
    release.notify_one();
    assert_eq!(
        reader.await.unwrap().unwrap_err().kind(),
        std::io::ErrorKind::ConnectionAborted
    );
    catalog.start(candidate.clone(), deadline()).await.unwrap();
    {
        let retired = old.close_evidence.lock().unwrap();
        assert_eq!(
            retired[&shared.instance_id()].exited_tasks.len(),
            1,
            "the actual registered replay worker was joined"
        );
    }
    assert!(old.confirmed(&std::collections::BTreeSet::from([shared.instance_id()])));
    assert_eq!(resource(&next).generation, ResourceGeneration(2));
    for tag in ["a", "b"] {
        let slot = next.configured(tag).unwrap();
        assert_eq!(slot.generation, ResourceGeneration(2));
        assert_eq!(
            slot.dependency_instances(),
            &[resource(&next).instance_id()]
        );
    }
    assert_eq!(controls.builds.load(Ordering::SeqCst), 6);
    assert!(Arc::ptr_eq(
        &unaffected,
        &next.configured("separate").unwrap()
    ));
    catalog.commit(&candidate, || Ok(())).unwrap();
    catalog.shutdown(deadline()).await.unwrap();
}

#[tokio::test]
async fn failed_shared_candidate_is_confirmed_closed_before_last_good_is_rebuilt() {
    let controls = Arc::new(Controls::default());
    let catalog = catalog(&controls);
    let original = config();
    catalog.start(original.clone(), deadline()).await.unwrap();
    catalog.commit(&original, || Ok(())).unwrap();
    let old = catalog.current(&original).unwrap();
    let separate = old.configured("separate").unwrap();
    controls.extra_task.store(true, Ordering::SeqCst);
    controls.fail_generation.store(2, Ordering::SeqCst);
    let candidate = Arc::new(original.as_ref().clone());
    let failed = catalog.prepare(candidate.clone()).unwrap();
    let error = catalog.start(candidate, deadline()).await.unwrap_err();
    assert_eq!(error.reason, Reason::InvalidConfig);
    assert!(old.confirmed(&std::collections::BTreeSet::from([
        resource(&old).instance_id()
    ])));
    controls.fail_generation.store(0, Ordering::SeqCst);
    controls.extra_task.store(false, Ordering::SeqCst);
    catalog.start(original.clone(), deadline()).await.unwrap();
    assert!(failed.confirmed(&std::collections::BTreeSet::from([
        resource(&failed).instance_id()
    ])));
    assert_eq!(
        failed.close_evidence.lock().unwrap()[&resource(&failed).instance_id()]
            .exited_tasks
            .len(),
        1
    );
    let restored = catalog
        .state
        .lock()
        .unwrap()
        .staged
        .as_ref()
        .unwrap()
        .ready()
        .clone();
    assert_eq!(resource(&restored).generation, ResourceGeneration(3));
    assert_eq!(
        restored.configured("a").unwrap().generation,
        ResourceGeneration(3)
    );
    assert!(Arc::ptr_eq(
        &separate,
        &restored.configured("separate").unwrap()
    ));
    assert!(
        Arc::ptr_eq(&old, &catalog.current(&original).unwrap()),
        "failed apply has not committed a candidate"
    );
    catalog.commit(&original, || Ok(())).unwrap();
    catalog.shutdown(deadline()).await.unwrap();
}

#[tokio::test]
async fn shared_user_timeout_retains_the_owner_and_never_admits_a_second_worker() {
    let controls = Arc::new(Controls::default());
    let catalog = catalog(&controls);
    let original = config();
    catalog.start(original.clone(), deadline()).await.unwrap();
    catalog.commit(&original, || Ok(())).unwrap();
    let old = catalog.current(&original).unwrap();
    let root = Arc::new(Root {
        listener: TcpListener::bind("127.0.0.1:0").await.unwrap(),
        peers: Mutex::new(vec![]),
    });
    let io = open(&old.configured("a").unwrap(), &root).await;
    controls.extra_task.store(true, Ordering::SeqCst);
    let candidate = Arc::new(original.as_ref().clone());
    let next = catalog.prepare(candidate.clone()).unwrap();
    for _ in 0..2 {
        let error = catalog
            .start(
                candidate.clone(),
                Instant::now() + Duration::from_millis(30),
            )
            .await
            .unwrap_err();
        assert_eq!(error.reason, Reason::Timeout);
        assert_eq!(controls.builds.load(Ordering::SeqCst), 3);
        assert!(!next.started.load(Ordering::Acquire));
        assert!(!old.confirmed(&std::collections::BTreeSet::from([
            resource(&old).instance_id()
        ])));
    }
    drop(io); // Only ends this user's custody; native/task proof remains separate.
    catalog.start(candidate.clone(), deadline()).await.unwrap();
    assert_eq!(
        old.close_evidence.lock().unwrap()[&resource(&old).instance_id()]
            .exited_tasks
            .len(),
        1
    );
    catalog.commit(&candidate, || Ok(())).unwrap();
    catalog.shutdown(deadline()).await.unwrap();
}

#[path = "transactions/historical.rs"]
mod historical;
