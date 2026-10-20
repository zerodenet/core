//! Actual H2 registration: the stored waker never owns its response future.
use super::*;
use std::{
    sync::atomic::{AtomicUsize, Ordering},
    task::Poll,
};
use tokio::time::{timeout, Duration};
#[derive(Default)]
struct Wakes(AtomicUsize, tokio::sync::Notify);
impl ArcWake for Wakes {
    fn wake_by_ref(this: &Arc<Self>) {
        this.0.fetch_add(1, Ordering::SeqCst);
        this.1.notify_one();
    }
}
#[tokio::test]
async fn actual_pending_h2_response_waker_does_not_retain_head_and_still_wakes_the_reader() {
    timeout(Duration::from_secs(4),async {
        let listener=tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();let address=listener.local_addr().unwrap();
        let (registered,registration)=tokio::sync::oneshot::channel();
        let (release,released)=tokio::sync::oneshot::channel();
        let (finish,finished)=tokio::sync::oneshot::channel();
        let server=tokio::spawn(async move {
            let (io,_)=listener.accept().await.unwrap();let mut connection=h2::server::handshake(io).await.unwrap();
            let (_request,mut respond)=connection.accept().await.unwrap().unwrap();registered.send(()).unwrap();
            // Preserve actual peer I/O while holding the response deliberately.
            let gate=async {released.await.unwrap();};tokio::pin!(gate);
            tokio::select! {_=&mut gate=>{},result=connection.accept()=>panic!("first response is still pending: {}",result.is_some())}
            respond.send_response(Response::builder().status(200).body(()).unwrap(),true).unwrap();
            let finish=async {finished.await.unwrap();};tokio::pin!(finish);
            tokio::select! {_=&mut finish=>{},_=connection.accept()=>{finish.await;}}
        });
        let io=tokio::net::TcpStream::connect(address).await.unwrap();
        let (sender,connection)=h2::client::handshake(io).await.unwrap();
        // Fixture-owned driver; its actual handle is joined below.
        let driver=tokio::spawn(async move {connection.await});
        let mut sender=sender.ready().await.unwrap();
        let (response,send)=sender.send_request(Request::builder().uri("https://localhost/test").body(()).unwrap(),true).unwrap();
        let head=Head::new(response);registration.await.unwrap();
        let wakes=Arc::new(Wakes::default());let waker=waker_ref(&wakes);
        assert!(head.poll(&mut Context::from_waker(&waker)).is_pending());
        let isolated = Arc::strong_count(&head.0) == 1;
        let weak=Arc::downgrade(&head.0);
        release.send(()).unwrap();
        // Keep the explicitly registered reader in place until real H2 wakes it.
        wakes.1.notified().await;
        let response=futures_util::future::poll_fn(|cx|head.poll(cx)).await.unwrap();
        assert_eq!(response.status(),http::StatusCode::OK);
        let notified = wakes.0.load(Ordering::SeqCst)>0;
        drop(response);drop(head);
        let reclaimed = weak.upgrade().is_none();
        drop(send);drop(sender);finish.send(()).unwrap();
        server.await.unwrap();driver.await.unwrap().unwrap();
        // Assertions happen after safe cleanup, even on the old owner cycle.
        assert!(isolated,"the real H2 stored waker must own only WakeRelay");
        assert!(notified,"the original registered reader was really notified");
        assert!(reclaimed);
    }).await.unwrap();
}
#[tokio::test]
async fn dropping_head_after_real_pending_response_registration_reclaims_it_before_connection_cleanup(
) {
    timeout(Duration::from_secs(4), async {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let (accepted, request_accepted) = tokio::sync::oneshot::channel();
        let (release, released) = tokio::sync::oneshot::channel();
        let (finish, finished) = tokio::sync::oneshot::channel();
        let server = tokio::spawn(async move {
            let (io, _) = listener.accept().await.unwrap();
            let mut connection = h2::server::handshake(io).await.unwrap();
            let (_request, mut respond) = connection.accept().await.unwrap().unwrap();
            accepted.send(()).unwrap();
            let gate = async {
                released.await.unwrap();
            };
            tokio::pin!(gate);
            let live = tokio::select! {_=&mut gate=>true,_=connection.accept()=>{gate.await;false}};
            // A correctly dropped response may already be reset. This response
            // exists only to break the old strong-owner cycle before asserting.
            let _ = respond.send_response(Response::builder().status(200).body(()).unwrap(), true);
            let finish = async {
                finished.await.unwrap();
            };
            tokio::pin!(finish);
            if live {
                tokio::select! {_=&mut finish=>{},_=connection.accept()=>{finish.await;}}
            } else {
                finish.await;
            }
        });
        let io = tokio::net::TcpStream::connect(address).await.unwrap();
        let (sender, connection) = h2::client::handshake(io).await.unwrap();
        let driver = tokio::spawn(async move { connection.await });
        let mut sender = sender.ready().await.unwrap();
        let (response, send) = sender
            .send_request(
                Request::builder()
                    .uri("https://localhost/pending")
                    .body(())
                    .unwrap(),
                true,
            )
            .unwrap();
        request_accepted.await.unwrap();
        let head = Head::new(response);
        let weak = Arc::downgrade(&head.0);
        futures_util::future::poll_fn(|cx| {
            assert!(head.poll(cx).is_pending());
            Poll::Ready(())
        })
        .await;
        let isolated = Arc::strong_count(&head.0) == 1;
        drop(head);
        let rescue = weak.upgrade();
        let reclaimed = rescue.is_none();
        // Acquire the fallback owner before allowing the remote notification,
        // including if this fixture is later run on a multithreaded runtime.
        release.send(()).unwrap();
        if let Some(shared) = rescue {
            // On the regressed implementation, consume the original future
            // with real H2 I/O before cleanup; an assertion must not deadlock
            // while unwinding the same pending future/waker ownership cycle.
            let rescue = Head(shared);
            let response = futures_util::future::poll_fn(|cx| rescue.poll(cx))
                .await
                .unwrap();
            drop(response);
            drop(rescue);
        }
        drop(send);
        drop(sender);
        finish.send(()).unwrap();
        server.await.unwrap();
        driver.await.unwrap().unwrap();
        assert!(
            isolated,
            "a registered H2 waker must not increase Shared ownership"
        );
        assert!(
            reclaimed,
            "pending Head must be reclaimed before connection cleanup"
        );
    })
    .await
    .unwrap();
}
