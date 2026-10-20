use super::*;
use quinn::Runtime;

#[tokio::test]
async fn runtime_registration_failure_is_retained_and_wakes_the_operation() {
    let owner = EndpointOwner::new();
    owner
        .shutdown(Instant::now() + std::time::Duration::from_secs(1))
        .await
        .unwrap();
    let runtime = runtime::OwnedRuntime {
        owner: Arc::downgrade(&owner),
        status: owner.status.clone(),
    };
    struct Dropped(Option<tokio::sync::oneshot::Sender<()>>);
    impl Drop for Dropped {
        fn drop(&mut self) {
            let _ = self.0.take().unwrap().send(());
        }
    }
    let (sent, dropped) = tokio::sync::oneshot::channel();
    let guard = Dropped(Some(sent));
    runtime.spawn(Box::pin(async move {
        let _guard = guard;
        std::future::pending::<()>().await;
    }));
    dropped
        .await
        .expect("rejected opaque driver future was dropped");
    assert!(owner.status.cancel.is_cancelled());
    assert_eq!(
        owner.status.check().unwrap_err().kind(),
        io::ErrorKind::NotConnected
    );
    assert_eq!(owner.task_count(), 0);
    assert_eq!(
        owner
            .shutdown(Instant::now() + std::time::Duration::from_secs(1))
            .await
            .unwrap_err()
            .kind(),
        io::ErrorKind::NotConnected
    );
}

#[test]
fn socket_error_retains_original_native_source() {
    let held = std::net::UdpSocket::bind("127.0.0.1:0").unwrap();
    let source = std::net::UdpSocket::bind(held.local_addr().unwrap()).unwrap_err();
    let native_code = source.raw_os_error().expect("native bind error");
    let status = Status::default();
    let _ = status.io_error(source);
    let retained = status.check().unwrap_err();
    let mut error: &(dyn std::error::Error + 'static) = &retained;
    loop {
        if error
            .downcast_ref::<io::Error>()
            .and_then(io::Error::raw_os_error)
            == Some(native_code)
        {
            break;
        }
        error = error
            .source()
            .expect("original native socket source must be reachable");
    }
    assert!(status.cancel.is_cancelled());
}

#[test]
fn cold_owner_does_not_require_a_runtime() {
    let owner = EndpointOwner::new();
    assert_eq!(owner.task_count(), 0);
    assert!(owner.drained());
}
