use super::*;
use crate::component::instance::Owner;
use std::sync::atomic::{AtomicUsize, Ordering};
use tokio::{net::TcpListener, sync::Notify};
use zero_platform_tokio::contract::{
    component::{ComponentInstance, ComponentRoles},
    lifecycle::{erase_lifecycle, BuildError, ResourceLifecycle},
    scope::{TaskClass, TaskScope},
    CloseReason,
};
struct Protocol {
    listener: Option<TcpListener>,
    entered: Arc<Notify>,
    release: Arc<Notify>,
    calls: Arc<AtomicUsize>,
}
impl ResourceLifecycle for Protocol {
    async fn start(&mut self, _: Instant) -> Result<(), BuildError> {
        Ok(())
    }
    fn revoke(&self, _: CloseReason) {}
    async fn shutdown(&mut self, _: Instant) -> Result<CloseReport, BuildError> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        if self.listener.is_some() {
            self.entered.notify_one();
            self.release.notified().await;
        }
        let mut report = CloseReport::default();
        if let Some(listener) = self.listener.take() {
            let address = listener.local_addr().unwrap();
            drop(listener);
            let proof = std::net::TcpListener::bind(address).unwrap();
            drop(proof);
            report.closed_io.push(address.to_string());
        }
        Ok(report)
    }
}
#[tokio::test]
async fn interrupted_protocol_cleanup_retains_actual_file_join_and_scope_receipts() {
    let directory = Directory::new();
    let path = directory.0.join("actual");
    std::fs::write(&path, b"actual").unwrap();
    let grants = grant(path.clone(), 6);
    let gate = Gate::new();
    let _release = gate.release_guard();
    grants.state.lock().unwrap().files.before_read = Some(gate.hook());
    let _access = grants.activate(deadline()).unwrap();
    let reading = grants.clone();
    let read = tokio::spawn(async move { reading.read_file(path, 6).await });
    gate.entered().await;
    let scope = TaskScope::new(
        tokio::runtime::Handle::current(),
        std::num::NonZeroUsize::new(1).unwrap(),
    );
    let registrar = scope.registrar();
    let cancel = registrar.cancellation();
    let task = registrar
        .spawn(TaskClass::Critical, async move {
            cancel.cancelled().await;
            Ok(())
        })
        .unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let entered = Arc::new(Notify::new());
    let release = Arc::new(Notify::new());
    let owner = Arc::new(tokio::sync::Mutex::new(Owner {
        instance: ComponentInstance {
            lifecycle: erase_lifecycle(Protocol {
                listener: Some(listener),
                entered: entered.clone(),
                release: release.clone(),
                calls: Arc::new(AtomicUsize::new(0)),
            }),
            roles: ComponentRoles::default(),
        },
        tasks: scope,
        pending_close: None,
        protocol_closed: false,
    }));
    let closing = owner.clone();
    let resources = grants.clone();
    let attempt = tokio::spawn(async move {
        closing
            .lock()
            .await
            .shutdown(ComponentId(201), deadline(), &resources)
            .await
    });
    tokio::time::timeout_at(deadline(), entered.notified())
        .await
        .unwrap();
    gate.release();
    tokio::time::timeout_at(deadline(), async {
        loop {
            if grants.state.lock().unwrap().files.records.is_empty() {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    assert!(
        !attempt.is_finished(),
        "the real protocol close remains suspended"
    );
    attempt.abort();
    assert!(attempt.await.unwrap_err().is_cancelled());
    assert!(
        std::net::TcpListener::bind(address).is_err(),
        "the cancelled close did not release its actual listener"
    );
    assert_eq!(
        owner
            .lock()
            .await
            .pending_close
            .as_ref()
            .unwrap()
            .released_resources
            .len(),
        1,
        "actual file join receipt is retained before another branch can suspend"
    );
    release.notify_one();
    let report = owner
        .lock()
        .await
        .shutdown(ComponentId(201), deadline(), &grants)
        .await
        .unwrap();
    assert!(report.is_complete());
    assert_eq!(report.released_resources.len(), 1);
    assert_eq!(report.exited_tasks, vec![task]);
    assert_eq!(report.closed_io, vec![address.to_string()]);
    assert!(std::net::TcpListener::bind(address).is_ok());
    assert!(
        read.await.unwrap().is_err(),
        "shutdown revoked the file caller without losing its actual job"
    );
}

#[tokio::test]
async fn a_confirmed_protocol_close_is_not_repeated_when_file_join_was_interrupted() {
    let directory = Directory::new();
    let path = directory.0.join("late-file");
    std::fs::write(&path, b"late").unwrap();
    let grants = grant(path.clone(), 4);
    let gate = Gate::new();
    let _release = gate.release_guard();
    grants.state.lock().unwrap().files.before_read = Some(gate.hook());
    let _access = grants.activate(deadline()).unwrap();
    let reading = grants.clone();
    let read = tokio::spawn(async move { reading.read_file(path, 4).await });
    gate.entered().await;
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let entered = Arc::new(Notify::new());
    let release = Arc::new(Notify::new());
    release.notify_one();
    let calls = Arc::new(AtomicUsize::new(0));
    let owner = Arc::new(tokio::sync::Mutex::new(Owner {
        instance: ComponentInstance {
            lifecycle: erase_lifecycle(Protocol {
                listener: Some(listener),
                entered: entered.clone(),
                release,
                calls: calls.clone(),
            }),
            roles: ComponentRoles::default(),
        },
        tasks: TaskScope::empty(tokio::runtime::Handle::current()),
        pending_close: None,
        protocol_closed: false,
    }));
    let closing = owner.clone();
    let resources = grants.clone();
    let attempt = tokio::spawn(async move {
        closing
            .lock()
            .await
            .shutdown(ComponentId(201), deadline(), &resources)
            .await
    });
    tokio::time::timeout_at(deadline(), entered.notified())
        .await
        .unwrap();
    let proof = std::net::TcpListener::bind(address).unwrap();
    drop(proof);
    assert!(
        !attempt.is_finished(),
        "the actual blocking file task still needs a join"
    );
    attempt.abort();
    assert!(attempt.await.unwrap_err().is_cancelled());
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    {
        let owner = owner.lock().await;
        assert!(owner.protocol_closed);
        assert_eq!(
            owner.pending_close.as_ref().unwrap().closed_io,
            vec![address.to_string()]
        );
    }
    assert_eq!(grants.state.lock().unwrap().files.records.len(), 1);
    gate.release();
    let report = owner
        .lock()
        .await
        .shutdown(ComponentId(201), deadline(), &grants)
        .await
        .unwrap();
    assert!(report.is_complete());
    assert_eq!(
        calls.load(Ordering::SeqCst),
        1,
        "only the unconfirmed branch is retried"
    );
    assert_eq!(report.closed_io, vec![address.to_string()]);
    assert_eq!(report.released_resources.len(), 1);
    assert!(read.await.unwrap().is_err());
}
