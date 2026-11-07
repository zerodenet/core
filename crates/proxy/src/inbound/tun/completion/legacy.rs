//! Historical notification/join fixture. Production TUN uses the actual Source owner.
//! This fixture acquires no native device; its tasks are the test's real handles.
use super::*;
use tokio::{
    sync::oneshot,
    task::{JoinError, JoinHandle},
};

type Outcome = Result<(), Source>;
#[derive(Debug)]
pub(super) struct Main {
    notification: Option<oneshot::Receiver<()>>,
    pub(super) notified: Option<Outcome>,
    pub(super) task: Option<JoinHandle<Result<(), zero_engine::EngineError>>>,
    pub(super) joined: Option<Outcome>,
}
#[derive(Debug)]
struct Routes {
    notification: Option<oneshot::Receiver<Result<(), String>>>,
    notified: Option<Outcome>,
    task: Option<JoinHandle<io::Result<()>>>,
    joined: Option<Outcome>,
}
#[derive(Debug)]
pub(super) struct LegacyCompletion {
    pub(super) main: Mutex<Main>,
    routes: Mutex<Routes>,
}
impl LegacyCompletion {
    pub(super) fn new(
        done: Option<oneshot::Receiver<()>>,
        route_done: Option<oneshot::Receiver<Result<(), String>>>,
    ) -> Self {
        Self {
            main: Mutex::new(Main {
                notification: done,
                notified: None,
                task: None,
                joined: None,
            }),
            routes: Mutex::new(Routes {
                notification: route_done,
                notified: None,
                task: None,
                joined: None,
            }),
        }
    }
    pub(super) async fn resources_complete(&self) -> bool {
        if self.main.lock().await.task.is_some() {
            return false;
        }
        let routes = self.routes.lock().await;
        routes.task.is_none()
            && (routes.notification.is_none()
                || (matches!(routes.notified, Some(Ok(())))
                    && matches!(routes.joined, Some(Ok(())))))
    }
    pub(super) async fn wait_native(&self, _deadline: Instant) -> io::Result<()> {
        if self.main.lock().await.task.is_some() {
            Err(io::Error::new(
                io::ErrorKind::WouldBlock,
                "TUN fixture main original task has not joined",
            ))
        } else {
            Ok(())
        }
    }
    pub(super) fn install_main(&self, task: JoinHandle<Result<(), zero_engine::EngineError>>) {
        let mut main = self.main.try_lock().expect("unpublished TUN task owner");
        assert!(main.task.is_none() && main.joined.is_none());
        main.task = Some(task);
    }
    pub(super) fn install_routes(&self, task: JoinHandle<io::Result<()>>) {
        let mut routes = self
            .routes
            .try_lock()
            .expect("unpublished route task owner");
        assert!(routes.task.is_none() && routes.joined.is_none());
        routes.task = Some(task);
    }
    pub(super) async fn wait_main(&self, deadline: Instant) -> io::Result<()> {
        timeout_at(deadline, async {
            let mut main = self.main.lock().await;
            // A test may install only its route handle, without a main task.
            if main.task.is_none() && main.joined.is_none() {
                return Ok(());
            }
            if main.notified.is_none() {
                main.notified = Some(
                    (main
                        .notification
                        .as_mut()
                        .expect("original legacy notification"))
                    .await
                    .map_err(|e| Arc::new(e) as Source),
                );
            }
            if main.joined.is_none() {
                main.joined = Some(match main.task.as_mut() {
                    Some(task) => match task.await {
                        Ok(result) => result.map_err(|source| Arc::new(source) as Source),
                        Err(source) => Err(join_source(source)),
                    },
                    None => Err(Arc::new(io::Error::other(
                        "TUN main original task was not installed",
                    )) as Source),
                });
                main.task = None;
            }
            combine(main.notified.as_ref(), main.joined.as_ref())
        })
        .await
        .map_err(|source| io::Error::new(io::ErrorKind::TimedOut, source))?
    }
    pub(super) async fn wait_routes(&self, deadline: Instant) -> io::Result<()> {
        timeout_at(deadline, async {
            let mut routes = self.routes.lock().await;
            if routes.notified.is_none() {
                routes.notified = Some(match routes.notification.as_mut() {
                    None => Ok(()),
                    Some(done) => match done.await {
                        Ok(Ok(())) => Ok(()),
                        Ok(Err(error)) => Err(Arc::new(io::Error::other(error)) as Source),
                        Err(error) => Err(Arc::new(error) as Source),
                    },
                });
            }
            if routes.joined.is_none() {
                let has_notification = routes.notification.is_some();
                routes.joined = Some(match routes.task.as_mut() {
                    None if !has_notification => Ok(()),
                    None => Err(Arc::new(io::Error::other(
                        "TUN route original task was not installed",
                    )) as Source),
                    Some(task) => match task.await {
                        Ok(result) => result.map_err(|source| Arc::new(source) as Source),
                        Err(source) => Err(join_source(source)),
                    },
                });
                routes.task = None;
            }
            combine(routes.notified.as_ref(), routes.joined.as_ref())
        })
        .await
        .map_err(|source| io::Error::new(io::ErrorKind::TimedOut, source))?
    }
}
fn join_source(error: JoinError) -> Source {
    Arc::new(error)
}
