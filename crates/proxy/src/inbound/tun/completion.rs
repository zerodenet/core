//! Persistent original task handles and notifications; cancelled waits retain both.
use std::{io, sync::Arc};
use tokio::{
    sync::{oneshot, Mutex},
    task::{JoinError, JoinHandle},
    time::{timeout_at, Instant},
};

type Source = Arc<dyn std::error::Error + Send + Sync>;
type Outcome = Result<(), Source>;

#[derive(Debug)]
struct Main {
    notification: oneshot::Receiver<()>,
    notified: Option<Outcome>,
    task: Option<JoinHandle<()>>,
    joined: Option<Outcome>,
}
#[derive(Debug)]
struct Routes {
    notification: Option<oneshot::Receiver<Result<(), String>>>,
    notified: Option<Outcome>,
    task: Option<JoinHandle<()>>,
    joined: Option<Outcome>,
}
#[derive(Debug)]
pub(crate) struct TunCompletion {
    main: Mutex<Main>,
    routes: Mutex<Routes>,
}
impl TunCompletion {
    pub(super) fn new(
        done: oneshot::Receiver<()>,
        route_done: Option<oneshot::Receiver<Result<(), String>>>,
    ) -> Arc<Self> {
        Arc::new(Self {
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
        })
    }
    /// Called synchronously before the start operation can suspend or return.
    pub(super) fn install_main(&self, task: JoinHandle<()>) {
        let mut main = self.main.try_lock().expect("unpublished TUN task owner");
        assert!(main.task.is_none() && main.joined.is_none());
        main.task = Some(task);
    }
    pub(super) fn install_routes(&self, task: JoinHandle<()>) {
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
            if main.notified.is_none() {
                main.notified = Some(
                    (&mut main.notification)
                        .await
                        .map_err(|e| Arc::new(e) as Source),
                );
            }
            if main.joined.is_none() {
                main.joined = Some(match main.task.as_mut() {
                    Some(task) => task.await.map_err(join_source),
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
    /// The exiting main task may observe routes, but never joins itself.
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
                    Some(task) => task.await.map_err(join_source),
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
fn combine(notification: Option<&Outcome>, joined: Option<&Outcome>) -> io::Result<()> {
    let failures = [notification, joined]
        .into_iter()
        .flatten()
        .filter_map(|r| r.as_ref().err())
        .cloned()
        .collect::<Vec<_>>();
    if failures.is_empty() {
        Ok(())
    } else {
        Err(io::Error::other(Failures(failures)))
    }
}
#[derive(Debug)]
struct Failures(Vec<Source>);
impl std::fmt::Display for Failures {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        for (index, source) in self.0.iter().enumerate() {
            if index != 0 {
                f.write_str("; ")?;
            }
            source.fmt(f)?;
        }
        Ok(())
    }
}
impl std::error::Error for Failures {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        self.0.first().map(|source| source.as_ref() as _)
    }
}

#[cfg(test)]
mod tests;
