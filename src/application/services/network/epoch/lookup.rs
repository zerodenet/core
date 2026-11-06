//! One real blocking lookup on this Epoch's existing Scope and retained ledger.
use super::*;
use crate::application::services::network::unique_candidates;
use std::net::ToSocketAddrs;
use zero_platform_tokio::contract::{ChannelError, Impact, Reason, Stage};

pub(super) struct Lookup {
    pub(super) witness: std::sync::OnceLock<TaskJoinWitness>,
    result: Mutex<Option<io::Result<Vec<SocketAddr>>>>,
    pub(super) caller_live: AtomicBool,
    changed: tokio::sync::Notify,
}
struct Caller(Arc<Lookup>, std::sync::Weak<Epoch>);
impl Drop for Caller {
    fn drop(&mut self) {
        self.0.caller_live.store(false, Ordering::Release);
        self.0.changed.notify_waiters();
        if let Some(owner) = self.1.upgrade() {
            owner.reap_lookups();
        }
    }
}
impl Lookup {
    pub(super) fn settled(&self) -> bool {
        !self.caller_live.load(Ordering::Acquire)
            && self.witness.get().is_some_and(|witness| {
                witness.try_joined().is_some_and(|joined| {
                    witness.matches_joined(&joined) && joined.registration_released()
                })
            })
    }
}
impl Epoch {
    pub(in super::super) async fn resolve(
        self: &Arc<Self>,
        host: String,
        port: u16,
    ) -> io::Result<Vec<SocketAddr>> {
        self.lookup(move || {
            (host.as_str(), port)
                .to_socket_addrs()
                .map(unique_candidates)
        })
        .await
    }
    pub(super) async fn lookup(
        self: &Arc<Self>,
        work: impl FnOnce() -> io::Result<Vec<SocketAddr>> + Send + 'static,
    ) -> io::Result<Vec<SocketAddr>> {
        self.reap_lookups();
        // Scope callbacks can reenter application owners. Their actual relay
        // is released only after both the admission and lookup-ledger locks.
        let (caller, start) = {
            let callbacks = self.registrar.defer_callbacks();
            let prepared = {
                let _admission = self.admission.lock().unwrap();
                if self.closing.load(Ordering::Acquire)
                    || self.failed.load(Ordering::Acquire)
                    || !self.faults.lock().unwrap().is_empty()
                {
                    return Err(io::Error::other("event generation closing"));
                }
                if self.lookups.lock().unwrap().len() >= self.limits.max_lookups().get() {
                    return Err(io::Error::new(
                        io::ErrorKind::WouldBlock,
                        "event original lookup capacity exhausted",
                    ));
                }
                let job = Arc::new(Lookup {
                    witness: std::sync::OnceLock::new(),
                    result: Mutex::new(None),
                    caller_live: AtomicBool::new(true),
                    changed: tokio::sync::Notify::new(),
                });
                let caller = Caller(job.clone(), Arc::downgrade(self));
                self.lookups.lock().unwrap().push(job.clone());
                let (start, gate) = std::sync::mpsc::sync_channel(1);
                let actual = job.clone();
                let registered =
                    self.registrar
                        .spawn_blocking_tracked(TaskClass::Auxiliary, move || {
                            let result = match gate.recv() {
                                Ok(()) => work(),
                                Err(_) => Err(io::Error::new(
                                    io::ErrorKind::Interrupted,
                                    "original lookup start was stopped",
                                )),
                            };
                            *actual.result.lock().unwrap() = Some(result);
                            actual.changed.notify_waiters();
                            Ok(())
                        });
                let (_, witness) = match registered {
                    Ok(registered) => registered,
                    Err(source) => {
                        self.lookups
                            .lock()
                            .unwrap()
                            .retain(|actual| !Arc::ptr_eq(actual, &job));
                        return Err(io::Error::other(source));
                    }
                };
                assert!(
                    job.witness.set(witness).is_ok(),
                    "one original lookup registration"
                );
                (caller, start)
            };
            drop(callbacks);
            prepared
        };
        // The sole real worker cannot invoke libc/resolver I/O before both its
        // exact witness and original result/custody record are installed.
        start
            .send(())
            .map_err(|_| io::Error::other("original lookup start stopped"))?;
        let witness = caller
            .0
            .witness
            .get()
            .expect("installed original lookup witness");
        let joined = witness.wait().await;
        if let Err(source) = joined.result() {
            self.failed.store(true, Ordering::Release);
            return Err(io::Error::other(
                ChannelError::new(Stage::Prepare, Reason::Io, Impact::Component)
                    .with_source(source),
            ));
        }
        let result = caller
            .0
            .result
            .lock()
            .unwrap()
            .take()
            .ok_or_else(|| io::Error::other("original lookup ended without its result"))?;
        drop(caller);
        self.reap_lookups();
        result
    }
    pub(super) fn reap_lookups(&self) {
        let retired = {
            let mut lookups = self.lookups.lock().unwrap();
            let mut retired = Vec::new();
            let mut index = 0;
            while index < lookups.len() {
                if lookups[index].settled() {
                    retired.push(lookups.swap_remove(index));
                } else {
                    index += 1;
                }
            }
            retired
        };
        // Actual result/fault payload destructors never run under owner locks.
        drop(retired);
    }
}
