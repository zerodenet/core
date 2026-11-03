use super::*;
use crate::application::services::network::{stream::EventStream, unique_candidates};
use std::net::ToSocketAddrs;
use zero_platform_tokio::{EgressInterfaceControl, PendingTcpConnect};
use zero_traits::DialPolicy;

impl Epoch {
    pub(in super::super) async fn resolve(
        &self,
        host: String,
        port: u16,
    ) -> io::Result<Vec<SocketAddr>> {
        if self.closing.load(Ordering::Acquire)
            || self.failed.load(Ordering::Acquire)
            || !self.faults.lock().unwrap().is_empty()
        {
            return Err(io::Error::other("event generation closing"));
        }
        let job = {
            let _admission = self.admission.lock().unwrap();
            if self.closing.load(Ordering::Acquire)
                || self.failed.load(Ordering::Acquire)
                || !self.faults.lock().unwrap().is_empty()
            {
                return Err(io::Error::other("event generation closing"));
            }
            let job = Arc::new(AsyncMutex::new(Some(tokio::task::spawn_blocking(
                move || {
                    (host.as_str(), port)
                        .to_socket_addrs()
                        .map(unique_candidates)
                },
            ))));
            self.lookups.lock().unwrap().push(job.clone());
            job
        };
        let mut retained = job.lock().await;
        let result = retained
            .as_mut()
            .expect("original lookup retained")
            .await
            .map_err(io::Error::other)?;
        retained.take();
        drop(retained);
        self.lookups
            .lock()
            .unwrap()
            .retain(|value| !Arc::ptr_eq(value, &job));
        result
    }
    pub(in super::super) async fn connect(
        self: &Arc<Self>,
        peer: SocketAddr,
        egress: &EgressInterfaceControl,
        deadline: Instant,
    ) -> io::Result<EventStream> {
        if self.closing.load(Ordering::Acquire)
            || self.failed.load(Ordering::Acquire)
            || !self.faults.lock().unwrap().is_empty()
        {
            return Err(io::Error::other("event generation closing"));
        }
        self.reap().await?;
        let (original, witness, rx) = {
            let _admission = self.admission.lock().unwrap();
            if self.closing.load(Ordering::Acquire)
                || self.failed.load(Ordering::Acquire)
                || !self.faults.lock().unwrap().is_empty()
            {
                return Err(io::Error::other("event generation closing"));
            }
            if self.originals.lock().unwrap().len() >= 256 {
                return Err(io::Error::new(
                    io::ErrorKind::WouldBlock,
                    "event native connection capacity exhausted",
                ));
            }
            let selection = egress.select_for_peer_with_policy(peer, &DialPolicy::default())?;
            selection.ensure_connectable()?;
            if selection.generation() != self.generation || egress.generation() != self.generation {
                return Err(io::Error::other("event dial network generation retired"));
            }
            let prepared = PendingTcpConnect::prepare(
                peer,
                &DialPolicy::default(),
                &selection,
                self.stop.child_token(),
                None,
            );
            let (mut pending, control) = match prepared {
                Ok(value) => value,
                Err(source) => {
                    let unknown = source.stage() != "validate_dial_policy";
                    let source = Arc::new(source);
                    if unknown {
                        self.faults
                            .lock()
                            .unwrap()
                            .push(io::Error::other(source.clone()));
                    }
                    return Err(io::Error::other(source));
                }
            };
            let original = Arc::new(Original {
                control: AsyncMutex::new(control),
                custody: Arc::new(Custody::default()),
                receipt: Mutex::new(None),
            });
            // Original controller installed synchronously before any connect await.
            self.originals.lock().unwrap().push(original.clone());
            let custody = original.custody.clone();
            let (tx, rx) = oneshot::channel();
            let spawned = self.spawn_original(Box::pin(async move {
                let result = pending
                    .connect(deadline)
                    .await
                    .map(|stream| EventStream::new(stream, custody))
                    .map_err(io::Error::other);
                let _ = tx.send(result);
            }));
            let witness = match spawned {
                Ok(witness) => {
                    original.custody.add(witness.clone());
                    witness
                }
                Err(error) => {
                    original.custody.seal();
                    return Err(error);
                }
            };
            original.custody.seal();
            (original, witness, rx)
        };
        let result = rx
            .await
            .map_err(|_| io::Error::other("original event connect task stopped"))?;
        let joined = tokio::time::timeout_at(deadline, witness.wait())
            .await
            .map_err(|_| {
                io::Error::new(io::ErrorKind::TimedOut, "event connect exit not joined")
            })?;
        if joined.result().is_err() {
            return Err(io::Error::other("original event connect task failed"));
        }
        match result {
            Ok(stream) => {
                if egress.generation() != self.generation {
                    drop(stream);
                    self.close_returned(deadline).await?;
                    return Err(io::Error::other(
                        "event dial completed on a retired network generation",
                    ));
                }
                Ok(stream)
            }
            Err(error) => {
                self.close_returned(deadline).await?;
                if original.receipt.lock().unwrap().is_none() {
                    return Err(io::Error::other(
                        "failed event candidate original not closed",
                    ));
                }
                Err(error)
            }
        }
    }
}
