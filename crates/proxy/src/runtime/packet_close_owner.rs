//! Own real packet-close receipts beyond HTTP/IPC acknowledgement deadlines.
use std::{
    num::NonZeroUsize,
    sync::{
        atomic::{AtomicBool, AtomicUsize, Ordering},
        Arc,
    },
};
use tokio::{
    sync::{Mutex, Notify},
    time::{Duration, Instant},
};
use zero_api::{ApiError, ApiErrorCode, ApiResult, CommandResponse, PacketRouteCloseCommand};
use zero_engine::Engine;
use zero_platform_tokio::contract::{
    lifecycle::TaskFailure,
    scope::{TaskClass, TaskExit, TaskScope},
};
mod lifecycle;
#[cfg(test)]
mod tests;

pub(super) struct PacketCloseOwner {
    closing: AtomicBool,
    completed: AtomicUsize,
    changed: Notify,
    accept_gate: std::sync::Mutex<()>,
    owner: Mutex<Owner>,
}
struct Owner {
    scope: Option<TaskScope>,
    limit: NonZeroUsize,
    active: usize,
    failures: Vec<TaskFailure>,
}
impl std::fmt::Debug for PacketCloseOwner {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PacketCloseOwner")
            .field("closing", &self.closing.load(Ordering::Acquire))
            .field("completed", &self.completed.load(Ordering::Acquire))
            .finish_non_exhaustive()
    }
}
struct Completion(Arc<PacketCloseOwner>);
impl Drop for Completion {
    fn drop(&mut self) {
        self.0.completed.fetch_add(1, Ordering::Release);
        self.0.changed.notify_one();
    }
}
impl Owner {
    fn record(&mut self, exit: TaskExit) {
        self.active -= 1;
        if let Err(error) = exit.result {
            self.failures.push(TaskFailure {
                id: exit.id,
                class: exit.class,
                error,
            });
        }
    }
    fn result(&self) -> Result<(), String> {
        self.failures.first().map_or(Ok(()), |failure| {
            Err(format!(
                "packet close task {} failed: {}",
                failure.id.0, failure.error
            ))
        })
    }
}
impl PacketCloseOwner {
    pub(super) fn new(limit: NonZeroUsize) -> Self {
        Self {
            closing: AtomicBool::new(false),
            completed: AtomicUsize::new(0),
            changed: Notify::new(),
            accept_gate: std::sync::Mutex::new(()),
            owner: Mutex::new(Owner {
                scope: None,
                limit,
                active: 0,
                failures: Vec::new(),
            }),
        }
    }
    pub(super) async fn close(
        self: &Arc<Self>,
        engine: Engine,
        request: PacketRouteCloseCommand,
        serial: Arc<Mutex<()>>,
        ack_timeout: Duration,
    ) -> ApiResult<CommandResponse> {
        if self.closing.load(Ordering::Acquire) {
            return Err(stopping());
        }
        let guard = serial.lock_owned().await;
        let mut owner = self.owner.lock().await;
        self.reap(&mut owner).await.map_err(internal)?;
        let receive = {
            let _accept = self
                .accept_gate
                .lock()
                .expect("packet close admission poisoned");
            if self.closing.load(Ordering::Acquire) {
                return Err(stopping());
            }
            let (send, receive) = tokio::sync::oneshot::channel();
            let completion_owner = self.clone();
            // Scope capacity is checked before the future can claim the actual
            // route. A rejected future contains no acquired PacketRouteControl.
            let limit = owner.limit;
            owner
                .scope
                .get_or_insert_with(|| TaskScope::new(tokio::runtime::Handle::current(), limit))
                .registrar()
                .spawn(TaskClass::Critical, async move {
                    let _completion = Completion(completion_owner);
                    let begun = engine.begin_close_packet_route(&request);
                    let (snapshot, control) = match begun {
                        Ok(begun) => begun,
                        Err(error) => {
                            let _ = send.send(Err(error));
                            return Ok(());
                        }
                    };
                    if tokio::time::timeout(ack_timeout, control.wait_released())
                        .await
                        .is_ok()
                    {
                        let snapshot = engine.record_packet_route_closed(snapshot);
                        let _ = send.send(Ok(CommandResponse {
                            accepted: true,
                            result: Some(serde_json::json!({ "route": snapshot, "closed": true })),
                        }));
                        return Ok(());
                    }
                    let _ = send.send(Err(internal(
                        "packet route close unconfirmed; query current state",
                    )));
                    // Device stop/configuration may be what releases this route.
                    // They must not wait for a receipt while blocked by this guard.
                    drop(guard);
                    control.wait_released().await;
                    engine.record_packet_route_closed(snapshot);
                    Ok(())
                })
                .map_err(|source| internal(format!("packet close was not accepted: {source}")))?;
            owner.active += 1;
            receive
        };
        drop(owner);
        receive.await.map_err(|_| {
            internal("packet close ended without an acknowledgement; query current state")
        })?
    }
    async fn reap(&self, owner: &mut Owner) -> Result<(), String> {
        while self.completed.load(Ordering::Acquire) > 0 {
            let exit = owner
                .scope
                .as_mut()
                .expect("accepted packet close scope")
                .next_exit()
                .await
                .ok_or_else(|| "packet close exit remains unobserved".to_owned())?;
            owner.record(exit);
            // Only acknowledge a real joined task, under the same owner lock
            // used by observers and shutdown. Cancelled polling consumes nothing.
            self.completed.fetch_sub(1, Ordering::AcqRel);
        }
        owner.result()
    }
}
fn stopping() -> ApiError {
    internal("packet close owner is stopping")
}
fn internal(message: impl Into<String>) -> ApiError {
    ApiError::new(ApiErrorCode::Internal, message.into())
}
