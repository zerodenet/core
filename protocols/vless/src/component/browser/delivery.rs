//! One actual setup result/start gate, not a queue, Scope or task executor.
//! The private erased owner keeps exactly one concrete data Arc. No raw Arc or
//! Weak escapes; every clone carries the closed paid header custody.
use super::*;
use std::{any::Any, sync::Mutex};
use zero_traits::packet_metadata::PacketMetadataWeak;
struct State<T> {
    value: Option<T>,
    producer_done: bool,
    receiver_closed: bool,
}
struct Data<T> {
    state: Mutex<State<T>>,
    changed: tokio::sync::Notify,
}
pub(super) struct Owner {
    actual: Arc<dyn Any + Send + Sync>,
}
pub(super) struct Sender<T: Send + 'static> {
    owner: Option<PacketMetadataArc<Owner>>,
    kind: std::marker::PhantomData<T>,
}
pub(super) struct Receiver<T: Send + 'static> {
    owner: PacketMetadataArc<Owner>,
    kind: std::marker::PhantomData<T>,
}
impl Owner {
    fn data<T: Send + 'static>(&self) -> &Data<T> {
        self.actual
            .downcast_ref()
            .expect("sealed original delivery type")
    }
    pub fn discard<T: Send + 'static>(&self) {
        let data = self.data::<T>();
        let removed = {
            let mut state = data
                .state
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            state.receiver_closed = true;
            state.value.take()
        };
        data.changed.notify_waiters();
        // This can be the actual unique original IO. Destruction/credit/wake
        // is outside the delivery gate and every Root/Entry/Scope mutex.
        drop(removed);
    }
}
pub(super) fn prepare<T: Send + 'static>(
    memory: &PacketDriverMetadataView,
) -> Result<(Sender<T>, Receiver<T>, PacketMetadataWeak<Owner>), ChannelError> {
    let layout = PacketMetadataLayout::arc::<Data<T>>()
        .and_then(|data| data.checked_add(PacketMetadataArc::<Owner>::layout()?))
        .map_err(capacity)?;
    let credit = memory.reserve(layout).map_err(capacity)?;
    // The only inner strong Arc is moved into this concrete paid Holder.
    // Holder destroys it before its own allocation/header credit can return.
    let actual: Arc<dyn Any + Send + Sync> = Arc::new(Data::<T> {
        state: Mutex::new(State {
            value: None,
            producer_done: false,
            receiver_closed: false,
        }),
        changed: tokio::sync::Notify::new(),
    });
    let owner = PacketMetadataArc::from_prepaid(Owner { actual }, credit)
        .map_err(|(source, _, _)| capacity(source))?;
    let weak = PacketMetadataArc::downgrade(&owner);
    Ok((
        Sender {
            owner: Some(owner.clone()),
            kind: std::marker::PhantomData,
        },
        Receiver {
            owner,
            kind: std::marker::PhantomData,
        },
        weak,
    ))
}
impl<T: Send + 'static> Sender<T> {
    pub fn send(mut self, value: T) -> Result<(), T> {
        let data = self.owner.as_ref().unwrap().data::<T>();
        let result = {
            let mut state = data
                .state
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            assert!(!state.producer_done, "one actual result");
            state.producer_done = true;
            if state.receiver_closed {
                Err(value)
            } else {
                state.value = Some(value);
                Ok(())
            }
        };
        data.changed.notify_waiters();
        drop(self.owner.take());
        result
    }
}
impl<T: Send + 'static> Drop for Sender<T> {
    fn drop(&mut self) {
        if let Some(owner) = &self.owner {
            let data = owner.data::<T>();
            {
                let mut state = data
                    .state
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner);
                state.producer_done = true;
            }
            data.changed.notify_waiters();
        }
    }
}
impl<T: Send + 'static> Receiver<T> {
    pub async fn receive(&mut self) -> Result<T, ChannelError> {
        let data = self.owner.data::<T>();
        loop {
            let changed = data.changed.notified();
            tokio::pin!(changed);
            changed.as_mut().enable();
            let (value, finished) = {
                let mut state = data
                    .state
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner);
                (
                    state.value.take(),
                    state.producer_done || state.receiver_closed,
                )
            };
            if let Some(value) = value {
                return Ok(value);
            }
            if finished {
                return Err(error(Stage::Prepare, Reason::Revoked));
            }
            changed.await;
        }
    }
}
impl<T: Send + 'static> Drop for Receiver<T> {
    fn drop(&mut self) {
        self.owner.discard::<T>();
    }
}

#[cfg(test)]
#[path = "../../../tests/component/browser/delivery.rs"]
mod tests;
