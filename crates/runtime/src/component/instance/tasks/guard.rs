//! Enclosing lock deferral includes native relay and real witness callbacks.
use super::*;
use std::{
    ops::{Deref, DerefMut},
    sync::MutexGuard,
};
use zero_platform_tokio::contract::scope::HostedCallbacks;

pub(super) struct TasksGuard<'a> {
    inner: Option<MutexGuard<'a, Tasks>>,
    callbacks: Option<HostedCallbacks>,
    wake: Option<wake::Deferred>,
}
impl TaskMonitor {
    pub(super) fn lock(&self) -> TasksGuard<'_> {
        let wake = self.wake.enter();
        let inner = self.inner.lock().expect("component tasks poisoned");
        let callbacks = inner.scope.defer_callbacks();
        TasksGuard {
            inner: Some(inner),
            callbacks: Some(callbacks),
            wake: Some(wake),
        }
    }
}
impl Deref for TasksGuard<'_> {
    type Target = Tasks;
    fn deref(&self) -> &Tasks {
        self.inner.as_deref().expect("locked component tasks")
    }
}
impl DerefMut for TasksGuard<'_> {
    fn deref_mut(&mut self) -> &mut Tasks {
        self.inner.as_deref_mut().expect("locked component tasks")
    }
}
impl Drop for TasksGuard<'_> {
    fn drop(&mut self) {
        // The caller may synchronously re-enter the same monitor on wake/Drop.
        drop(self.inner.take());
        drop(self.callbacks.take());
        drop(self.wake.take());
    }
}
