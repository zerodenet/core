//! Same original stack worker futures, admitted to the listener's retained set.
use super::tasks::RetainedTasks;
use std::{future::Future, pin::Pin};

pub(super) struct StackTasks(pub(super) RetainedTasks<()>);
impl zero_stack::TcpTaskOwner for StackTasks {
    fn spawn(&self, worker: Pin<Box<dyn Future<Output = ()> + Send + 'static>>) -> bool {
        let Some(mut tasks) = self.0.try_lock() else {
            return false;
        };
        while let Some(joined) = tasks.try_join_next() {
            if let Err(error) = joined {
                let cancelled = error.is_cancelled();
                self.0.remember(error);
                if !cancelled {
                    return false;
                }
            }
        }
        if tasks.len() >= zero_stack::UserTcpStack::BACKGROUND_TASK_LIMIT {
            return false;
        }
        tasks.spawn(worker);
        true
    }
}
