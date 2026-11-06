//! A returned stream and both real worker futures share one finite admission.
use super::{state::Physical, *};
use std::{
    num::NonZeroUsize,
    sync::{Arc, Mutex, Weak},
};
pub(super) struct Budget {
    limit: NonZeroUsize,
    records: Mutex<Vec<Arc<Record>>>,
}
pub(super) struct Record {
    instance: ComponentInstanceId,
    state: Mutex<State>,
    pub(super) stop: CancellationToken,
    changed: tokio::sync::Notify,
}
#[derive(Default)]
struct State {
    physical: Option<Arc<Physical>>,
    app: Weak<()>,
    attempted: bool,
    read_done: bool,
    write_done: bool,
    read_returned: bool,
    write_returned: bool,
}
pub(super) struct Lease {
    pub(super) record: Arc<Record>,
    budget: Weak<Budget>,
    app: Option<Arc<()>>,
}
impl Budget {
    pub(super) fn new(limit: NonZeroUsize) -> Arc<Self> {
        Arc::new(Self {
            limit,
            records: Mutex::new(Vec::new()),
        })
    }
    pub(super) fn reserve(
        self: &Arc<Self>,
        instance: ComponentInstanceId,
        totals: Option<&super::totals::Active>,
    ) -> Result<Lease, ChannelError> {
        self.harvest();
        let total = totals.map(|totals| totals.logical(instance)).transpose()?;
        let mut records = self.records.lock().unwrap();
        if records.len() >= self.limit.get() {
            drop(records);
            if let Some(total) = total {
                total.refuse_unassigned()?;
            }
            return Err(error(instance, Stage::Prepare, Reason::Capacity));
        }
        let app = Arc::new(());
        let record = Arc::new(Record {
            instance,
            stop: CancellationToken::new(),
            changed: Default::default(),
            state: Mutex::new(State {
                app: Arc::downgrade(&app),
                ..Default::default()
            }),
        });
        records.push(record.clone());
        if let Some(total) = total {
            total.install(record.clone())?;
        }
        Ok(Lease {
            record,
            budget: Arc::downgrade(self),
            app: Some(app),
        })
    }
    pub(super) fn harvest(&self) {
        self.records
            .lock()
            .unwrap()
            .retain(|record| !record.complete());
    }
    pub(super) fn retained(&self) -> usize {
        self.harvest();
        self.records.lock().unwrap().len()
    }
}
impl Record {
    pub(super) fn complete(&self) -> bool {
        let state = self.state.lock().unwrap();
        state.app.upgrade().is_none()
            && (!state.attempted
                || (state.read_done
                    && state.write_done
                    && state.read_returned
                    && state.write_returned)
                || state
                    .physical
                    .as_ref()
                    .is_some_and(|physical| physical.closed()))
    }
    pub(super) fn returned(&self) -> bool {
        let state = self.state.lock().unwrap();
        state.app.upgrade().is_none()
            && (!state.attempted
                || (state.read_returned && state.write_returned)
                || state
                    .physical
                    .as_ref()
                    .is_some_and(|physical| physical.closed()))
    }
    pub(super) fn worker_returned(&self, read: bool) {
        {
            let mut state = self.state.lock().unwrap();
            if read {
                state.read_returned = true;
            } else {
                state.write_returned = true;
            }
        }
        self.changed.notify_waiters();
    }
    pub(super) async fn wait_returned(
        &self,
        end: Instant,
    ) -> Result<zero_platform_tokio::contract::lifecycle::CloseReport, ChannelError> {
        loop {
            let changed = self.changed.notified();
            tokio::pin!(changed);
            changed.as_mut().enable();
            let complete = self.returned();
            if complete {
                return Ok(Default::default());
            }
            if tokio::time::timeout_at(end, changed).await.is_err() {
                return Ok(zero_platform_tokio::contract::lifecycle::CloseReport {
                    unfinished: vec![zero_platform_tokio::contract::lifecycle::Unfinished::Io(
                        "actual gRPC logical stream/workers remain retained".into(),
                    )],
                    ..Default::default()
                });
            }
        }
    }
    pub(super) fn worker_done(&self, read: bool) {
        let mut state = self.state.lock().unwrap();
        if read {
            state.read_done = true;
        } else {
            state.write_done = true;
        }
    }
}
impl Lease {
    pub(super) fn bind(&self, physical: Arc<Physical>) -> Result<(), ChannelError> {
        let mut state = self.record.state.lock().unwrap();
        if self.record.instance != physical.adoption.instance()
            || state.physical.is_some()
            || state.attempted
        {
            return Err(error(
                self.record.instance,
                Stage::Prepare,
                Reason::StalePlan,
            ));
        }
        state.physical = Some(physical);
        Ok(())
    }
    pub(super) fn attempting(&self) {
        self.record.state.lock().unwrap().attempted = true;
    }
}
impl Drop for Lease {
    fn drop(&mut self) {
        drop(self.app.take());
        self.record.changed.notify_waiters();
        if let Some(budget) = self.budget.upgrade() {
            budget.harvest();
        }
    }
}
