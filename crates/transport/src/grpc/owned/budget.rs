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
}
#[derive(Default)]
struct State {
    physical: Option<Arc<Physical>>,
    app: Weak<()>,
    attempted: bool,
    read_done: bool,
    write_done: bool,
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
    ) -> Result<Lease, ChannelError> {
        self.harvest();
        let mut records = self.records.lock().unwrap();
        if records.len() >= self.limit.get() {
            return Err(error(instance, Stage::Prepare, Reason::Capacity));
        }
        let app = Arc::new(());
        let record = Arc::new(Record {
            instance,
            state: Mutex::new(State {
                app: Arc::downgrade(&app),
                ..Default::default()
            }),
        });
        records.push(record.clone());
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
    fn complete(&self) -> bool {
        let state = self.state.lock().unwrap();
        state.app.upgrade().is_none()
            && (!state.attempted
                || (state.read_done && state.write_done)
                || state
                    .physical
                    .as_ref()
                    .is_some_and(|physical| physical.closed()))
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
        if let Some(budget) = self.budget.upgrade() {
            budget.harvest();
        }
    }
}
