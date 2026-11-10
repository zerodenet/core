//! This Source's sole quota writer, explicitly genuine or Host-bound.
use super::*;
use std::{num::NonZeroUsize, sync::Condvar, task::{Context, Poll}, time::Duration};
use zero_platform_tokio::contract::scope::TaskRegistrationBudget;
mod producer;
use producer::Producer;
mod task;
pub use task::QuotaWriterFailure;
mod business;
mod standard;
mod scoped;
mod drive;
mod shutdown;

#[derive(Default)]
struct Gate { open: Mutex<bool>, changed: Condvar }
impl Gate {
    fn reset(&self) { *self.open.lock().unwrap_or_else(|error| error.into_inner()) = false; }
    fn release(&self) { *self.open.lock().unwrap_or_else(|error| error.into_inner()) = true; self.changed.notify_all(); }
    fn wait(&self) {
        let mut open = self.open.lock().unwrap_or_else(|error| error.into_inner());
        while !*open { open = self.changed.wait(open).unwrap_or_else(|error| error.into_inner()); }
    }
}
#[derive(Default)]
pub(super) struct Execution {
    state: Mutex<State>,
    pub(super) changed: tokio::sync::Notify,
}
#[derive(Default)]
struct State {
    memory: Option<QuotaMemoryBudget>,
    producer: Option<Producer>,
    tasks: Option<Arc<TaskRegistrationBudget>>,
    pending: bool,
    stopping: bool,
    creating: bool,
    polling: bool,
    final_done: bool,
    last_error: Option<Arc<io::Error>>,
    working_cell: Option<business::Cell>,
    terminal: Option<Arc<io::Error>>,
    standard: Option<standard::Actor>,
    scoped: Option<scoped::Actor>,
    // Actual controller/witness data above drops before the pure promises.
    gate: Option<Arc<Gate>>,
    slot: Option<task::Slot>,
    standard_failure: Option<standard::FailureSlot>,
    business: [Option<business::Cell>; 2],
    // One true std->Scope retirement and one final Scoped retirement. Ordinary
    // retries cannot spend these already accepted final-flush contingencies.
    final_cells: [Option<business::Cell>; 2],
}
impl Execution {
    pub(super) fn cold(memory: &QuotaMemoryBudget) -> io::Result<Self> {
        // All possible fixed liabilities precede the first lease open/decode.
        let slot = task::Slot::reserve(memory)?;
        let standard_failure = standard::FailureSlot::reserve(memory)?;
        let business = [Some(business::Cell::reserve(memory)?), Some(business::Cell::reserve(memory)?)];
        let final_cells = [Some(business::Cell::reserve(memory)?), Some(business::Cell::reserve(memory)?)];
        let gate = Arc::new(Gate::default());
        Ok(Self { state: Mutex::new(State { memory: Some(memory.clone()), slot: Some(slot), standard_failure: Some(standard_failure), business, final_cells, gate: Some(gate), ..Default::default() }), changed: tokio::sync::Notify::new() })
    }
    fn configure<F>(&self, work: F) -> io::Result<()>
    where F: Fn() -> io::Result<()> + Send + Sync + 'static {
        let memory = self.state.lock().unwrap_or_else(|error| error.into_inner()).memory.clone().ok_or_else(|| io::Error::new(io::ErrorKind::PermissionDenied, "quota writer has no original Source budget"))?;
        let producer = Producer::new(&memory, work)?;
        let mut state = self.state.lock().unwrap_or_else(|error| error.into_inner());
        if state.producer.is_some() { drop(state); drop(producer); return Err(io::ErrorKind::AlreadyExists.into()); }
        state.producer = Some(producer);
        Ok(())
    }
    pub(super) fn bind_kernel(&self, tasks: Arc<TaskRegistrationBudget>) -> io::Result<()> {
        let signal = {
            let mut state = self.state.lock().unwrap_or_else(|error| error.into_inner());
            if state.tasks.as_ref().is_some_and(|original| !Arc::ptr_eq(original, &tasks)) {
                return Err(io::Error::new(io::ErrorKind::PermissionDenied, "foreign quota Host task book"));
            }
            state.tasks.get_or_insert(tasks);
            // Enqueue thereafter preserves dirty facts in this same owner;
            // only the already accepted std worker receives its final flush.
            state.standard.as_ref().map(|worker| worker.signal.clone())
        };
        if let Some(signal) = signal { signal.shutdown(); }
        self.changed.notify_waiters();
        Ok(())
    }
    pub(super) fn take_write_cell(&self, closing: bool) -> io::Result<Option<business::Cell>> {
        let (existing, memory) = {
            let mut state = self.state.lock().unwrap_or_else(|error| error.into_inner());
            let cells = if closing { &mut state.final_cells } else { &mut state.business };
            let existing = cells.iter_mut().find_map(Option::take);
            (existing, state.memory.clone())
        };
        if existing.is_some() || closing { return Ok(existing); }
        let Some(memory) = memory else { return Err(io::ErrorKind::PermissionDenied.into()); };
        match business::Cell::reserve(&memory) {
            Ok(cell) => Ok(Some(cell)),
            Err(error) if error.kind() == io::ErrorKind::WouldBlock => Ok(None),
            Err(error) => Err(error),
        }
    }
    fn install_working_cell(&self, cell: business::Cell) -> Result<(), business::Cell> {
        let mut state = self.state.lock().unwrap_or_else(|error| error.into_inner());
        if state.working_cell.is_some() { return Err(cell); }
        state.working_cell = Some(cell);
        Ok(())
    }
    fn take_working_cell(&self) -> Option<business::Cell> {
        self.state.lock().unwrap_or_else(|error| error.into_inner()).working_cell.take()
    }
    pub(super) fn receive_write(&self, result: io::Result<()>, cell: business::Cell, closing: bool) -> Option<Arc<io::Error>> {
        match result {
            Ok(()) => {
                let (old, returned) = {
                    let mut state = self.state.lock().unwrap_or_else(|error| error.into_inner());
                    if closing { state.final_done = true; }
                    let returned = if let Some(slot) = state.business.iter_mut().find(|slot| slot.is_none()) { *slot = Some(cell); None } else { Some(cell) };
                    (state.last_error.take(), returned)
                };
                drop(old); drop(returned);
                None
            }
            Err(original) => {
                let original = cell.receive(original);
                let previous = {
                    let mut state = self.state.lock().unwrap_or_else(|error| error.into_inner());
                    if closing { state.final_done = true; }
                    state.last_error.replace(original.clone())
                };
                // Drop a completed known-close business status only after the
                // next actual error is strongly held, outside the owner lock.
                drop(previous);
                Some(original)
            }
        }
    }
    pub(super) fn last_error(&self) -> Option<Arc<io::Error>> {
        self.state.lock().unwrap_or_else(|error| error.into_inner()).last_error.clone()
    }
}
impl OwnedQuotaStorage {
    pub fn mark_writer_dirty(&self) -> io::Result<()> {
        let (signal, launch) = {
            let mut state = self.execution.state.lock().unwrap_or_else(|error| error.into_inner());
            if state.stopping { return Err(io::ErrorKind::BrokenPipe.into()); }
            state.pending = true;
            if state.tasks.is_some() || state.creating || state.polling {
                (None, None)
            } else if let Some(worker) = &state.standard {
                (Some(worker.signal.clone()), None)
            } else {
                let source = self.source.upgrade().ok_or_else(|| io::Error::new(io::ErrorKind::NotConnected, "quota Source retired"))?;
                if source.persistence_book.get().is_some() {
                    return Err(io::Error::new(io::ErrorKind::PermissionDenied, "Kernel quota writer has no actual Host task book"));
                }
                let producer = state.producer.clone().ok_or_else(|| io::Error::other("quota producer not installed"))?;
                let gate = state.gate.clone().ok_or_else(|| io::Error::other("quota controller not prepared"))?;
                gate.reset();
                state.creating = true;
                (None, Some((producer, gate)))
            }
        };
        if let Some(signal) = signal { signal.dirty(); }
        if let Some((producer, gate)) = launch {
            let started = standard::Actor::start(self.downgrade(), producer, gate.clone());
            let signal = {
                let mut state = self.execution.state.lock().unwrap_or_else(|error| error.into_inner());
                state.creating = false;
                match started {
                    Ok(worker) => {
                        let signal = worker.signal.clone();
                        let moving = state.tasks.is_some();
                        state.standard = Some(worker);
                        Some((signal, moving))
                    }
                    Err(error) => { drop(state); return Err(error); }
                }
            };
            // The actual JoinHandle is strongly installed before the same gate
            // permits first IO, including a concurrent accepted Host handoff.
            if let Some((signal, moving)) = signal {
                if moving { signal.shutdown(); } else { signal.dirty(); }
            }
            gate.release();
        }
        self.execution.changed.notify_waiters();
        Ok(())
    }
    /// The actual quota registry installs only its frozen-book flush producer.
    /// Host-bound mode is explicit and never inferred from Tokio availability.
    pub fn prepare_writer<F>(&self, work: F) -> io::Result<()>
    where F: Fn() -> io::Result<()> + Send + Sync + 'static {
        self.execution.configure(work)
    }
}
