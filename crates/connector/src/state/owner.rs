//! One concrete synchronous inspection; unknown originals fence its successor.
use crate::resources::DispatcherResources;
use std::{
    io,
    sync::{Arc, Condvar, Mutex},
};
use zero_runtime::configuration::ConfigurationSource;
struct Slot {
    state: Mutex<State>,
    changed: Condvar,
}
struct State {
    current: Option<Arc<Attempt>>,
    running: bool,
}
static SLOT: Slot = Slot {
    state: Mutex::new(State {
        current: None,
        running: false,
    }),
    changed: Condvar::new(),
};
pub(super) struct Attempt {
    pub(super) source: Arc<ConfigurationSource>,
    pub(super) resources: DispatcherResources,
    faults: Mutex<Faults>,
}
#[derive(Default)]
struct Faults {
    work: Vec<Arc<io::Error>>,
    resources: Option<Arc<io::Error>>,
    source: Option<Arc<io::Error>>,
}
pub(super) struct Lease<'a> {
    pub(super) original: Arc<Attempt>,
    _execution: Running<'a>,
}
struct Running<'a> {
    slot: &'a Slot,
}
impl Drop for Running<'_> {
    fn drop(&mut self) {
        self.slot
            .state
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .running = false;
        self.slot.changed.notify_all();
    }
}
impl Attempt {
    pub(super) fn work_failed(&self, error: io::Error) {
        // Exactly the configured outbox and dead-letter inspection, not one
        // row per JSONL record or an unbounded history of retry attempts.
        self.work_failed_arc(Arc::new(error));
    }
    pub(super) fn work_failed_arc(&self, error: Arc<io::Error>) {
        let mut faults = self
            .faults
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        assert!(
            faults.work.len() < 2,
            "one failure per actual inspected file"
        );
        faults.work.push(error);
    }
    fn progress(&self) -> bool {
        let resources = self.resources.close();
        let source = self.source.confirm_original_io_closed();
        let mut faults = self
            .faults
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        if let Err(error) = resources {
            faults.resources.get_or_insert_with(|| Arc::new(error));
        }
        if let Err(error) = source {
            faults.source.get_or_insert_with(|| Arc::new(error));
        }
        drop(faults);
        // Historical reported faults never substitute for, or veto, positive
        // independent original close. Unknown native ownership remains held.
        self.resources.retirement_complete() && self.source.confirm_original_io_closed().is_ok()
    }
    pub(super) fn cleanup_failure(&self) -> Option<Arc<io::Error>> {
        let faults = self
            .faults
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        faults
            .resources
            .as_ref()
            .or(faults.source.as_ref())
            .cloned()
    }
    fn received(self: &Arc<Self>) -> Vec<Arc<io::Error>> {
        let faults = self
            .faults
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        faults
            .work
            .iter()
            .chain(faults.resources.iter())
            .chain(faults.source.iter())
            .map(|fault| {
                Arc::new(io::Error::new(
                    fault.kind(),
                    Retained {
                        _original: self.clone(),
                        fault: fault.clone(),
                    },
                ))
            })
            .collect()
    }
}
struct Retained {
    // Real pending IO/SQLite custody survives a dropped/converted report.
    _original: Arc<Attempt>,
    fault: Arc<io::Error>,
}
impl std::fmt::Debug for Retained {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("OriginalInspectionFailure")
            .field("fault", &self.fault)
            .finish_non_exhaustive()
    }
}
impl std::fmt::Display for Retained {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.fault.fmt(f)
    }
}
impl std::error::Error for Retained {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(self.fault.as_ref())
    }
}
type ReceivedFailures = Vec<Arc<io::Error>>;
type BeginResult<'a> = Result<(Lease<'a>, ReceivedFailures), ReceivedFailures>;
pub(super) fn begin(source: Arc<ConfigurationSource>) -> BeginResult<'static> {
    begin_on(&SLOT, source)
}
fn begin_on(slot: &Slot, source: Arc<ConfigurationSource>) -> BeginResult<'_> {
    let previous = {
        let mut state = slot.state.lock().unwrap_or_else(|error| error.into_inner());
        while state.running {
            state = slot
                .changed
                .wait(state)
                .unwrap_or_else(|error| error.into_inner());
        }
        state.running = true;
        state.current.clone()
    };
    let execution = Running { slot };
    let mut inherited = Vec::new();
    if let Some(previous) = previous {
        let complete = previous.progress();
        inherited.extend(previous.received());
        if !complete {
            return Err(inherited);
        }
    }
    let original = Arc::new(Attempt {
        source,
        resources: DispatcherResources::default(),
        faults: Mutex::default(),
    });
    let old = {
        let mut state = slot.state.lock().unwrap_or_else(|error| error.into_inner());
        state.current.replace(original.clone())
    };
    drop(old); // Actual close above, outside the slot lock.
    Ok((
        Lease {
            original,
            _execution: execution,
        },
        inherited,
    ))
}
impl Lease<'_> {
    pub(super) fn finish(self) -> (bool, Vec<Arc<io::Error>>) {
        let complete = self.original.progress();
        let faults = self.original.received();
        let retired = {
            let mut state = self
                ._execution
                .slot
                .state
                .lock()
                .unwrap_or_else(|error| error.into_inner());
            if complete
                && state
                    .current
                    .as_ref()
                    .is_some_and(|current| Arc::ptr_eq(current, &self.original))
            {
                state.current.take()
            } else {
                None
            }
        };
        drop(retired);
        (complete, faults)
    }
}

#[cfg(test)]
mod tests;
