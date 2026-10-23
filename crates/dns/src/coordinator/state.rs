use super::*;

#[derive(Debug)]
pub(super) struct CoordinatorState<T> {
    pub(super) closing: bool,
    pub(super) observed_egress_generation: Option<u64>,
    pub(super) in_flight: HashMap<QueryKey, Arc<Flight<T>>>,
    pub(super) failures: HashMap<QueryKey, CachedFailure>,
    // The coordinator owns every actual worker join handle, including after
    // the last caller cancels. Abort requests never stand in for an actual join.
    pub(super) tasks: JoinSet<()>,
    pub(super) task_failure: Option<SharedError>,
    #[cfg(test)]
    pub(super) spawn_gate: Option<super::tests::SpawnGate>,
}

impl<T> CoordinatorState<T> {
    pub(super) fn observe_exit(&mut self, result: Result<(), tokio::task::JoinError>) {
        if let Err(source) = result {
            if !source.is_cancelled() {
                let error = SharedError::from(io::Error::other(source));
                for flight in self.in_flight.drain().map(|(_, flight)| flight) {
                    flight.cancel(error.to_io_error());
                }
                self.task_failure = Some(error);
            }
        }
    }
    pub(super) fn reap_finished(&mut self) {
        while let Some(result) = self.tasks.try_join_next() {
            self.observe_exit(result);
        }
    }
}

#[derive(Debug)]
pub(super) struct CachedFailure {
    pub(super) error: SharedError,
    pub(super) expires_at: Instant,
}
