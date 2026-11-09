//! Main's actual fault receiver, external to every worker/controller graph.
use super::*;
#[derive(Debug, Clone)]
pub enum LoggingFailure {
    Io(IoFailure),
    Worker(WorkerFailure),
}
impl std::fmt::Display for LoggingFailure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Io(source) => write!(f, "{source}"),
            Self::Worker(source) => write!(f, "{source}"),
        }
    }
}
pub(super) struct Archive {
    values: Vec<LoggingFailure>,
    _storage: Option<book::Storage>,
}
impl Archive {
    pub(super) fn new() -> Self {
        Self {
            values: Vec::new(),
            _storage: None,
        }
    }
    fn reserve(&mut self, book: &book::Book) -> io::Result<()> {
        if self.values.len() < self.values.capacity() {
            return Ok(());
        }
        let slots = self
            .values
            .len()
            .checked_add(1)
            .and_then(usize::checked_next_power_of_two)
            .ok_or_else(capacity)?;
        let credit = book.layout(
            zero_traits::packet_metadata::PacketMetadataLayout::array::<LoggingFailure>(slots)
                .map_err(io::Error::other)?,
        )?;
        let mut next = Vec::new();
        next.try_reserve_exact(slots).map_err(io::Error::other)?;
        if next.capacity() > slots {
            return Err(capacity());
        }
        next.append(&mut self.values);
        drop(std::mem::replace(&mut self.values, next));
        drop(self._storage.replace(credit));
        Ok(())
    }
    fn push(&mut self, source: LoggingFailure) {
        self.values.push(source);
    }
}
impl ScopedTracing {
    /// Every actual source is held here before local acceptance/Hosted ACK.
    /// Full metadata leaves that same pending original installed, without IO
    /// retry or eviction. A later real budget increase can resume publication.
    pub fn collect_failures(&mut self) -> io::Result<()> {
        while let Some(source) = self.peek_io_failure() {
            self.archive.reserve(&self.shared.book)?;
            self.archive.push(LoggingFailure::Io(source.clone()));
            assert!(
                self.accept_io_failure(&source),
                "same actual logger IO source"
            );
        }
        loop {
            // Reserve receiver storage BEFORE accepting an actual task source.
            // No failed exit exists if every outstanding witness is successful.
            let pending = self.lanes.iter().any(|lane| {
                !lane.failure_delivered
                    && lane
                        .witness
                        .as_ref()
                        .and_then(TaskJoinWitness::try_joined)
                        .is_some_and(|joined| joined.result().is_err())
            });
            if !pending {
                break;
            }
            self.archive.reserve(&self.shared.book)?;
            let Some(source) = self.take_worker_failure()? else {
                break;
            };
            self.archive.push(LoggingFailure::Worker(source.clone()));
            assert!(
                self.accept_worker_failure(&source),
                "same actual logger task source"
            );
        }
        Ok(())
    }
    pub fn failures(&self) -> &[LoggingFailure] {
        &self.archive.values
    }
    /// Poll only the actual supplied Scope. Complete work is never a synthetic
    /// join; its original Hosted receipt remains separate from native close.
    pub fn poll_workers(
        &mut self,
        scope: &mut zero_platform_tokio::contract::scope::HostedScope,
        cx: &mut std::task::Context<'_>,
    ) -> io::Result<()> {
        if !self
            .shared
            .scope
            .same_scope(&scope.registrar().scope_identity())
        {
            return Err(io::Error::new(
                io::ErrorKind::PermissionDenied,
                "foreign logger host",
            ));
        }
        self.shared.book.wake.register(cx.waker());
        self.collect_failures()?;
        self.drive(&scope.registrar())?;
        for _ in 0..self.lanes.len() {
            let Poll::Ready(Some(receipt)) = scope.poll_exit(cx) else {
                break;
            };
            self.collect_failures()?;
            if receipt.result().is_err() && !receipt.local_failure_accepted() {
                return Err(io::Error::other("logger failure receiver not yet accepted"));
            }
            scope.acknowledge(&receipt).map_err(io::Error::other)?;
            drop(receipt);
        }
        self.collect_failures()?;
        Ok(())
    }
}
