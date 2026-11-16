//! Repeatable declared reads, retaining each original and blocking join on cancellation.
use super::{FileNativeEvidence, PendingFileRead};
use crate::contract::{
    lifecycle::{CloseReport, ResourceFailure, ResourceFailurePhase, Unfinished},
    BoxFuture, CancellationToken, ChannelError, Impact, Reason, Stage,
};
use std::{collections::BTreeMap, io, path::PathBuf, sync::Arc};
use tokio::{sync::Mutex, task::JoinHandle, time::Instant};
type Output = Result<Vec<u8>, Arc<io::Error>>;
struct Slot {
    bound: usize,
    job: Option<JoinHandle<Output>>,
    original: Option<Arc<PendingFileRead>>,
    registering: bool,
    failure: Option<Arc<ChannelError>>,
    resource_failure: Option<Arc<ResourceFailure>>,
    closed: usize,
    joined: usize,
}
pub struct DeclaredFileReads {
    instance: Option<zero_core::component::ComponentInstanceId>,
    slots: BTreeMap<PathBuf, Mutex<Slot>>,
    cancel: CancellationToken,
    active: std::sync::atomic::AtomicBool,
}
fn error(reason: Reason, source: impl std::error::Error + Send + Sync + 'static) -> ChannelError {
    ChannelError::new(Stage::Prepare, reason, Impact::Component).with_source(source)
}
impl DeclaredFileReads {
    pub fn new(
        files: impl IntoIterator<Item = (PathBuf, usize)>,
        cancel: CancellationToken,
    ) -> Result<Self, ChannelError> {
        let mut slots = BTreeMap::new();
        for (path, bound) in files {
            if path.as_os_str().is_empty()
                || bound.checked_add(1).is_none()
                || slots.contains_key(&path)
            {
                return Err(error(
                    Reason::InvalidConfig,
                    io::Error::other("invalid or duplicate original file declaration"),
                ));
            }
            slots.insert(
                path,
                Mutex::new(Slot {
                    bound,
                    job: None,
                    original: None,
                    registering: false,
                    failure: None,
                    resource_failure: None,
                    closed: 0,
                    joined: 0,
                }),
            );
        }
        Ok(Self {
            instance: None,
            slots,
            cancel,
            active: false.into(),
        })
    }
    pub fn for_component(
        instance: zero_core::component::ComponentInstanceId,
        files: impl IntoIterator<Item = (PathBuf, usize)>,
        cancel: CancellationToken,
    ) -> Result<Self, ChannelError> {
        let mut reads = Self::new(files, cancel)?;
        reads.instance = Some(instance);
        Ok(reads)
    }
    pub fn activate(&self) {
        self.active
            .store(true, std::sync::atomic::Ordering::Release);
    }
    pub fn revoke(&self) {
        self.active
            .store(false, std::sync::atomic::Ordering::Release);
    }
    fn admitted(&self) -> Result<(), ChannelError> {
        if self.cancel.is_cancelled() || !self.active.load(std::sync::atomic::Ordering::Acquire) {
            return Err(error(
                Reason::Revoked,
                io::Error::other("original file source is not active"),
            ));
        }
        Ok(())
    }
    pub fn read(
        &self,
        path: PathBuf,
        limit: usize,
    ) -> BoxFuture<'_, Result<Vec<u8>, ChannelError>> {
        Box::pin(async move {
            self.admitted()?;
            let mut slot = self
                .slots
                .get(&path)
                .ok_or_else(|| {
                    error(
                        Reason::InvalidConfig,
                        io::Error::other("undeclared original file"),
                    )
                })?
                .lock()
                .await;
            self.admitted()?;
            if limit > slot.bound {
                return Err(error(
                    Reason::InvalidConfig,
                    io::Error::other("original file read exceeds its grant"),
                ));
            }
            // A cancelled earlier caller left its exact job here. Join/close it
            // before a new read, never replace it by another PendingFileRead.
            if slot.job.is_some() {
                let _ = slot.finish().await?;
            }
            if let Some(failure) = &slot.failure {
                return Err(ChannelError::retained(failure.clone()));
            }
            if slot.registering {
                return Err(error(
                    Reason::Internal,
                    io::Error::other("original file job registration is unknown"),
                ));
            }
            let original = Arc::new(
                PendingFileRead::new(path, limit).map_err(|e| error(Reason::InvalidConfig, e))?,
            );
            slot.original = Some(original.clone());
            slot.registering = true;
            slot.job = Some(tokio::task::spawn_blocking(move || original.run()));
            slot.registering = false;
            slot.finish()
                .await?
                .map_err(|source| error(Reason::Io, SharedIo(source)))
        })
    }
    pub async fn shutdown(&self, end: Instant) -> Result<CloseReport, ChannelError> {
        self.revoke();
        let mut report = CloseReport::default();
        for (path, slot) in &self.slots {
            let mut slot = match tokio::time::timeout_at(end, slot.lock()).await {
                Ok(slot) => slot,
                Err(_) => {
                    report.unfinished.push(Unfinished::Resource(
                        "original file read waiter remains pending".into(),
                    ));
                    continue;
                }
            };
            if slot.job.is_some() && tokio::time::timeout_at(end, slot.finish()).await.is_err() {
                report.unfinished.push(Unfinished::Resource(
                    "original file worker remains pending".into(),
                ));
                continue;
            }
            if let Some(failure) = slot.failure.clone() {
                match self.instance {
                    Some(instance) => {
                        let original = slot.resource_failure.get_or_insert_with(|| {
                            Arc::new(ResourceFailure::new(
                                instance,
                                ResourceFailurePhase::DeclaredResources,
                                failure,
                            ))
                        });
                        report.resource_failures.push(original.clone());
                    }
                    None => return Err(ChannelError::retained(failure)),
                }
            }
            if slot.registering || slot.original.is_some() {
                report.unfinished.push(Unfinished::Resource(
                    "original file registration/native close is unconfirmed".into(),
                ));
            }
            if slot.closed > 0 {
                report.closed_io.push(format!(
                    "{} original file handles explicitly closed for {}",
                    slot.closed,
                    path.display()
                ));
            }
            if slot.joined > 0 {
                report.released_resources.push(format!(
                    "{} original blocking file jobs joined for {}",
                    slot.joined,
                    path.display()
                ));
            }
        }
        Ok(report)
    }
}
impl Slot {
    async fn finish(&mut self) -> Result<Output, ChannelError> {
        let joined = self
            .job
            .as_mut()
            .expect("installed original file job")
            .await;
        self.job = None;
        self.joined += 1;
        // Retain the original blocking failure before observing native close;
        // a secondary close fault must not replace that real JoinError.
        let result = match joined {
            Ok(value) => Some(value),
            Err(source) => {
                self.failure
                    .get_or_insert_with(|| Arc::new(error(Reason::Internal, source)));
                None
            }
        };
        let evidence = self
            .original
            .as_ref()
            .expect("same original holder")
            .close_after_join();
        match evidence.native {
            FileNativeEvidence::KnownNoAcquisition => {
                self.original = None;
            }
            FileNativeEvidence::ConfirmedClosed => {
                self.closed += 1;
                self.original = None;
            }
            FileNativeEvidence::Unknown(source) => {
                self.failure.get_or_insert_with(|| {
                    Arc::new(
                        ChannelError::new(Stage::Close, Reason::Io, Impact::Component)
                            .with_source(SharedIo(source)),
                    )
                });
            }
            _ => {
                self.failure.get_or_insert_with(|| {
                    Arc::new(
                        ChannelError::new(Stage::Close, Reason::Internal, Impact::Component)
                            .with_source(io::Error::other(
                                "joined file worker has no safe original close disposition",
                            )),
                    )
                });
            }
        }
        match &self.failure {
            Some(failure) => Err(ChannelError::retained(failure.clone())),
            None => Ok(result.expect("successful original join without retained failure")),
        }
    }
}
#[derive(Debug)]
struct SharedIo(Arc<io::Error>);
impl std::fmt::Display for SharedIo {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        std::fmt::Display::fmt(self.0.as_ref(), f)
    }
}
impl std::error::Error for SharedIo {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(self.0.as_ref())
    }
}

#[cfg(test)]
#[path = "../../tests/file/declared.rs"]
mod tests;
