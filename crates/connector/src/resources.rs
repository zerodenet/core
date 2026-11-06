//! The original dispatcher's finite resource inventory, installed before initialization.
use std::{
    fs::{File, Metadata, TryLockError},
    io::{self, Read, Seek, SeekFrom, Write},
    sync::{Arc, Mutex},
};
use zero_api::JsonLineEventSink;
use zero_platform_tokio::file::OriginalFile;
mod database;
mod dead_letter;
pub(crate) use database::OriginalDatabase;
pub(crate) use dead_letter::RetainedDeadLetter;

#[derive(Clone, Default)]
pub(crate) struct DispatcherResources(Arc<Resources>);
#[derive(Default)]
struct Resources {
    files: Mutex<Vec<Arc<Mutex<OriginalFile>>>>,
    databases: Mutex<Vec<Arc<Mutex<OriginalDatabase>>>>,
    jsonl: Mutex<Vec<Arc<JsonLineEventSink<FileView>>>>,
    dead_letters: Mutex<Vec<Arc<RetainedDeadLetter>>>,
    failure: Mutex<Option<Arc<io::Error>>>,
    // At most one completed fault per actual writer/database removed at retirement.
    retired_faults: Mutex<Vec<Arc<io::Error>>>,
    retired_databases: Mutex<Vec<Arc<Mutex<OriginalDatabase>>>>,
}
#[derive(Debug)]
pub(crate) struct FileView {
    original: Arc<Mutex<OriginalFile>>,
}
impl DispatcherResources {
    #[cfg(test)]
    pub(crate) fn original_file_count_for_test(&self) -> usize {
        self.0.files.lock().unwrap().len()
    }
    /// Every row is removed only after its own actual native close and returned views.
    pub(crate) fn retirement_complete(&self) -> bool {
        self.0.files.lock().unwrap().is_empty()
            && self.0.databases.lock().unwrap().is_empty()
            && self.0.jsonl.lock().unwrap().is_empty()
            && self.0.dead_letters.lock().unwrap().is_empty()
    }
    fn available(&self) -> io::Result<()> {
        if let Some(error) = self.0.failure.lock().unwrap().as_ref() {
            return Err(io::Error::new(error.kind(), error.clone()));
        }
        Ok(())
    }
    fn retain_failure(&self, error: io::Error) -> io::Error {
        let source = Arc::new(error);
        self.0
            .failure
            .lock()
            .unwrap()
            .get_or_insert_with(|| source.clone());
        io::Error::new(source.kind(), source)
    }
    fn retain_retired_failure(&self, error: io::Error) {
        let source = Arc::new(error);
        self.0
            .failure
            .lock()
            .unwrap()
            .get_or_insert_with(|| source.clone());
        self.0.retired_faults.lock().unwrap().push(source);
    }
    pub(crate) fn open(&self, open: impl FnOnce() -> io::Result<File>) -> io::Result<FileView> {
        self.retire_files(false)?;
        self.available()?;
        let original = Arc::new(Mutex::new(OriginalFile::adopt(open()?)));
        self.0.files.lock().unwrap().push(original.clone());
        Ok(FileView { original })
    }
    pub(crate) fn temporary(
        &self,
        parent: &std::path::Path,
    ) -> io::Result<(FileView, tempfile::TempPath)> {
        self.retire_files(false)?;
        self.available()?;
        let (file, path) = tempfile::NamedTempFile::new_in(parent)?.into_parts();
        let original = Arc::new(Mutex::new(OriginalFile::adopt(file)));
        self.0.files.lock().unwrap().push(original.clone());
        Ok((FileView { original }, path))
    }
    pub(crate) fn jsonl(&self, file: FileView) -> Arc<JsonLineEventSink<FileView>> {
        let sink = Arc::new(JsonLineEventSink::new(file));
        self.0.jsonl.lock().unwrap().push(sink.clone());
        sink
    }
    /// Release actual returned views first, then consume each original receipt.
    pub(crate) fn retire_files(&self, require_all: bool) -> io::Result<()> {
        let mut files = self.0.files.lock().unwrap();
        let mut position = 0;
        let mut outstanding = false;
        while position < files.len() {
            if Arc::strong_count(&files[position]) != 1 {
                outstanding = true;
                position += 1;
                continue;
            }
            let mut original = files[position]
                .lock()
                .unwrap_or_else(|error| error.into_inner());
            let outcome = original.close();
            match outcome {
                Ok(receipt) if original.matches_closed(&receipt) => {
                    drop(original);
                    files.remove(position);
                }
                Ok(_) => {
                    self.retain_failure(io::Error::other("file receipt mismatches original"));
                    drop(original);
                    position += 1;
                }
                Err(error) => {
                    self.retain_failure(error);
                    drop(original);
                    position += 1;
                }
            }
        }
        if require_all && outstanding {
            return Err(self.retain_failure(io::Error::other(
                "original dispatcher file still has a transferred view",
            )));
        }
        self.available()
    }
    pub(crate) fn close(&self) -> io::Result<()> {
        // The real blocking publishers and original sink workers must already
        // have joined. Arc uniqueness is only view retirement, never native close.
        let mut sinks = self.0.jsonl.lock().unwrap();
        let mut held = Vec::new();
        while let Some(sink) = sinks.pop() {
            match Arc::try_unwrap(sink) {
                Ok(sink) => match sink.into_inner_recover() {
                    Ok(writer) => drop(writer),
                    Err((writer, error)) => {
                        drop(writer);
                        self.retain_retired_failure(io::Error::other(error));
                    }
                },
                Err(sink) => held.push(sink),
            }
        }
        let held_jsonl = !held.is_empty();
        *sinks = held;
        drop(sinks);
        let mut dead_letters = self.0.dead_letters.lock().unwrap();
        let mut held = Vec::new();
        while let Some(sink) = dead_letters.pop() {
            match Arc::try_unwrap(sink) {
                Ok(sink) => match sink.into_inner_recover() {
                    Ok(writer) => drop(writer),
                    Err((writer, error)) => {
                        drop(writer);
                        self.retain_retired_failure(io::Error::other(error));
                    }
                },
                Err(sink) => held.push(sink),
            }
        }
        let held_dead_letters = !held.is_empty();
        *dead_letters = held;
        drop(dead_letters);
        let databases = self.retire_databases(true);
        let files = self.retire_files(true);
        databases.and(files)?;
        if held_jsonl || held_dead_letters {
            return Err(self.retain_failure(io::Error::other(
                "original event writer still has an active publisher",
            )));
        }
        self.available()
    }
}
impl FileView {
    pub(crate) fn metadata(&self) -> io::Result<Metadata> {
        self.original.lock().unwrap().metadata()
    }
    pub(crate) fn sync_all(&self) -> io::Result<()> {
        self.original.lock().unwrap().sync_all()
    }
    pub(crate) fn sync_data(&self) -> io::Result<()> {
        self.original.lock().unwrap().sync_data()
    }
    pub(crate) fn set_len(&self, size: u64) -> io::Result<()> {
        self.original.lock().unwrap().set_len(size)
    }
    pub(crate) fn try_lock(&self) -> Result<(), TryLockError> {
        self.original.lock().unwrap().try_lock()
    }
}
impl Read for FileView {
    fn read(&mut self, bytes: &mut [u8]) -> io::Result<usize> {
        self.original.lock().unwrap().read(bytes)
    }
}
impl Write for FileView {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        self.original.lock().unwrap().write(bytes)
    }
    fn flush(&mut self) -> io::Result<()> {
        self.original.lock().unwrap().flush()
    }
}
impl Seek for FileView {
    fn seek(&mut self, position: SeekFrom) -> io::Result<u64> {
        self.original.lock().unwrap().seek(position)
    }
}

#[cfg(test)]
mod tests;
