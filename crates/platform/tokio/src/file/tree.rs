//! One host-granted file session. Dropping a waiter never drops the actual job
//! or closes the original descriptor. The host owns both independently.
use super::*;
use std::{
    path::PathBuf,
    sync::{Mutex, MutexGuard},
};
use tokio::sync::{mpsc, oneshot};
use tokio_util::sync::CancellationToken;
mod directory;
pub enum FileTreeOperation {
    Canonicalize(PathBuf),
    Metadata(PathBuf),
    List(PathBuf),
    Open(PathBuf),
    Read { offset: u64, length: usize },
}
pub enum FileTreeOutput {
    Path(PathBuf),
    Metadata(std::fs::Metadata),
    Entries(Vec<String>),
    Bytes(Vec<u8>),
}
pub struct FileTreeCommand {
    pub operation: FileTreeOperation,
    pub reply: oneshot::Sender<io::Result<FileTreeOutput>>,
}
pub struct FileTreeSession {
    sender: mpsc::Sender<FileTreeCommand>,
    stop: CancellationToken,
    returned: CancellationToken,
}
impl FileTreeSession {
    /// Called only after the host installs the actual accepted resource owner.
    pub fn channel(
        stop: CancellationToken,
        returned: CancellationToken,
    ) -> (Self, mpsc::Receiver<FileTreeCommand>) {
        let (sender, receiver) = mpsc::channel(1);
        (
            Self {
                sender,
                stop,
                returned,
            },
            receiver,
        )
    }
    async fn execute(&mut self, operation: FileTreeOperation) -> io::Result<FileTreeOutput> {
        let (reply, received) = oneshot::channel();
        tokio::select! {biased;
            _=self.stop.cancelled()=>return Err(retired()),
            result=self.sender.send(FileTreeCommand {operation,reply})=>result.map_err(|_|retired())?,
        }
        tokio::select! {biased;_=self.stop.cancelled()=>Err(retired()), result=received=>result.map_err(|_|retired())?}
    }
    pub async fn canonicalize(&mut self, path: PathBuf) -> io::Result<PathBuf> {
        match self.execute(FileTreeOperation::Canonicalize(path)).await? {
            FileTreeOutput::Path(path) => Ok(path),
            _ => unreachable!(),
        }
    }
    pub async fn metadata(&mut self, path: PathBuf) -> io::Result<std::fs::Metadata> {
        match self.execute(FileTreeOperation::Metadata(path)).await? {
            FileTreeOutput::Metadata(metadata) => Ok(metadata),
            _ => unreachable!(),
        }
    }
    pub async fn list(&mut self, path: PathBuf) -> io::Result<Vec<String>> {
        match self.execute(FileTreeOperation::List(path)).await? {
            FileTreeOutput::Entries(entries) => Ok(entries),
            _ => unreachable!(),
        }
    }
    pub async fn open(&mut self, path: PathBuf) -> io::Result<std::fs::Metadata> {
        match self.execute(FileTreeOperation::Open(path)).await? {
            FileTreeOutput::Metadata(metadata) => Ok(metadata),
            _ => unreachable!(),
        }
    }
    pub async fn read_at(&mut self, offset: u64, length: usize) -> io::Result<Vec<u8>> {
        match self
            .execute(FileTreeOperation::Read { offset, length })
            .await?
        {
            FileTreeOutput::Bytes(bytes) => Ok(bytes),
            _ => unreachable!(),
        }
    }
}
impl Drop for FileTreeSession {
    fn drop(&mut self) {
        self.stop.cancel();
        self.returned.cancel();
    }
}
#[derive(Default)]
struct State {
    file: Option<File>,
    directory: Option<directory::Directory>,
    closed: u64,
    reported: u64,
    fault: Option<Arc<io::Error>>,
    retired: bool,
}
pub struct FileTreeNative {
    root: PathBuf,
    state: Arc<Mutex<State>>,
}
/// The original controller's opaque close receipt. It retains no file or job.
pub struct FileTreeCloseReceipt {
    original: std::sync::Weak<Mutex<State>>,
    closed: u64,
}
impl FileTreeCloseReceipt {
    pub fn matches(&self, original: &FileTreeNative) -> bool {
        self.original.ptr_eq(&Arc::downgrade(&original.state))
    }
    pub fn closed_handles(&self) -> u64 {
        self.closed
    }
}
impl FileTreeNative {
    pub fn new(root: PathBuf) -> Self {
        Self {
            root,
            state: Arc::new(Mutex::new(State::default())),
        }
    }
    fn state(&self) -> MutexGuard<'_, State> {
        self.state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }
    pub fn validate_root(&self) -> io::Result<()> {
        let root = std::fs::canonicalize(&self.root)?;
        if !root.is_dir() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "file root is not a directory",
            ));
        }
        Ok(())
    }
    fn authorized(&self, path: PathBuf) -> io::Result<PathBuf> {
        let root = &self.root;
        let path = std::fs::canonicalize(path)?;
        if path.starts_with(root) {
            Ok(path)
        } else {
            Err(io::Error::new(
                io::ErrorKind::NotFound,
                "path escapes declared file root",
            ))
        }
    }
    pub fn execute(&self, operation: FileTreeOperation) -> io::Result<FileTreeOutput> {
        use std::io::{Read, Seek};
        let mut state = self.state();
        if state.retired {
            return Err(retired());
        }
        if let Some(source) = &state.fault {
            return Err(retained(source.clone()));
        }
        match operation {
            FileTreeOperation::Canonicalize(path) => {
                Ok(FileTreeOutput::Path(self.authorized(path)?))
            }
            FileTreeOperation::Metadata(path) => Ok(FileTreeOutput::Metadata(std::fs::metadata(
                self.authorized(path)?,
            )?)),
            FileTreeOperation::List(path) => {
                let path = self.authorized(path)?;
                if state.directory.is_some() {
                    return Err(io::Error::other(
                        "previous original directory remains owned",
                    ));
                }
                state.close_file()?;
                state.directory = Some(directory::Directory::new());
                let original = state.directory.as_mut().unwrap();
                let result = original
                    .acquire(&path)
                    .and_then(|()| original.entries(&path));
                let closed = state.directory.take().unwrap().close();
                state.record_directory_close(closed)?;
                result.map(FileTreeOutput::Entries)
            }
            FileTreeOperation::Open(path) => {
                state.close_file()?;
                state.file = Some(super::native::open(&self.authorized(path)?)?);
                #[cfg(windows)]
                super::windows::validate(state.file.as_ref().unwrap())?;
                let metadata = state.file.as_ref().unwrap().metadata()?;
                if !metadata.is_file() {
                    state.close_file()?;
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidInput,
                        "not a regular file",
                    ));
                }
                Ok(FileTreeOutput::Metadata(metadata))
            }
            FileTreeOperation::Read { offset, length } => {
                if length > 32768 {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidInput,
                        "file read exceeds declared chunk bound",
                    ));
                }
                let file = state
                    .file
                    .as_mut()
                    .ok_or_else(|| io::Error::other("no original file"))?;
                file.seek(std::io::SeekFrom::Start(offset))?;
                let mut bytes = vec![0; length];
                let count = file.read(&mut bytes)?;
                bytes.truncate(count);
                Ok(FileTreeOutput::Bytes(bytes))
            }
        }
    }
    /// Observation only; a count does not authorize closure or replacement.
    pub fn owned_originals(&self) -> Option<usize> {
        self.state
            .try_lock()
            .ok()
            .map(|state| usize::from(state.file.is_some()) + usize::from(state.directory.is_some()))
    }
    /// Each real native close is consumed once. Unknown remains sticky.
    pub fn close_originals(&self) -> io::Result<FileTreeCloseReceipt> {
        let mut state = self.state();
        state.retired = true;
        if let Some(directory) = state.directory.take() {
            state.record_directory_close(directory.close())?;
        }
        state.close_file()?;
        if let Some(source) = &state.fault {
            return Err(retained(source.clone()));
        }
        let count = state.closed - state.reported;
        state.reported = state.closed;
        Ok(FileTreeCloseReceipt {
            original: Arc::downgrade(&self.state),
            closed: count,
        })
    }
}
impl State {
    fn record_directory_close(&mut self, result: io::Result<bool>) -> io::Result<()> {
        match result {
            Ok(false) => Ok(()),
            Ok(true) => self.record_close(Ok(())),
            Err(source) => self.record_close(Err(source)),
        }
    }
    fn record_close(&mut self, result: io::Result<()>) -> io::Result<()> {
        match result {
            Ok(()) => match self.closed.checked_add(1) {
                Some(count) => {
                    self.closed = count;
                    Ok(())
                }
                None => {
                    let source = Arc::new(io::Error::other("file receipt counter exhausted"));
                    self.fault.get_or_insert_with(|| source.clone());
                    Err(retained(source))
                }
            },
            Err(source) => {
                let source = Arc::new(source);
                self.fault.get_or_insert_with(|| source.clone());
                Err(retained(source))
            }
        }
    }
    fn close_file(&mut self) -> io::Result<()> {
        if let Some(file) = self.file.take() {
            self.record_close(super::native::close(file))?;
        }
        Ok(())
    }
}
fn retired() -> io::Error {
    io::Error::new(io::ErrorKind::Interrupted, "file session retired")
}
fn retained(source: Arc<io::Error>) -> io::Error {
    io::Error::new(source.kind(), super::RetainedIo(source))
}

#[cfg(test)]
#[path = "../../tests/file/tree.rs"]
mod tests;
