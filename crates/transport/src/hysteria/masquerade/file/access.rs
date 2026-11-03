//! Existing HTTP semantics consume either their legacy I/O or the exact host
//! session. A selected owned session never retries the legacy filesystem.
use super::*;
use tokio::io::{AsyncReadExt, AsyncSeekExt};
pub(super) enum Access<'a> {
    Legacy,
    Owned(&'a mut zero_platform_tokio::file::tree::FileTreeSession),
}
impl Access<'_> {
    pub(super) async fn canonicalize(&mut self, path: PathBuf) -> io::Result<PathBuf> {
        match self {
            Self::Legacy => tokio::fs::canonicalize(path).await,
            Self::Owned(files) => files.canonicalize(path).await,
        }
    }
    pub(super) async fn metadata(&mut self, path: PathBuf) -> io::Result<std::fs::Metadata> {
        match self {
            Self::Legacy => tokio::fs::metadata(path).await,
            Self::Owned(files) => files.metadata(path).await,
        }
    }
    pub(super) async fn list(&mut self, path: PathBuf) -> io::Result<Vec<String>> {
        match self {
            Self::Owned(files) => files.list(path).await,
            Self::Legacy => {
                let mut entries = tokio::fs::read_dir(path).await?;
                let mut names = Vec::new();
                while let Some(entry) = entries.next_entry().await? {
                    let mut name = entry.file_name().to_string_lossy().into_owned();
                    if entry.file_type().await?.is_dir() {
                        name.push('/');
                    }
                    names.push(name);
                    if names.len() > 65536 {
                        return Err(io::Error::new(
                            io::ErrorKind::OutOfMemory,
                            "directory entry bound exceeded",
                        ));
                    }
                }
                Ok(names)
            }
        }
    }
    pub(super) async fn open(&mut self, path: &Path) -> io::Result<Opened<'_>> {
        match self {
            Self::Legacy => Ok(Opened::Legacy(tokio::fs::File::open(path).await?)),
            Self::Owned(files) => {
                let metadata = files.open(path.to_path_buf()).await?;
                Ok(Opened::Owned {
                    files,
                    metadata,
                    position: 0,
                })
            }
        }
    }
}
pub(super) enum Opened<'a> {
    Legacy(tokio::fs::File),
    Owned {
        files: &'a mut zero_platform_tokio::file::tree::FileTreeSession,
        metadata: std::fs::Metadata,
        position: u64,
    },
}
impl Opened<'_> {
    pub(super) async fn metadata(&self) -> io::Result<std::fs::Metadata> {
        match self {
            Self::Legacy(file) => file.metadata().await,
            Self::Owned { metadata, .. } => Ok(metadata.clone()),
        }
    }
    pub(super) async fn read(&mut self, bytes: &mut [u8]) -> io::Result<usize> {
        match self {
            Self::Legacy(file) => file.read(bytes).await,
            Self::Owned {
                files, position, ..
            } => {
                let read = files.read_at(*position, bytes.len()).await?;
                bytes[..read.len()].copy_from_slice(&read);
                *position += read.len() as u64;
                Ok(read.len())
            }
        }
    }
    pub(super) async fn seek(&mut self, position: std::io::SeekFrom) -> io::Result<()> {
        match self {
            Self::Legacy(file) => {
                file.seek(position).await?;
                Ok(())
            }
            Self::Owned {
                position: cursor, ..
            } => match position {
                std::io::SeekFrom::Start(position) => {
                    *cursor = position;
                    Ok(())
                }
                _ => Err(io::Error::other("file service uses absolute ranges")),
            },
        }
    }
}
