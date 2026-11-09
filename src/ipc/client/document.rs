//! The one CLI source document, on its actual application book. The existing
//! Source owns its original FD; bytes/permission stay outside its IO mutex.
use super::*;
use std::{io::Read, path::Path};
use zero_runtime::configuration::{ConfigurationSource, ControlBudgetPermit, ControlCharge};
pub(crate) struct Document {
    text: String,
    _credit: ControlBudgetPermit,
}
impl AsRef<str> for Document {
    fn as_ref(&self) -> &str {
        &self.text
    }
}
pub(super) fn load(
    source: &Arc<ConfigurationSource>,
    path: &Path,
    book: &Arc<ControlBudget>,
) -> io::Result<Document> {
    let maximum = book.limits().messages.request_bytes_total;
    // These owners are installed outside with_file before any native read; all
    // data/permission destruction (including read/close error) is lock-external.
    let mut credit = None;
    let mut bytes = Vec::new();
    let (_, read) = source.inspect_seekable_file_snapshot(path, |length, reader| {
        let length = usize::try_from(length).map_err(|_| capacity())?;
        if length > maximum {
            return Err(capacity());
        }
        credit = Some(book.reserve(ControlCharge::RequestBytes(length))?);
        bytes.try_reserve_exact(length).map_err(io::Error::other)?;
        bytes.resize(length, 0);
        let mut used = 0;
        while used < length {
            let n = reader.read(&mut bytes[used..])?;
            if n == 0 {
                break;
            }
            used += n;
        }
        bytes.truncate(used); // its original capacity remains paid until real free
        Ok(())
    })?;
    read?; // outer Source already consumed matching OriginalFile close receipt
    let credit = credit.ok_or_else(|| io::Error::other("original document credit absent"))?;
    match String::from_utf8(bytes) {
        Ok(text) => Ok(Document {
            text,
            _credit: credit,
        }),
        Err(source) => Err(io::Error::new(
            io::ErrorKind::InvalidData,
            Utf8Document {
                source,
                _credit: credit,
            },
        )),
    }
}
fn capacity() -> io::Error {
    io::Error::new(
        io::ErrorKind::WouldBlock,
        "CLI source document exceeds its actual total byte capacity",
    )
}
struct Utf8Document {
    source: std::string::FromUtf8Error,
    _credit: ControlBudgetPermit,
}
impl std::fmt::Debug for Utf8Document {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_tuple("ClientDocumentUtf8")
            .field(&self.source)
            .finish()
    }
}
impl std::fmt::Display for Utf8Document {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        std::fmt::Display::fmt(&self.source, f)
    }
}
impl std::error::Error for Utf8Document {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(&self.source)
    }
}
#[cfg(test)]
#[path = "document/tests.rs"]
mod tests;
