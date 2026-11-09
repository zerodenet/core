//! Borrow the original wire frame; never materialize a response Value tree.
use super::*;
use serde_json::value::RawValue;
use zero_runtime::configuration::{ControlBudgetPermit, ControlCharge};
mod count;
mod field;
pub(super) mod format;
mod header;
mod semantic;
mod typed;
pub(super) use count::Layout;
pub(crate) struct Reply {
    pub(super) frame: frames::Frame,
    book: Arc<ControlBudget>,
    pub(super) layout: Layout,
}
pub(super) struct Guard<T> {
    pub(super) value: T,
    _credit: ControlBudgetPermit,
}
impl<T> std::ops::Deref for Guard<T> {
    type Target = T;
    fn deref(&self) -> &T {
        &self.value
    }
}
pub(super) fn capacity() -> io::Error {
    io::Error::new(
        io::ErrorKind::WouldBlock,
        "control client decoded workspace exceeds byte budget",
    )
}
impl Reply {
    pub(super) fn new(frame: frames::Frame, book: Arc<ControlBudget>) -> io::Result<Self> {
        let scratch = count::scan(&frame.bytes)?.scratch()?;
        let credit = book.reserve(ControlCharge::ResponseBytes(scratch))?;
        match count::layout(&frame.bytes, 0) {
            Ok(layout) => {
                let raw =
                    serde_json::from_slice::<&RawValue>(&frame.bytes).map_err(io::Error::other)?;
                semantic::validate(raw, &book)?;
                drop(credit);
                Ok(Self {
                    frame,
                    book,
                    layout,
                })
            }
            Err(error) => Err(io::Error::new(
                io::ErrorKind::InvalidData,
                DecodeFailure {
                    error,
                    _frame: frame,
                    _credit: credit,
                },
            )),
        }
    }
    /// send_request has a typed RawResponse root; unknown root fields are
    /// ignored by that original API, and only id/result use Value semantics.
    pub(super) fn new_response(frame: frames::Frame, book: Arc<ControlBudget>) -> io::Result<Self> {
        let bytes = count::scan(&frame.bytes)?
            .scratch()?
            .checked_add(frame.bytes.len())
            .and_then(|n| n.checked_add(4096))
            .ok_or_else(capacity)?;
        let credit = book.reserve(ControlCharge::ResponseBytes(bytes))?;
        let result = (|| -> io::Result<Layout> {
            let header: header::Header<'_> =
                serde_json::from_slice(&frame.bytes).map_err(io::Error::other)?;
            let mut layout = Layout::default();
            for raw in [header.id, header.result].into_iter().flatten() {
                let child = count::layout(raw.get().as_bytes(), 1).map_err(io::Error::other)?;
                semantic::validate(raw, &book)?;
                layout.maps = layout.maps.max(child.maps);
                layout.depth = layout.depth.max(child.depth);
                layout.escaped = layout.escaped.max(child.escaped);
            }
            Ok(layout)
        })();
        match result {
            Ok(layout) => {
                drop(credit);
                Ok(Self {
                    frame,
                    book,
                    layout,
                })
            }
            Err(source) => Err(io::Error::new(
                io::ErrorKind::InvalidData,
                ResponseFailure {
                    source,
                    _frame: frame,
                    _credit: credit,
                },
            )),
        }
    }
    pub(super) fn raw(&self) -> io::Result<&RawValue> {
        serde_json::from_slice(&self.frame.bytes).map_err(io::Error::other)
    }
    fn header(&self) -> io::Result<Guard<header::Header<'_>>> {
        let credit = self.book.reserve(ControlCharge::ResponseBytes(
            count::scan(&self.frame.bytes)?
                .scratch()?
                .checked_add(self.frame.bytes.len())
                .ok_or_else(capacity)?,
        ))?;
        let value = match serde_json::from_slice(&self.frame.bytes) {
            Ok(value) => value,
            Err(error) => return Err(workspace_error(error, credit)),
        };
        Ok(Guard {
            value,
            _credit: credit,
        })
    }
    pub(crate) fn ensure_ok(self, fallback: &'static str) -> io::Result<Self> {
        let header = self.header()?;
        if header.ok {
            drop(header);
            return Ok(self);
        }
        let raw = header.error.map(|error| error.message);
        let size = raw.map_or(fallback.len(), |raw| raw.get().len());
        let credit = self.book.reserve(ControlCharge::ResponseBytes(
            size.checked_mul(3).ok_or_else(capacity)?,
        ))?;
        let message = match raw {
            Some(raw) => serde_json::from_str::<String>(raw.get()).map_err(io::Error::other)?,
            None => fallback.to_owned(),
        };
        drop(header);
        Err(io::Error::other(CommandFailure {
            message,
            _reply: self,
            _credit: credit,
        }))
    }
    pub(crate) fn print(&self, pretty: bool, result_only: bool) -> io::Result<()> {
        let credit = self.book.reserve(ControlCharge::ResponseBytes(
            self.layout.workspace(self.frame.bytes.len())?,
        ))?;
        let raw = if result_only {
            self.header()?.result
        } else {
            Some(self.raw()?)
        };
        if let Some(raw) = raw {
            let wire = format::Wire::new(raw, &self.book)?;
            let output = std::io::stdout();
            let mut output = output.lock();
            format::write(&mut output, &wire, pretty)?;
            std::io::Write::write_all(&mut output, b"\n")?;
            std::io::Write::flush(&mut output)?;
        }
        drop(credit);
        Ok(())
    }
}
struct DecodeFailure {
    error: serde_json::Error,
    _frame: frames::Frame,
    _credit: ControlBudgetPermit,
}
impl std::fmt::Debug for DecodeFailure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_tuple("ClientDecodeFailure")
            .field(&self.error)
            .finish()
    }
}
impl std::fmt::Display for DecodeFailure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        std::fmt::Display::fmt(&self.error, f)
    }
}
impl std::error::Error for DecodeFailure {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(&self.error)
    }
}
struct CommandFailure {
    message: String,
    _reply: Reply,
    _credit: ControlBudgetPermit,
}
impl std::fmt::Debug for CommandFailure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_tuple("ClientCommandFailure")
            .field(&self.message)
            .finish()
    }
}
impl std::fmt::Display for CommandFailure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.message)
    }
}
impl std::error::Error for CommandFailure {}

pub(crate) struct RawInput<'a> {
    wire: format::Wire<'a>,
    _credit: ControlBudgetPermit,
}
impl serde::Serialize for RawInput<'_> {
    fn serialize<S: serde::Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
        serde::Serialize::serialize(&self.wire, s)
    }
}
impl<'a> RawInput<'a> {
    pub(super) fn new(text: &'a str, book: &Arc<ControlBudget>) -> io::Result<Self> {
        let mut credit = book.reserve(ControlCharge::RequestBytes(
            count::scan(text.as_bytes())?.scratch()?,
        ))?;
        let layout = count::layout(text.as_bytes(), 0).map_err(io::Error::other)?;
        credit.grow(layout.workspace(text.len())?)?;
        let raw = serde_json::from_str(text).map_err(io::Error::other)?;
        let wire = format::Wire::new_request(raw, book)?;
        Ok(Self {
            wire,
            _credit: credit,
        })
    }
}

#[cfg(test)]
impl Reply {
    pub(super) fn test_write(&self, writer: &mut impl io::Write, pretty: bool) -> io::Result<()> {
        let credit = self.book.reserve(ControlCharge::ResponseBytes(
            self.layout.workspace(self.frame.bytes.len())?,
        ))?;
        let wire = format::Wire::new(self.raw()?, &self.book)?;
        format::write(writer, &wire, pretty)?;
        drop(credit);
        Ok(())
    }
}

pub(super) fn workspace_error(error: serde_json::Error, credit: ControlBudgetPermit) -> io::Error {
    io::Error::new(
        io::ErrorKind::InvalidData,
        WorkspaceFailure {
            error,
            _credit: credit,
        },
    )
}
struct WorkspaceFailure {
    error: serde_json::Error,
    _credit: ControlBudgetPermit,
}
impl std::fmt::Debug for WorkspaceFailure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_tuple("ClientDecodeWorkspace")
            .field(&self.error)
            .finish()
    }
}
impl std::fmt::Display for WorkspaceFailure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        std::fmt::Display::fmt(&self.error, f)
    }
}
impl std::error::Error for WorkspaceFailure {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(&self.error)
    }
}

struct ResponseFailure {
    source: io::Error,
    _frame: frames::Frame,
    _credit: ControlBudgetPermit,
}
impl std::fmt::Debug for ResponseFailure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_tuple("ClientTypedResponseFailure")
            .field(&self.source)
            .finish()
    }
}
impl std::fmt::Display for ResponseFailure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        std::fmt::Display::fmt(&self.source, f)
    }
}
impl std::error::Error for ResponseFailure {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(&self.source)
    }
}
