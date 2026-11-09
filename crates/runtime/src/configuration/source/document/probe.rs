//! Inert IO-budget facts only, not configuration acceptance. No protocol keys,
//! AST, decoder result, callback, IO grant or second Source. Real serde remains
//! authoritative after rewind of this same original readonly inode.
use std::io::{self, Read};
mod scan;
mod string;
use scan::{Key, Scan};
#[derive(Clone, Copy)]
pub(super) enum Dialect {
    Config,
    Value,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) struct Facts {
    pub file: usize,
    pub request: usize,
}
impl Default for Facts {
    fn default() -> Self {
        let actual = zero_config::RuntimeOptionsConfig::default();
        Self {
            file: actual.effective_file_read_max_bytes(),
            request: actual.effective_control_max_in_flight_request_bytes(),
        }
    }
}
#[derive(Debug)]
pub(super) enum Unknown {
    Structure,
    Serde(serde_json::Error),
    Budget(serde_json::Error),
}
impl std::fmt::Display for Unknown {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Structure => f.write_str("initial document IO-budget fact is unknown"),
            Self::Serde(source) | Self::Budget(source) => std::fmt::Display::fmt(source, f),
        }
    }
}
impl std::error::Error for Unknown {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Serde(source) | Self::Budget(source) => Some(source),
            _ => None,
        }
    }
}
fn invalid() -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, Unknown::Structure)
}
fn unknown(source: serde_json::Error) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, Unknown::Serde(source))
}
fn budget_unknown(source: serde_json::Error) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, Unknown::Budget(source))
}
fn deferred(error: &io::Error) -> bool {
    error
        .get_ref()
        .and_then(|source| source.downcast_ref::<Unknown>())
        .is_some_and(|source| matches!(source, Unknown::Budget(_)))
}
pub(super) fn is_unknown(error: &io::Error) -> bool {
    error.get_ref().is_some_and(|source| source.is::<Unknown>())
}
pub(super) fn read(reader: &mut dyn Read, dialect: Dialect) -> io::Result<Facts> {
    let mut input = Scan::new(reader);
    if matches!(dialect, Dialect::Config) {
        input.bom()?;
    }
    let facts = object(&mut input, dialect, false, 0)?;
    input.ws()?;
    if input.peek()?.is_some() {
        return Err(invalid());
    }
    Ok(facts)
}
fn object(
    input: &mut Scan<'_>,
    dialect: Dialect,
    runtime: bool,
    depth: usize,
) -> io::Result<Facts> {
    // A magic string needs another layer of JSON escaping. More layers than the
    // input address space can represent cannot originate in this same document.
    if depth >= usize::BITS as usize {
        return Err(invalid());
    }
    input.ws()?;
    if input.peek()? != Some(b'{') {
        input.skip()?;
        return Ok(Facts::default());
    }
    input.take()?;
    input.ws()?;
    if input.peek()? == Some(b'}') {
        input.take()?;
        return Ok(Facts::default());
    }
    let mut key = input.key()?;
    input.colon()?;
    if matches!(dialect, Dialect::Value) && key == Key::Magic {
        input.ws()?;
        if input.take()? != Some(b'"') {
            return Err(invalid());
        }
        let mut decoded = string::Decoded::new(input);
        let mut child = Scan::new(&mut decoded);
        let facts = object(&mut child, dialect, runtime, depth + 1);
        if facts.as_ref().is_err_and(|source| !deferred(source)) {
            return facts;
        }
        child.ws()?;
        if child.peek()?.is_some() {
            return Err(invalid());
        }
        drop(child);
        if !decoded.done() {
            return Err(invalid());
        }
        drop(decoded);
        input.ws()?;
        if input.take()? != Some(b'}') {
            return Err(invalid());
        }
        return facts;
    }
    let mut facts = Facts::default();
    let (mut runtime_error, mut file_error, mut request_error) = (None, None, None);
    loop {
        match (runtime, key) {
            (false, Key::Runtime) => match object(input, dialect, true, depth + 1) {
                Ok(actual) => {
                    facts = actual;
                    runtime_error = None;
                }
                Err(source) if deferred(&source) => runtime_error = Some(source),
                Err(source) => return Err(source),
            },
            (true, Key::File) => match bound_value(input, dialect, false, depth + 1) {
                Ok(actual) => {
                    facts.file = actual.unwrap_or_else(|| Facts::default().file);
                    file_error = None;
                }
                Err(source) if deferred(&source) => file_error = Some(source),
                Err(source) => return Err(source),
            },
            (true, Key::Request) => match bound_value(input, dialect, true, depth + 1) {
                Ok(actual) => {
                    facts.request = actual.unwrap_or_else(|| Facts::default().request);
                    request_error = None;
                }
                Err(source) if deferred(&source) => request_error = Some(source),
                Err(source) => return Err(source),
            },
            _ => input.skip()?,
        }
        input.ws()?;
        match input.take()? {
            Some(b'}') => break,
            Some(b',') => {
                key = input.key()?;
                input.colon()?;
            }
            _ => return Err(invalid()),
        }
    }
    if let Some(source) = runtime_error.or(file_error).or(request_error) {
        Err(source)
    } else {
        Ok(facts)
    }
}

fn bound_value(
    input: &mut Scan<'_>,
    dialect: Dialect,
    request: bool,
    depth: usize,
) -> io::Result<Option<usize>> {
    if depth >= usize::BITS as usize {
        return Err(invalid());
    }
    input.ws()?;
    if matches!(dialect, Dialect::Value) && input.peek()? == Some(b'{') {
        input.take()?;
        input.ws()?;
        if input.peek()? == Some(b'}') {
            input.take()?;
            return Err(non_integer());
        }
        let first = input.key()?;
        input.colon()?;
        if first != Key::Magic {
            // This is an ordinary Value object, not an unsigned fact. Finish
            // it without allocation so a later duplicate field can win.
            input.skip()?;
            loop {
                input.ws()?;
                match input.take()? {
                    Some(b'}') => return Err(non_integer()),
                    Some(b',') => {
                        input.key()?;
                        input.colon()?;
                        input.skip()?;
                    }
                    _ => return Err(invalid()),
                }
            }
        }
        input.ws()?;
        if input.take()? != Some(b'"') {
            return Err(invalid());
        }
        let mut decoded = string::Decoded::new(input);
        let mut child = Scan::new(&mut decoded);
        let fact = bound_value(&mut child, dialect, request, depth + 1);
        if fact.as_ref().is_err_and(|source| !deferred(source)) {
            return fact;
        }
        child.ws()?;
        if child.peek()?.is_some() {
            return Err(invalid());
        }
        drop(child);
        if !decoded.done() {
            return Err(invalid());
        }
        drop(decoded);
        input.ws()?;
        if input.take()? != Some(b'}') {
            return Err(invalid());
        }
        return fact;
    }
    input.bound(request)
}

fn non_integer() -> io::Error {
    budget_unknown(<serde_json::Error as serde::de::Error>::custom(
        "initial IO-budget fact is not an unsigned integer or null",
    ))
}
