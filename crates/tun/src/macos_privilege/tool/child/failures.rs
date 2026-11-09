//! Four actual phases of one fixed Child, not a retry/history registry.
use super::*;
use std::fmt;
struct Failures {
    phases: [Option<io::Error>; 4],
}
impl fmt::Debug for Failures {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(self, f)
    }
}
impl fmt::Display for Failures {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut first = true;
        for (phase, source) in self
            .phases
            .iter()
            .enumerate()
            .filter_map(|(phase, source)| source.as_ref().map(|source| (phase, source)))
        {
            if !first {
                f.write_str("; ")?;
            }
            first = false;
            write!(f, "fixed tool phase {phase}: {source}")?;
        }
        Ok(())
    }
}
impl std::error::Error for Failures {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        self.phases
            .iter()
            .flatten()
            .next()
            .map(|source| source as _)
    }
}
fn result(mut phases: [Option<io::Error>; 4]) -> io::Result<()> {
    match phases.iter().flatten().count() {
        0 => Ok(()),
        1 => Err(phases.iter_mut().find_map(Option::take).unwrap()),
        _ => Err(io::Error::other(Failures { phases })),
    }
}
pub(super) fn spawn_failed(source: io::Error, closed: io::Result<()>) -> io::Error {
    result([Some(source), None, closed.err(), None]).unwrap_err()
}
pub(super) fn finish(
    ends: io::Result<()>,
    collected: io::Result<(Vec<u8>, Vec<u8>)>,
    closed: io::Result<()>,
    waited: io::Result<ExitStatus>,
) -> io::Result<Output> {
    let (bytes, collect) = match collected {
        Ok(bytes) => (Some(bytes), None),
        Err(source) => (None, Some(source)),
    };
    let (status, wait) = match waited {
        Ok(status) => (Some(status), None),
        Err(source) => (None, Some(source)),
    };
    // Move all exact sources before testing the first result. A read error
    // never destroys an independently returned close/wait error or vice versa.
    result([ends.err(), collect, closed.err(), wait])?;
    let (stdout, stderr) = bytes.unwrap();
    Ok(Output {
        status: status.unwrap(),
        stdout,
        stderr,
    })
}
#[cfg(test)]
#[path = "failures/tests.rs"]
mod tests;
