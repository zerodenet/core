//! Stream retained causes only when diagnostics are enabled, without copying them.
use std::{error::Error, fmt};

pub(crate) struct Sources<'a>(pub(crate) &'a dyn Error);

impl fmt::Display for Sources<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut source = self.0.source();
        for index in 0..16 {
            let Some(actual) = source else { return Ok(()) };
            if index != 0 {
                f.write_str("; caused by: ")?;
            }
            write!(f, "{actual}")?;
            source = actual.source();
        }
        if source.is_some() {
            f.write_str("; further causes omitted")?;
        }
        Ok(())
    }
}

#[cfg(test)]
#[path = "sources/tests.rs"]
mod tests;
