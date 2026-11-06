//! Final application results, after every original service join was attempted.
use std::error::Error;

#[derive(Debug, Default)]
pub(super) struct RunFailures(Vec<(&'static str, Box<dyn Error>)>);
impl RunFailures {
    pub(super) fn error(&mut self, service: &'static str, error: Box<dyn Error>) {
        self.0.push((service, error));
    }
    pub(super) fn result<E: Error + 'static>(
        &mut self,
        service: &'static str,
        result: Result<(), E>,
    ) {
        if let Err(error) = result {
            self.error(service, Box::new(error));
        }
    }
    pub(super) fn finish(mut self) -> Result<(), Box<dyn Error>> {
        match self.0.len() {
            0 => Ok(()),
            1 => Err(self.0.remove(0).1),
            _ => Err(Box::new(self)),
        }
    }
}
impl std::fmt::Display for RunFailures {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        for (index, (service, error)) in self.0.iter().enumerate() {
            if index != 0 {
                f.write_str("; ")?;
            }
            write!(f, "{service}: {error}")?;
        }
        Ok(())
    }
}
impl Error for RunFailures {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        self.0.first().map(|(_, error)| error.as_ref())
    }
}

#[cfg(test)]
#[path = "tests/failures.rs"]
mod tests;
