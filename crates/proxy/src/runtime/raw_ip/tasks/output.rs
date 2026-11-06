//! Only the actual output shapes of the listener's existing child tasks.
use super::Source;
use std::sync::Arc;
use zero_engine::EngineError;
pub(crate) trait TaskOutput: Send + 'static {
    fn into_failure(self) -> Option<Source>;
    /// Retain the original typed cause for the actual Critical Scope result,
    /// while leaving the same typed output available to its original receiver.
    fn preserve_failure(&mut self) -> Option<Source>;
}
impl TaskOutput for () {
    fn preserve_failure(&mut self) -> Option<Source> {
        None
    }
    fn into_failure(self) -> Option<Source> {
        None
    }
}
impl TaskOutput for Result<(), EngineError> {
    fn preserve_failure(&mut self) -> Option<Source> {
        match std::mem::replace(self, Ok(())) {
            Ok(()) => None,
            Err(source) => {
                let source: Source = Arc::new(source);
                *self = Err(EngineError::Io(std::io::Error::other(RetainedOutput(
                    source.clone(),
                ))));
                Some(source)
            }
        }
    }
    fn into_failure(self) -> Option<Source> {
        self.err().map(|error| Arc::new(error) as Source)
    }
}
impl TaskOutput for Result<(), std::io::Error> {
    fn preserve_failure(&mut self) -> Option<Source> {
        match std::mem::replace(self, Ok(())) {
            Ok(()) => None,
            Err(source) => {
                let kind = source.kind();
                let source: Source = Arc::new(source);
                *self = Err(std::io::Error::new(kind, RetainedOutput(source.clone())));
                Some(source)
            }
        }
    }
    fn into_failure(self) -> Option<Source> {
        self.err().map(|error| Arc::new(error) as Source)
    }
}
#[derive(Debug)]
struct AssociationFailure {
    source: zero_traits::SocketAddress,
    association: u64,
    error: EngineError,
}
impl std::fmt::Display for AssociationFailure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "UDP association {} at {:?}: {}",
            self.association, self.source, self.error
        )
    }
}
impl std::error::Error for AssociationFailure {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(&self.error)
    }
}
impl TaskOutput for (zero_traits::SocketAddress, u64, Result<(), EngineError>) {
    fn preserve_failure(&mut self) -> Option<Source> {
        self.2.preserve_failure()
    }
    fn into_failure(self) -> Option<Source> {
        self.2.err().map(|error| {
            Arc::new(AssociationFailure {
                source: self.0,
                association: self.1,
                error,
            }) as Source
        })
    }
}

#[derive(Debug)]
struct RetainedOutput(Source);
impl std::fmt::Display for RetainedOutput {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.0.fmt(f)
    }
}
impl std::error::Error for RetainedOutput {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(self.0.as_ref())
    }
}
