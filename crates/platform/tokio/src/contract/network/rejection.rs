use std::{error::Error, fmt, io};

/// A provider received an explicit negative response to its setup request.
/// Preserve the provider's diagnostic. This fact grants no retry, close,
/// registration release or permission to deliver a carrier.
#[derive(Debug)]
pub struct UpstreamRejection(Box<dyn Error + Send + Sync>);

impl UpstreamRejection {
    pub fn new(source: impl Error + Send + Sync + 'static) -> Self {
        Self(Box::new(source))
    }

    pub(crate) fn present_in(error: &dyn Error) -> bool {
        let mut source = error.source();
        for _ in 0..64 {
            let Some(current) = source else { return false };
            if current.is::<Self>() {
                return true;
            }
            // io::Error::source skips its immediate payload in some wrappers.
            source = current
                .downcast_ref::<io::Error>()
                .and_then(|error| error.get_ref())
                .map(|inner| inner as &(dyn Error + 'static))
                .or_else(|| current.source());
        }
        false
    }
}

impl fmt::Display for UpstreamRejection {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(f)
    }
}

impl Error for UpstreamRejection {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        Some(self.0.as_ref())
    }
}
