use std::error::Error;

#[derive(Debug)]
pub(super) struct ControlStartupFailure {
    pub(super) startup: Box<dyn Error>,
    pub(super) cleanup: std::io::Error,
}
impl std::fmt::Display for ControlStartupFailure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{}; original control cleanup also failed: {}",
            self.startup, self.cleanup
        )
    }
}
impl Error for ControlStartupFailure {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        Some(self.startup.as_ref())
    }
}
