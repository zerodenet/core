/// Listener-scoped HTTP upload/download session registry.
#[derive(Clone)]
pub struct SplitHttpRegistry {
    pub(super) sessions: super::sessions::Sessions,
    pub(super) requests: std::sync::Arc<tokio::sync::Semaphore>,
}
impl SplitHttpRegistry {
    pub fn new() -> Self {
        Self::default()
    }
}

impl Default for SplitHttpRegistry {
    fn default() -> Self {
        Self {
            sessions: Default::default(),
            requests: std::sync::Arc::new(tokio::sync::Semaphore::new(128)),
        }
    }
}
