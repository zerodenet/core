use super::RuntimeOptionsConfig;
impl RuntimeOptionsConfig {
    pub fn effective_navigation_max_jobs(&self) -> usize {
        self.navigation_max_jobs.unwrap_or(64)
    }
    pub fn effective_navigation_max_requests(&self) -> usize {
        self.navigation_max_requests.unwrap_or(32)
    }
}
