use super::*;
use tokio::time::timeout_at;
impl HostResources {
    /// Drive actual joins; all results remain in FileReads before this branch
    /// yields. Native cleanup runs alongside this branch under the host owner.
    pub(in super::super) async fn drive_files(&self, deadline: Instant) {
        while matches!(timeout_at(deadline, self.next_file_exit()).await, Ok(true)) {}
    }
}
