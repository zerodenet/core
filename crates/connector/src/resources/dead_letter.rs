//! Same V1 dead-letter encoding, with its original File retained by the dispatcher.
use super::*;
use zero_api::{ApiError, ApiErrorCode, ApiResult, EventSink, PublishResult, RawApiEvent};
pub(crate) struct RetainedDeadLetter {
    writer: Mutex<FileView>,
}
impl RetainedDeadLetter {
    pub(crate) fn new(
        path: &std::path::Path,
        resources: &DispatcherResources,
    ) -> ApiResult<Arc<Self>> {
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent).map_err(failure)?;
        }
        let file = resources
            .open(|| {
                std::fs::OpenOptions::new()
                    .create(true)
                    .append(true)
                    .open(path)
            })
            .map_err(failure)?;
        let original = Arc::new(Self {
            writer: Mutex::new(file),
        });
        resources
            .0
            .dead_letters
            .lock()
            .unwrap()
            .push(original.clone());
        Ok(original)
    }
    pub(crate) fn into_inner_recover(self) -> Result<FileView, (FileView, ApiError)> {
        self.writer.into_inner().map_err(|error| {
            (
                error.into_inner(),
                failure("original dead-letter writer lock poisoned"),
            )
        })
    }
}
fn failure(error: impl std::fmt::Display) -> ApiError {
    ApiError::new(ApiErrorCode::Internal, error.to_string())
}
impl EventSink for RetainedDeadLetter {
    fn name(&self) -> &str {
        "dead-letter"
    }
    fn publish(&self, event: &RawApiEvent) -> ApiResult<PublishResult> {
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64;
        let envelope = serde_json::json!({"dead_lettered_at_unix_ms":now,"original_event":event});
        let mut line = serde_json::to_vec(&envelope).map_err(failure)?;
        line.push(b'\n');
        let mut writer = self
            .writer
            .lock()
            .map_err(|_| failure("dead-letter lock poisoned"))?;
        writer.write_all(&line).map_err(failure)?;
        // Preserve the public DeadLetterSink's historical flush behavior.
        writer.flush().ok();
        Ok(PublishResult::delivered())
    }
}
