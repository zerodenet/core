//! Encode the initial snapshot once from its real guards into the final frame.
use super::Record;
use std::{io, sync::Arc};
use zero_runtime::configuration::ControlCharge;
struct Count {
    bytes: usize,
    limit: usize,
}
impl io::Write for Count {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        self.bytes = self
            .bytes
            .checked_add(bytes.len())
            .filter(|n| *n <= self.limit)
            .ok_or_else(|| {
                io::Error::other("initial snapshot exceeds its admitted message limit")
            })?;
        Ok(bytes.len())
    }
    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}
impl Record {
    pub(crate) fn initial_snapshot(
        self: &Arc<Self>,
        engine: &zero_engine::EngineHandle,
    ) -> io::Result<crate::budget::QueuedEvent> {
        // Workspace credit is outside all Engine guards. The final actual
        // protobuf/raw/encoded ticket is admitted atomically before allocation.
        let mut workspace = self.budget.reserve(ControlCharge::ResponseBytes(0))?;
        let mut encoded = None;
        let prepared = (|| -> io::Result<(crate::PbEvent, usize)> {
            let guard = engine.inner().control_flow_snapshot();
            let view = guard.freeze(&mut workspace)?;
            let mut payload = Count {
                bytes: 0,
                limit: self.policy.response_bytes,
            };
            serde_json::to_writer(&mut payload, &view.payload()).map_err(io::Error::other)?;
            let mut id = Count {
                bytes: 0,
                limit: self.policy.response_bytes,
            };
            use io::Write;
            id.write_fmt(format_args!("{}", view.event_id()))?;
            let frame = crate::budget::fields_frame_len(
                view.event_type(),
                id.bytes,
                view.occurred_at_unix_ms(),
                payload.bytes,
            )?;
            encoded = Some(self.encoded(frame, ControlCharge::QueuedEventBytes(frame))?);
            let mut bytes = Vec::new();
            bytes
                .try_reserve_exact(payload.bytes)
                .map_err(io::Error::other)?;
            serde_json::to_writer(&mut bytes, &view.payload()).map_err(io::Error::other)?;
            let mut event_id = String::new();
            event_id
                .try_reserve_exact(id.bytes)
                .map_err(io::Error::other)?;
            use std::fmt::Write as _;
            write!(&mut event_id, "{}", view.event_id()).expect("String format is infallible");
            if bytes.len() != payload.bytes || event_id.len() != id.bytes {
                return Err(io::Error::other(
                    "same original initial snapshot changed during encoding",
                ));
            }
            Ok((
                crate::PbEvent {
                    event_type: view.event_type().to_owned(),
                    event_id,
                    occurred_at: view.occurred_at_unix_ms(),
                    payload: bytes,
                },
                frame,
            ))
        })();
        drop(workspace);
        let (event, frame) = prepared?;
        Ok(crate::budget::QueuedEvent {
            event: Ok(event),
            _charge: None,
            encoded: Some((
                self.clone(),
                frame,
                encoded.take().expect("same original snapshot output"),
            )),
        })
    }
}
