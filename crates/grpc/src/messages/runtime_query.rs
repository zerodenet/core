//! Freeze mutable Engine rows once, with prepaid actual workspace and output.
use super::{output::Lease, Record};
use std::{io, sync::Arc};
use zero_runtime::configuration::{ControlBudgetPermit, ControlCharge};

struct Count {
    bytes: usize,
    limit: usize,
}
impl io::Write for Count {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        self.bytes = self
            .bytes
            .checked_add(bytes.len())
            .filter(|size| *size <= self.limit)
            .ok_or_else(|| io::Error::other("gRPC response exceeds its admitted message limit"))?;
        Ok(bytes.len())
    }
    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}
enum Prepared {
    Json(Vec<u8>, usize),
    Missing(String),
}
impl Record {
    pub(crate) fn runtime_query(
        &self,
        engine: &zero_engine::EngineHandle,
        request: &zero_api::QueryRequest,
    ) -> Option<Result<Vec<u8>, tonic::Status>> {
        if !matches!(
            request,
            zero_api::QueryRequest::Runtime(_)
                | zero_api::QueryRequest::Stats(_)
                | zero_api::QueryRequest::Diagnostics(_)
                | zero_api::QueryRequest::ActiveFlows(_)
                | zero_api::QueryRequest::RecentFlows(_)
                | zero_api::QueryRequest::Flow(_)
                | zero_api::QueryRequest::PrincipalFlows(_)
                | zero_api::QueryRequest::Sinks(_)
                | zero_api::QueryRequest::PacketRoutes(_)
                | zero_api::QueryRequest::PacketRoute(_)
        ) {
            return None;
        }
        // Every credit is outside all Engine guards, including on failure or
        // panic. Never await another refund while holding this own workspace.
        let mut workspace = match self.budget.reserve(ControlCharge::ResponseBytes(0)) {
            Ok(credit) => credit,
            Err(error) => return Some(Err(capacity(error))),
        };
        let mut encoded: Option<Arc<Lease>> = None;
        let mut status: Option<ControlBudgetPermit> = None;
        let prepared = (|| -> io::Result<Prepared> {
            let guard = engine
                .inner()
                .control_runtime_query(request)
                .expect("selected concrete Engine producer");
            let selected = guard.freeze(&mut workspace)?;
            match selected {
                Ok(view) => {
                    let mut count = Count {
                        bytes: 0,
                        limit: self.policy.response_bytes,
                    };
                    serde_json::to_writer(&mut count, &view).map_err(io::Error::other)?;
                    let frame = super::output::frame_length(count.bytes)?;
                    encoded = Some(self.encoded(frame, ControlCharge::ResponseBytes(count.bytes))?);
                    let mut bytes = Vec::new();
                    bytes
                        .try_reserve_exact(count.bytes)
                        .map_err(io::Error::other)?;
                    serde_json::to_writer(&mut bytes, &view).map_err(io::Error::other)?;
                    if bytes.len() != count.bytes {
                        return Err(io::Error::other(
                            "same frozen Engine response changed during encoding",
                        ));
                    }
                    Ok(Prepared::Json(bytes, frame))
                }
                Err(missing) => {
                    let size = missing
                        .message_len()
                        .and_then(|size| size.checked_add("not_found: ".len()))
                        .filter(|size| *size <= self.policy.response_bytes)
                        .ok_or_else(|| {
                            io::Error::other("gRPC error exceeds its admitted message limit")
                        })?;
                    let bound = size
                        .checked_mul(10)
                        .and_then(|size| size.checked_add(super::CODEC_BUFFER_SIZE))
                        .ok_or_else(|| io::Error::other("gRPC error storage overflow"))?;
                    status = Some(self.budget.reserve(ControlCharge::ResponseBytes(bound))?);
                    let mut message = String::new();
                    message.try_reserve_exact(size).map_err(io::Error::other)?;
                    use std::fmt::Write;
                    write!(&mut message, "not_found: {}", missing.message())
                        .expect("String formatting is infallible");
                    Ok(Prepared::Missing(message))
                }
            }
        })();
        // At this point every original registry/history/stats/row guard and
        // its actual allocated workspace Vec has been returned synchronously.
        drop(workspace);
        Some(match prepared {
            Ok(Prepared::Json(bytes, frame)) => {
                self.enqueue(frame, encoded.take().expect("same encoded output owner"));
                Ok(bytes)
            }
            Ok(Prepared::Missing(message)) => {
                let previous = self
                    .status
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner)
                    .replace(status.take().expect("same actual status credit"));
                drop(previous);
                Err(tonic::Status::internal(message))
            }
            Err(error) => Err(capacity(error)),
        })
    }
}
fn capacity(error: io::Error) -> tonic::Status {
    // Unary admission refuses an unavailable total. In particular it cannot
    // wait for the same request's temporary row workspace to refund itself.
    tonic::Status::resource_exhausted(error.to_string())
}
