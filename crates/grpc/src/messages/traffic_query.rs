//! Same synchronous traffic owner/epoch/activity projection as HTTP and IPC.
use super::{output::Lease, Record};
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
            .filter(|bytes| *bytes <= self.limit)
            .ok_or_else(|| io::Error::other("gRPC response exceeds its admitted message limit"))?;
        Ok(bytes.len())
    }
    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}
enum Failure {
    Query(zero_engine::TrafficQueryError),
    Capacity(io::Error),
}
struct QueryError(zero_engine::TrafficQueryError);
impl std::fmt::Display for QueryError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}: {}", self.0.code().as_code_str(), self.0.message())
    }
}
impl Record {
    pub(crate) async fn traffic_query(
        &self,
        engine: &zero_engine::EngineHandle,
        request: &zero_api::QueryRequest,
    ) -> Option<Result<Vec<u8>, tonic::Status>> {
        if !matches!(
            request,
            zero_api::QueryRequest::TrafficStat(_) | zero_api::QueryRequest::TrafficStats(_)
        ) {
            return None;
        }
        // Actual output/workspace custody outlives every original guard,
        // including on error/unwind. No await or refund inside owner guards.
        let mut workspace = match self.budget.reserve(ControlCharge::ResponseBytes(0)) {
            Ok(credit) => credit,
            Err(error) => return Some(Err(tonic::Status::resource_exhausted(error.to_string()))),
        };
        let mut encoded: Option<Arc<Lease>> = None;
        let prepared = (|| -> Result<(Vec<u8>, usize), Failure> {
            let guard = engine
                .inner()
                .control_traffic_query(request)
                .expect("selected concrete traffic producer")
                .map_err(Failure::Query)?;
            let view = guard.freeze(&mut workspace).map_err(Failure::Capacity)?;
            let mut count = Count {
                bytes: 0,
                limit: self.policy.response_bytes,
            };
            serde_json::to_writer(&mut count, &view)
                .map_err(|error| Failure::Capacity(io::Error::other(error)))?;
            let frame = super::output::frame_length(count.bytes).map_err(Failure::Capacity)?;
            encoded = Some(
                self.encoded(frame, ControlCharge::ResponseBytes(count.bytes))
                    .map_err(Failure::Capacity)?,
            );
            let mut bytes = Vec::new();
            bytes
                .try_reserve_exact(count.bytes)
                .map_err(|error| Failure::Capacity(io::Error::other(error)))?;
            serde_json::to_writer(&mut bytes, &view)
                .map_err(|error| Failure::Capacity(io::Error::other(error)))?;
            if bytes.len() != count.bytes {
                return Err(Failure::Capacity(io::Error::other(
                    "same frozen traffic response changed during encoding",
                )));
            }
            Ok((bytes, frame))
        })();
        drop(workspace);
        Some(match prepared {
            Ok((bytes, frame)) => {
                self.enqueue(frame, encoded.take().expect("same actual traffic output"));
                Ok(bytes)
            }
            Err(Failure::Query(error)) => Err(self
                .error_status(tonic::Code::Internal, "", &QueryError(error))
                .await),
            Err(Failure::Capacity(error)) => {
                Err(tonic::Status::resource_exhausted(error.to_string()))
            }
        })
    }
}
