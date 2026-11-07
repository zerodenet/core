//! The same actual Proxy status guard as HTTP/IPC, before output allocation.
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
            .filter(|size| *size <= self.limit)
            .ok_or_else(|| io::Error::other("gRPC response exceeds its admitted message limit"))?;
        Ok(bytes.len())
    }
    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}
impl Record {
    pub(crate) fn proxy_query(
        &self,
        proxy: &zero_proxy::ProxyHandle,
        request: &zero_api::QueryRequest,
    ) -> Option<Result<Vec<u8>, tonic::Status>> {
        if !matches!(request, zero_api::QueryRequest::TunStatus(_)) {
            return None;
        }
        // This view has no heap workspace. The actual output permit is outside
        // all owner guards so every failure/unwind releases them before refund.
        let mut encoded: Option<Arc<Lease>> = None;
        let prepared = (|| -> io::Result<(Vec<u8>, usize)> {
            let view = proxy
                .control_proxy_query(request)
                .expect("selected concrete Proxy owner");
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
                    "same guarded Proxy response changed during encoding",
                ));
            }
            Ok((bytes, frame))
        })();
        Some(match prepared {
            Ok((bytes, frame)) => {
                self.enqueue(frame, encoded.take().expect("same original Proxy output"));
                Ok(bytes)
            }
            Err(error) => Err(tonic::Status::resource_exhausted(error.to_string())),
        })
    }
}
#[cfg(test)]
mod tests;
