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
        if !matches!(
            request,
            zero_api::QueryRequest::TunStatus(_)
                | zero_api::QueryRequest::Health(_)
                | zero_api::QueryRequest::Capabilities(_)
        ) {
            return None;
        }
        // Both output and row workspace outlive original view guards so every
        // failure/unwind releases the actual owners before refund/wake.
        let mut workspace = match self.budget.reserve(ControlCharge::ResponseBytes(0)) {
            Ok(original) => original,
            Err(error) => return Some(Err(tonic::Status::resource_exhausted(error.to_string()))),
        };
        let mut encoded: Option<Arc<Lease>> = None;
        let prepared = (|| -> io::Result<(Vec<u8>, usize)> {
            if matches!(request, zero_api::QueryRequest::Capabilities(_)) {
                let view = proxy.control_capabilities_query(&mut workspace)?;
                self.encode_proxy_view(&view, &mut encoded)
            } else if matches!(request, zero_api::QueryRequest::Health(_)) {
                let view = proxy.control_health_query(&mut workspace)?;
                self.encode_proxy_view(&view, &mut encoded)
            } else {
                let view = proxy
                    .control_proxy_query(request)
                    .expect("selected concrete Proxy owner");
                self.encode_proxy_view(&view, &mut encoded)
            }
        })();
        Some(match prepared {
            Ok((bytes, frame)) => {
                self.enqueue(frame, encoded.take().expect("same original Proxy output"));
                Ok(bytes)
            }
            Err(error) => Err(tonic::Status::resource_exhausted(error.to_string())),
        })
    }
    pub(super) fn encode_proxy_view(
        &self,
        view: &impl serde::Serialize,
        encoded: &mut Option<Arc<Lease>>,
    ) -> io::Result<(Vec<u8>, usize)> {
        let mut count = Count {
            bytes: 0,
            limit: self.policy.response_bytes,
        };
        serde_json::to_writer(&mut count, view).map_err(io::Error::other)?;
        let frame = super::output::frame_length(count.bytes)?;
        *encoded = Some(self.encoded(frame, ControlCharge::ResponseBytes(count.bytes))?);
        let mut bytes = Vec::new();
        bytes
            .try_reserve_exact(count.bytes)
            .map_err(io::Error::other)?;
        serde_json::to_writer(&mut bytes, view).map_err(io::Error::other)?;
        if bytes.len() != count.bytes {
            return Err(io::Error::other(
                "same original Proxy response changed during encoding",
            ));
        }
        Ok((bytes, frame))
    }
}
#[cfg(test)]
mod tests;
