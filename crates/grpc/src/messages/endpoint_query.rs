//! The actual production Proxy endpoint view, before payload/Prost allocation.
use super::{output::Lease, Record};
use std::{io, sync::Arc};
use zero_runtime::configuration::ControlCharge;
enum Failure<'a> {
    Query(zero_proxy::ProxyEndpointQueryError<'a>),
    Capacity(io::Error),
}
struct QueryError<'a>(zero_proxy::ProxyEndpointQueryError<'a>);
pub(crate) enum PreparedProxyQuery<'a> {
    Ready(Result<Vec<u8>, tonic::Status>),
    QueryError(zero_proxy::ProxyEndpointQueryError<'a>),
}
impl PreparedProxyQuery<'_> {
    pub(crate) async fn finish(self, original: &Record) -> Result<Vec<u8>, tonic::Status> {
        match self {
            Self::Ready(result) => result,
            Self::QueryError(error) => Err(original
                .error_status(tonic::Code::Internal, "", &QueryError(error))
                .await),
        }
    }
}
impl std::fmt::Display for QueryError<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}: {}", self.0.code().as_code_str(), self.0.message())
    }
}
impl Record {
    pub(crate) fn actual_proxy_query<'a>(
        &self,
        proxy: &'a zero_proxy::ProxyHandle,
        request: &'a zero_api::QueryRequest,
    ) -> Option<PreparedProxyQuery<'a>> {
        if let Some(result) = self.endpoint_query(proxy, request) {
            return Some(result);
        }
        self.proxy_query(proxy, request)
            .map(PreparedProxyQuery::Ready)
    }
    fn endpoint_query<'a>(
        &self,
        proxy: &'a zero_proxy::ProxyHandle,
        request: &'a zero_api::QueryRequest,
    ) -> Option<PreparedProxyQuery<'a>> {
        if !matches!(
            request,
            zero_api::QueryRequest::Endpoints(_)
                | zero_api::QueryRequest::Endpoint(_)
                | zero_api::QueryRequest::EndpointDetails(_)
        ) {
            return None;
        }
        let mut workspace = match self.budget.reserve(ControlCharge::ResponseBytes(0)) {
            Ok(original) => original,
            Err(error) => {
                return Some(PreparedProxyQuery::Ready(Err(
                    tonic::Status::resource_exhausted(error.to_string()),
                )))
            }
        };
        let mut encoded: Option<Arc<Lease>> = None;
        let prepared = (|| -> Result<(Vec<u8>, usize), Failure<'_>> {
            let guard = proxy
                .control_endpoint_query(request)
                .expect("selected concrete endpoint owner")
                .map_err(Failure::Query)?;
            let view = guard.freeze(&mut workspace).map_err(Failure::Capacity)?;
            self.encode_proxy_view(&view, &mut encoded)
                .map_err(Failure::Capacity)
        })();
        drop(workspace);
        Some(match prepared {
            Ok((bytes, frame)) => {
                self.enqueue(
                    frame,
                    encoded.take().expect("same original endpoint envelope"),
                );
                PreparedProxyQuery::Ready(Ok(bytes))
            }
            Err(Failure::Query(error)) => PreparedProxyQuery::QueryError(error),
            Err(Failure::Capacity(error)) => {
                PreparedProxyQuery::Ready(Err(tonic::Status::resource_exhausted(error.to_string())))
            }
        })
    }
}

#[cfg(test)]
mod tests;
