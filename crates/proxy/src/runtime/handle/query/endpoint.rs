use super::super::model::ProxyHandle;
use zero_api::{ApiError, ApiErrorCode, EndpointDetailsSnapshot, QueryRequest, QueryResponse};

impl ProxyHandle {
    pub(super) fn query_endpoint(
        &self,
        request: &QueryRequest,
    ) -> Option<zero_api::ApiResult<QueryResponse>> {
        match request {
            QueryRequest::Endpoints(query) => {
                let snapshot = self.proxy.engine().runtime_snapshot();
                let mut list = self.proxy.engine().endpoints_snapshot_in(&snapshot, query);
                for endpoint in &mut list.endpoints {
                    self.proxy
                        .protocols
                        .observe_endpoint(snapshot.config(), endpoint);
                }
                Some(Ok(QueryResponse::Endpoints(list)))
            }
            QueryRequest::Endpoint(query) | QueryRequest::EndpointDetails(query) => Some(
                self.query_one_endpoint(query, matches!(request, QueryRequest::EndpointDetails(_))),
            ),
            _ => None,
        }
    }

    fn query_one_endpoint(
        &self,
        query: &zero_api::EndpointGetQuery,
        details: bool,
    ) -> zero_api::ApiResult<QueryResponse> {
        let snapshot = self.proxy.engine().runtime_snapshot();
        let mut endpoint = self.proxy.engine().endpoint_snapshot_in(&snapshot, query)?;
        let observed = self
            .proxy
            .protocols
            .observe_endpoint(snapshot.config(), &mut endpoint);
        if !details {
            return Ok(QueryResponse::Endpoint(endpoint));
        }
        let (schema_id, schema_version, details) =
            observed.and_then(|facts| facts.details).ok_or_else(|| {
                ApiError::new(
                    ApiErrorCode::Unsupported,
                    "endpoint protocol has no registered detail observer",
                )
            })?;
        Ok(QueryResponse::EndpointDetails(EndpointDetailsSnapshot {
            endpoint_id: endpoint.endpoint_id,
            generation: endpoint.generation,
            schema_id,
            schema_version,
            details,
        }))
    }
}
