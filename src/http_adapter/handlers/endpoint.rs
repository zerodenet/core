use std::io;
use zero_api::{EndpointGetQuery, EndpointListQuery, QueryRequest, QueryService};
use zero_proxy::ProxyHandle;

use super::{parse_query_param, serialize_query};

pub fn endpoints_list(handle: &ProxyHandle, query: &str) -> io::Result<Vec<u8>> {
    let params = (|| {
        let parse = |name| {
            parse_query_param(query, name)
                .map(|value| value.parse::<usize>())
                .transpose()
                .map_err(|_| {
                    zero_api::ApiError::new(
                        zero_api::ApiErrorCode::InvalidArgument,
                        format!("invalid endpoint pagination `{name}`"),
                    )
                })
        };
        Ok::<_, zero_api::ApiError>(EndpointListQuery {
            offset: parse("offset")?.unwrap_or_default(),
            limit: parse("limit")?,
        })
    })();
    serialize_query(params.and_then(|params| handle.query(QueryRequest::Endpoints(params))))
}

pub fn endpoint_get(handle: &ProxyHandle, endpoint_id: &str, details: bool) -> io::Result<Vec<u8>> {
    let id = percent_encoding::percent_decode_str(endpoint_id)
        .decode_utf8()
        .map_err(|_| {
            zero_api::ApiError::new(
                zero_api::ApiErrorCode::InvalidArgument,
                "endpoint ID is not valid UTF-8",
            )
        });
    serialize_query(id.and_then(|id| {
        let query = EndpointGetQuery {
            endpoint_id: id.into_owned(),
        };
        handle.query(if details {
            QueryRequest::EndpointDetails(query)
        } else {
            QueryRequest::Endpoint(query)
        })
    }))
}
