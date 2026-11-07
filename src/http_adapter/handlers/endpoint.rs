use super::parse_query_param;
use zero_api::{EndpointGetQuery, EndpointListQuery, QueryRequest};
pub(in crate::http_adapter) fn endpoint_list_request(
    query: &str,
) -> Result<QueryRequest, &'static str> {
    let parse = |name| {
        parse_query_param(query, name)
            .map(|value| value.parse::<usize>())
            .transpose()
            .map_err(|_| {
                if name == "offset" {
                    "invalid endpoint pagination `offset`"
                } else {
                    "invalid endpoint pagination `limit`"
                }
            })
    };
    Ok(QueryRequest::Endpoints(EndpointListQuery {
        offset: parse("offset")?.unwrap_or_default(),
        limit: parse("limit")?,
    }))
}
pub(in crate::http_adapter) fn endpoint_get_request(
    endpoint_id: &str,
    details: bool,
) -> Result<QueryRequest, &'static str> {
    let id = percent_encoding::percent_decode_str(endpoint_id)
        .decode_utf8()
        .map_err(|_| "endpoint ID is not valid UTF-8")?;
    let query = EndpointGetQuery {
        endpoint_id: id.into_owned(),
    };
    Ok(if details {
        QueryRequest::EndpointDetails(query)
    } else {
        QueryRequest::Endpoint(query)
    })
}
