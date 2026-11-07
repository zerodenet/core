//! The original typed DTO is measured before allocating its single wire body.
use std::io::{self, Write};
use zero_api::{ApiError, ApiResponse, CommandResponse, QueryResponse, RawApiEvent};

pub enum HttpBody {
    Query(Result<Box<QueryResponse>, ApiError>),
    Command(ApiResponse<CommandResponse>),
    Events(ApiResponse<Vec<RawApiEvent>>),
    Error(ApiError),
    BareError(ApiError),
    Raw(&'static [u8]),
    Text(String),
}
impl HttpBody {
    pub(super) fn query(response: Result<QueryResponse, ApiError>) -> Self {
        Self::Query(response.map(Box::new))
    }
    pub(super) fn write_json(&self, writer: &mut impl Write) -> io::Result<()> {
        match self {
            Self::Query(Ok(query)) => {
                serde_json::to_writer_pretty(writer, &ApiResponse::ok(Flat(query)))
            }
            Self::Query(Err(error)) => serde_json::to_writer_pretty(
                writer,
                &crate::ipc::owner::BorrowedErrorResponse::new(None, error),
            ),
            Self::Command(value) => serde_json::to_writer_pretty(writer, value),
            Self::Events(value) => serde_json::to_writer_pretty(writer, value),
            Self::Error(value) => serde_json::to_writer_pretty(
                writer,
                &crate::ipc::owner::BorrowedErrorResponse::new(None, value),
            ),
            Self::BareError(value) => serde_json::to_writer_pretty(writer, value),
            Self::Raw(value) => return writer.write_all(value),
            Self::Text(value) => return writer.write_all(value.as_bytes()),
        }
        .map_err(io::Error::other)
    }
    pub(super) fn len(&self) -> io::Result<usize> {
        let mut count = Counter(0);
        self.write_json(&mut count)?;
        Ok(count.0)
    }
    #[cfg(test)]
    pub(super) fn bytes_for_test(&self) -> Vec<u8> {
        let mut bytes = Vec::new();
        self.write_json(&mut bytes).unwrap();
        bytes
    }
}
struct Counter(usize);
impl Write for Counter {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        self.0 = self
            .0
            .checked_add(bytes.len())
            .ok_or_else(|| io::Error::other("HTTP envelope length overflow"))?;
        Ok(bytes.len())
    }
    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}
struct Flat<'a>(&'a QueryResponse);
impl serde::Serialize for Flat<'_> {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        use serde::Serialize;
        match self.0 {
            QueryResponse::Capabilities(value) => value.serialize(serializer),
            QueryResponse::Health(value) => value.serialize(serializer),
            QueryResponse::PacketRoutes(value) => value.serialize(serializer),
            QueryResponse::PacketRoute(value) => value.serialize(serializer),
            QueryResponse::Endpoints(value) => value.serialize(serializer),
            QueryResponse::Endpoint(value) => value.serialize(serializer),
            QueryResponse::EndpointDetails(value) => value.serialize(serializer),
            QueryResponse::Config(value) => value.serialize(serializer),
            QueryResponse::Runtime(value) => value.serialize(serializer),
            QueryResponse::TrafficStat(value) => value.serialize(serializer),
            QueryResponse::TrafficStats(value) => value.serialize(serializer),
            QueryResponse::Stats(value) => value.serialize(serializer),
            QueryResponse::PrincipalFlows(value) => value.serialize(serializer),
            QueryResponse::ActiveFlows(value) => value.serialize(serializer),
            QueryResponse::RecentFlows(value) => value.serialize(serializer),
            QueryResponse::Flow(value) => value.serialize(serializer),
            QueryResponse::Policies(value) => value.serialize(serializer),
            QueryResponse::Policy(value) => value.serialize(serializer),
            QueryResponse::Diagnostics(value) => value.serialize(serializer),
            QueryResponse::Sinks(value) => value.serialize(serializer),
            QueryResponse::TunStatus(value) => value.serialize(serializer),
            QueryResponse::Unknown(value) => value.serialize(serializer),
        }
    }
}

#[cfg(test)]
mod tests;
