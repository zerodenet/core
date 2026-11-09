use super::super::{
    body::{Body, ReceivedBody},
    client::carrier,
};
use super::*;
#[derive(Clone)]
pub(in crate::split_http) struct Sender {
    pub(super) pool: XhttpClientPool,
    pub(super) factory: XhttpCarrierFactory,
    pub(super) usage: UsageRef,
    pub(super) owned: bool,
    pub(super) owner: Option<OwnerKey>,
    pub(super) logical: Option<owned::OwnedLogicalReturn>,
}
impl Sender {
    pub(super) async fn owned_original(
        &self,
    ) -> io::Result<(
        zero_traits::packet_metadata::PacketMetadataArc<owned::Runtime>,
        zero_platform_tokio::controlled_carrier::CarrierOrigin,
    )> {
        let group = self.usage.lock().unwrap().group.clone();
        let connection = group
            .connections
            .acquire_owned(&group, &self.factory)
            .await?;
        let runtime = connection
            .owned_runtime()
            .ok_or(io::ErrorKind::PermissionDenied)?;
        let origin = connection
            .owned_origin()
            .ok_or(io::ErrorKind::PermissionDenied)?;
        Ok((runtime, origin))
    }
    pub(super) async fn owned_runtime(
        &self,
    ) -> io::Result<zero_traits::packet_metadata::PacketMetadataArc<owned::Runtime>> {
        let group = self.usage.lock().unwrap().group.clone();
        let original = group
            .connections
            .acquire_owned(&group, &self.factory)
            .await?;
        original
            .owned_runtime()
            .ok_or(io::ErrorKind::PermissionDenied.into())
    }
    pub(in crate::split_http) async fn send_request(
        &mut self,
        mut request: http::Request<Body>,
    ) -> io::Result<http::Response<ReceivedBody>> {
        let group = {
            let mut usage = self.usage.lock().unwrap();
            if !usage.group.take_request() {
                *usage = self.pool.select(self.owner.as_ref())?;
                if !usage.group.take_request() {
                    return Err(io::Error::other("xhttp client group unavailable"));
                }
            }
            usage.group.clone()
        };
        let group_memory = self
            .owner
            .as_ref()
            .map(|owner| {
                owner
                    .memory
                    .reserve(group_body::OwnedGroupBody::layout())
                    .map_err(io::Error::other)
            })
            .transpose()?;
        let response_use = self.owned.then(|| group::ResponseUse::new(group.clone()));
        let mut connection = if self.owned {
            group
                .connections
                .acquire_owned(&group, &self.factory)
                .await?
        } else {
            group.connections.acquire(&group, &self.factory).await?
        };
        if matches!(connection.sender, carrier::Sender::Http2(_)) {
            let host = request
                .headers()
                .get("host")
                .and_then(|v| v.to_str().ok())
                .unwrap_or("localhost");
            *request.uri_mut() = format!("https://{host}{}", request.uri())
                .parse()
                .map_err(io::Error::other)?;
        }
        if let Some(original) = &self.logical {
            connection.sender.bind_logical(original.clone())?;
        }
        let response = match connection.sender.send_request(request).await {
            Ok(response) => response,
            Err(error) => {
                group.fail();
                return Err(error);
            }
        };
        if let (Some(memory), Some(response_use)) = (group_memory, response_use) {
            return Ok(response.map(|body| {
                ReceivedBody::OwnedGroup(group_body::OwnedGroupBody::new(
                    body,
                    connection,
                    response_use,
                    memory,
                ))
            }));
        }
        // HTTP/1 cannot be reused until its body is consumed. For H2/H3 this
        // guard only pins the shared driver; other logical streams use clones.
        Ok(response.map(|body| {
            body.guarded(move |complete| {
                if complete && matches!(connection.sender, carrier::Sender::Http1(_)) {
                    group.connections.recycle(connection);
                }
            })
        }))
    }
}
