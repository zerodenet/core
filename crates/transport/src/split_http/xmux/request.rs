use super::super::{
    body::{Body, ReceivedBody},
    client::carrier,
};
use super::*;
#[derive(Clone)]
pub(in crate::split_http) struct Sender {
    pub(super) pool: XhttpClientPool,
    pub(super) factory: OwnedXhttpCarrierFactory,
    pub(super) usage: UsageRef,
    pub(super) owned_kind: Option<super::owned_connect::OwnedConnectionKind>,
    pub(super) owner: Option<OwnerKey>,
    pub(super) logical: Option<owned::OwnedLogicalReturn>,
}
impl Sender {
    pub(in crate::split_http) fn bind_logical(&mut self, original: owned::OwnedLogicalReturn) {
        self.logical = Some(original);
    }

    pub(in crate::split_http) async fn send_request(
        &mut self,
        mut request: http::Request<Body>,
    ) -> io::Result<http::Response<ReceivedBody>> {
        let streaming = request
            .extensions()
            .get::<super::super::request::StreamRequest>()
            .is_none_or(|kind| kind.0);
        let (group, response_use) = {
            let mut usage = self.usage.lock().unwrap();
            if !usage.group.take_request() {
                *usage = self.pool.select(self.owner.as_ref())?;
                if !usage.group.take_request() {
                    return Err(io::Error::other("xhttp client group unavailable"));
                }
            }
            let group = usage.group.clone();
            let response_use = self
                .owned_kind
                .map(|_| group::ResponseUse::new(group.clone(), streaming));
            (group, response_use)
        };
        // Publish this actual response custody before collecting exhausted
        // groups: consuming the last request must not evict its selected idle
        // original before the new session has published its driver work.
        // Reap the previous group's returned Usage outside its mutex.
        self.pool.reap();
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
        if self.owned_kind.is_some() {
            self.pool
                .wait_rotating_responses(&group, self.owner.as_ref())
                .await;
        }
        let mut connection = if let Some(kind) = self.owned_kind {
            group
                .connections
                .acquire_owned(&group, &self.factory, kind)
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
            connection.bind_logical(original.clone())?;
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
                    group,
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
