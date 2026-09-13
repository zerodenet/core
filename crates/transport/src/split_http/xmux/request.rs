use super::super::{
    body::{Body, ReceivedBody},
    client::carrier,
};
use super::*;
#[derive(Clone)]
pub(in crate::split_http) struct Sender {
    pub(super) pool: XhttpClientPool,
    pub(super) factory: XhttpCarrierFactory,
    pub(super) usage: Arc<Mutex<Usage>>,
}
impl Sender {
    pub(in crate::split_http) async fn send_request(
        &mut self,
        mut request: http::Request<Body>,
    ) -> io::Result<http::Response<ReceivedBody>> {
        let group = {
            let mut usage = self.usage.lock().unwrap();
            if !usage.group.take_request() {
                *usage = self.pool.select(self.factory.clone());
                if !usage.group.take_request() {
                    return Err(io::Error::other("xhttp client group unavailable"));
                }
            }
            usage.group.clone()
        };
        let mut connection = group.connections.acquire(&group).await?;
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
        let response = match connection.sender.send_request(request).await {
            Ok(response) => response,
            Err(error) => {
                group.fail();
                return Err(error);
            }
        };
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
