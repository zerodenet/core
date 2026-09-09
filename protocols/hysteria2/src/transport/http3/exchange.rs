use bytes::{Buf, Bytes};
use std::io;
use zero_transport::http_server::HttpExchange;
pub(super) struct Exchange<'a>(pub(super) &'a mut super::RequestStream);
#[async_trait::async_trait]
impl HttpExchange for Exchange<'_> {
    async fn recv_data(&mut self) -> io::Result<Option<Bytes>> {
        Ok(self
            .0
            .recv_data()
            .await
            .map_err(io::Error::other)?
            .map(|mut data| {
                let len = data.remaining();
                data.copy_to_bytes(len)
            }))
    }
    async fn send_response(&mut self, response: http::Response<()>) -> io::Result<()> {
        self.0
            .send_response(response)
            .await
            .map_err(io::Error::other)
    }
    async fn send_data(&mut self, data: Bytes) -> io::Result<()> {
        self.0.send_data(data).await.map_err(io::Error::other)
    }
    async fn finish(&mut self) -> io::Result<()> {
        self.0.finish().await.map_err(io::Error::other)
    }
}
