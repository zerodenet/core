//! Actual old+new request storage on both ends is paid before resizing the peer.
use super::*;
use crate::packet_memory::PacketMetadataCredit;
use std::io::Write;
pub(super) struct Growth {
    capacity: usize,
    parent: Option<PacketMetadataCredit>,
    peer: Option<PacketMetadataCredit>,
    submitted: bool,
}
impl Epoch {
    pub(super) fn ensure_request_capacity(
        &mut self,
        request: &HelperRequest,
        memory: &PacketMemoryView,
    ) -> io::Result<()> {
        struct Count(usize);
        impl Write for Count {
            fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
                self.0 = self
                    .0
                    .checked_add(bytes.len())
                    .ok_or_else(|| io::Error::other("request layout overflow"))?;
                Ok(bytes.len())
            }
            fn flush(&mut self) -> io::Result<()> {
                Ok(())
            }
        }
        let mut count = Count(1); // actual newline framing
        serde_json::to_writer(&mut count, request).map_err(io::Error::other)?;
        if self.growth.is_none() && count.0 <= self.rpc.request_capacity() {
            return Ok(());
        }
        if self.growth.is_none() {
            let layout = rpc::Request::layout(count.0);
            let all = layout.checked_add(layout).map_err(io::Error::other)?;
            let mut credit = memory
                .reserve_driver_metadata(all)
                .map_err(io::Error::other)?;
            self.growth = Some(Growth {
                capacity: count.0,
                parent: Some(credit.split(layout).map_err(io::Error::other)?),
                peer: Some(credit),
                submitted: false,
            });
        }
        let growth = self.growth.as_mut().unwrap();
        if !growth.submitted {
            self.rpc.begin(
                &HelperRequest::ResizeWorkspace {
                    capacity: growth.capacity,
                },
                None,
            )?;
            growth.submitted = true;
        }
        self.drive_rpc()?;
        if !self.rpc.take_response()?.success {
            return Err(io::Error::other(
                "same helper rejected prepaid workspace growth",
            ));
        }
        let mut growth = self.growth.take().unwrap();
        self.rpc
            .replace_request(growth.capacity, growth.parent.take().unwrap())?;
        self._peer_request = growth.peer.take().unwrap(); // peer ACK means old Box freed
        if count.0 > self.rpc.request_capacity() {
            return self.ensure_request_capacity(request, memory);
        }
        Ok(())
    }
}
