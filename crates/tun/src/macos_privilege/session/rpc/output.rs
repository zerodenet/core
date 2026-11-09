//! Same original result claims; no new helper or foreign buffer fallback.
use super::*;
impl Rpc {
    pub(in crate::macos_privilege::session) fn bind_output_storage(
        &mut self,
        storage: [crate::packet_memory::PacketMetadataArc<crate::macos_privilege::command::inline::Storage>;
            2],
    ) {
        self.result_storage = Some(storage);
    }
    pub(in crate::macos_privilege::session) fn output_storage(
        &self,
    ) -> io::Result<
        &[crate::packet_memory::PacketMetadataArc<crate::macos_privilege::command::inline::Storage>;
             2],
    > {
        self.result_storage
            .as_ref()
            .ok_or_else(|| io::Error::other("Source output storage was not installed"))
    }
    pub(in crate::macos_privilege::session) fn cancel_prepared(
        &mut self,
        nonce: u64,
    ) -> io::Result<()> {
        if self.command != Some(nonce) || self.written != 0 {
            return Err(io::Error::other("cannot cancel submitted command framing"));
        }
        self.receiving = None;
        self.started = false;
        Ok(())
    }
    pub(in crate::macos_privilege::session) fn take_command_output(
        &mut self,
    ) -> io::Result<crate::macos_privilege::command::CommandOutput> {
        let receiving = self
            .receiving
            .take()
            .ok_or_else(|| io::Error::other("same command response is missing"))?;
        self.started = false;
        receiving.take()
    }
    pub(in crate::macos_privilege::session) fn discard_response(&mut self) -> io::Result<()> {
        if self.receiving.is_some() {
            self.take_command_output().map(drop)
        } else {
            self.take_response().map(drop)
        }
    }
}
