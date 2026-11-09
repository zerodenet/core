//! Remote disposition is emitted from the real stored utun original only.
use super::*;
impl Server {
    pub(super) fn create(
        &mut self,
        stream: &mut UnixStream,
        name: Option<&str>,
        transfer: Option<u64>,
    ) -> io::Result<()> {
        if self
            .utun
            .as_ref()
            .is_some_and(|original| !original.closed())
        {
            return Err(io::Error::other(
                "previous helper utun original is not closed",
            ));
        }
        self.utun = None;
        let fd = match crate::Utun::open_created(name) {
            Ok(fd) => fd,
            Err(source) => return failure(stream, transfer, 1, 0, source),
        };
        let id = transfer.unwrap_or(0);
        self.utun = Some(Original::new(fd.into_raw_fd(), id));
        let original = self.utun.as_mut().unwrap();
        if let Err(source) = crate::Utun::initialize_created(original.fd(), name) {
            let close = original.close_once();
            let errno = close
                .as_ref()
                .err()
                .map_or(0, |source| source.raw_os_error().unwrap_or(libc::EIO));
            return failure(
                stream,
                transfer,
                if errno == 0 { 2 } else { 3 },
                errno,
                source,
            );
        }
        let stdout = transfer.map(|id| disposition(id, 0, 0)).unwrap_or_default();
        write_helper_response(
            stream,
            &HelperResponse {
                success: true,
                code: 0,
                stdout,
                stderr: Vec::new(),
            },
        )?;
        // Exact one-byte ancillary send. If it fails the same stored Original
        // remains available for explicit cleanup and actual helper exit.
        send_fd(stream.as_raw_fd(), original.fd())?;
        match transfer {
            Some(_) => original.finish(stream),
            None => original.close_once(),
        }
    }
}
fn disposition(id: u64, kind: u8, errno: i32) -> Vec<u8> {
    let mut bytes = Vec::with_capacity(13);
    bytes.extend_from_slice(&id.to_be_bytes());
    bytes.push(kind);
    bytes.extend_from_slice(&errno.to_be_bytes());
    bytes
}
fn failure(
    stream: &mut UnixStream,
    transfer: Option<u64>,
    kind: u8,
    errno: i32,
    source: io::Error,
) -> io::Result<()> {
    write_helper_response(
        stream,
        &HelperResponse {
            success: false,
            code: source.raw_os_error().unwrap_or(libc::EIO),
            stdout: transfer
                .map(|id| disposition(id, kind, errno))
                .unwrap_or_default(),
            stderr: source.to_string().into_bytes(),
        },
    )
}
