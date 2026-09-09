use std::io::{self, Read, Write};
use std::net::{Shutdown, TcpStream};
use std::time::Duration;

pub(super) const TLS_ALERT: [u8; 7] = [21, 3, 3, 0, 2, 2, 40];

pub(super) fn respond(stream: &mut TcpStream) -> io::Result<usize> {
    // Windows inherits the listener's nonblocking mode on accept. Timeouts
    // alone do not switch the accepted socket back to blocking I/O.
    stream.set_nonblocking(false)?;
    stream.set_read_timeout(Some(Duration::from_secs(2)))?;
    stream.set_write_timeout(Some(Duration::from_secs(2)))?;
    let mut header = [0_u8; 5];
    stream.read_exact(&mut header)?;
    let size = usize::from(u16::from_be_bytes([header[3], header[4]]));
    if header[0] != 22 || size == 0 || size > 18_432 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "invalid test TLS record",
        ));
    }
    let mut payload = vec![0; size];
    stream.read_exact(&mut payload)?;
    // Only acknowledge a complete record; never send a success marker after
    // WouldBlock, timeout, EOF, or a partial read.
    stream.write_all(&TLS_ALERT)?;
    stream.shutdown(Shutdown::Write)?;
    Ok(header.len() + size)
}

#[cfg(test)]
#[path = "tls_responder_tests.rs"]
mod tests;
