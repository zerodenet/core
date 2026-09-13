use super::*;

pub(crate) async fn perform_reality_server_handshake<IO>(
    session: &mut RealityServerConnection,
    io: &mut IO,
) -> io::Result<()>
where
    IO: AsyncRead + AsyncWrite + Unpin,
{
    let mut read_buf = vec![0_u8; ztls::common::TLS_MAX_RECORD_SIZE].into_boxed_slice();

    while session.is_handshaking() || session.wants_write() {
        if session.wants_write() {
            let mut write_buf = Vec::new();
            while session.wants_write() {
                session.write_tls(&mut write_buf)?;
            }
            if !write_buf.is_empty() {
                io.write_all(&write_buf).await?;
                io.flush().await?;
            }
        }

        if session.is_handshaking() && session.wants_read() {
            let read = io.read(&mut read_buf).await?;
            if read == 0 {
                return Err(io::Error::new(
                    io::ErrorKind::UnexpectedEof,
                    "EOF during Reality server handshake",
                ));
            }
            feed_reality_server_connection(session, &read_buf[..read])?;
            session.process_new_packets()?;
        }
    }

    io.flush().await
}

pub(crate) async fn perform_reality_handshake<IO>(
    session: &mut RealityClientConnection,
    io: &mut IO,
) -> io::Result<()>
where
    IO: AsyncRead + AsyncWrite + Unpin,
{
    let mut read_buf = vec![0_u8; ztls::common::TLS_MAX_RECORD_SIZE].into_boxed_slice();

    while session.is_handshaking() || session.wants_write() {
        if session.wants_write() {
            let mut write_buf = Vec::new();
            while session.wants_write() {
                session.write_tls(&mut write_buf)?;
            }
            if !write_buf.is_empty() {
                io.write_all(&write_buf).await?;
                io.flush().await?;
            }
        }

        if session.is_handshaking() && session.wants_read() {
            let read = io.read(&mut read_buf).await?;
            if read == 0 {
                return Err(io::Error::new(
                    io::ErrorKind::UnexpectedEof,
                    "EOF during Reality handshake",
                ));
            }

            feed_reality_connection(session, &read_buf[..read])?;
            session.process_new_packets()?;
        }
    }

    io.flush().await
}

fn feed_reality_connection(session: &mut RealityClientConnection, data: &[u8]) -> io::Result<()> {
    let mut cursor = io::Cursor::new(data);
    let mut consumed = 0;
    while consumed < data.len() {
        let read = session.read_tls(&mut cursor)?;
        if read == 0 {
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "Reality handshake input was not fully consumed",
            ));
        }
        consumed += read;
    }

    Ok(())
}

pub(crate) fn feed_reality_server_connection(
    session: &mut RealityServerConnection,
    data: &[u8],
) -> io::Result<()> {
    let mut cursor = io::Cursor::new(data);
    let mut consumed = 0;
    while consumed < data.len() {
        let read = session.read_tls(&mut cursor)?;
        if read == 0 {
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "Reality server handshake input was not fully consumed",
            ));
        }
        consumed += read;
    }

    Ok(())
}
