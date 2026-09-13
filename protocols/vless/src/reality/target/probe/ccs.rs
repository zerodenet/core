use super::{client_config, io::connect_target, AlpnClass};
use crate::reality::target::Profile;
use std::io;
use std::time::Duration;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use zero_transport::handshake_target::Connector;

pub(super) const CCS_RECORD: &[u8] = &[20, 3, 3, 0, 1, 1];
const CCS_RESPONSE_WINDOW: Duration = Duration::from_secs(1);

pub(super) async fn accepts_ccs(
    profile: &Profile,
    connector: &Connector,
    server_name: &str,
    alpn: AlpnClass,
) -> io::Result<usize> {
    drive_ccs_probe(
        connect_target(profile, connector).await?,
        client_config(server_name, alpn),
    )
    .await
}

pub(super) async fn drive_ccs_probe<S>(
    mut target: S,
    config: ztls::handshake::Tls13Config,
) -> io::Result<usize>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    let mut connection = ztls::handshake::Tls13Connection::new(config)?;
    let mut injected = false;
    loop {
        if connection.wants_write() {
            let mut output = Vec::new();
            connection.write_tls(&mut output)?;
            if !injected && starts_client_finished_flight(&output) {
                let mut measured_limit = None;
                for (count, accepted_before_stage) in [(2, 1), (15, 16), (16, 32)] {
                    for _ in 0..count {
                        target.write_all(CCS_RECORD).await?;
                    }
                    if rejected_ccs(&mut target).await? {
                        measured_limit = Some(accepted_before_stage);
                        break;
                    }
                }
                injected = true;
                if let Some(limit) = measured_limit {
                    let _ = target.write_all(&output).await;
                    return Ok(limit);
                }
            }
            target.write_all(&output).await?;
        }
        connection.process_new_packets()?;
        if !connection.is_handshaking() && !connection.wants_write() {
            if !injected {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "CCS probe did not observe the client Finished flight",
                ));
            }
            break;
        }
        if connection.wants_write() {
            continue;
        }
        let mut input = [0; 8192];
        let count = target.read(&mut input).await?;
        if count == 0 {
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "target closed during CCS probe",
            ));
        }
        connection.read_tls(&mut &input[..count])?;
    }
    Ok(usize::MAX)
}

pub(super) fn starts_client_finished_flight(output: &[u8]) -> bool {
    output.len() >= 5 && output[1..3] == [3, 3] && matches!(output[0], 20 | 23)
}

async fn rejected_ccs<S>(target: &mut S) -> io::Result<bool>
where
    S: AsyncRead + Unpin,
{
    let mut response = [0; 512];
    match tokio::time::timeout(CCS_RESPONSE_WINDOW, target.read(&mut response)).await {
        Err(_) => Ok(false),
        Ok(Ok(0)) => Err(io::Error::new(
            io::ErrorKind::UnexpectedEof,
            "target closed during CCS probe",
        )),
        Ok(Err(error)) => Err(error),
        Ok(Ok(count)) => Ok(response[..count].starts_with(&[21, 3, 3])),
    }
}
