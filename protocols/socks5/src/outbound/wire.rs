use crate::shared::{
    read_address, read_exact, write_address, CMD_CONNECT, METHOD_NO_AUTH, METHOD_USERNAME_PASSWORD,
    REP_ADDRESS_TYPE_NOT_SUPPORTED, REP_COMMAND_NOT_SUPPORTED, REP_CONNECTION_NOT_ALLOWED,
    REP_GENERAL_FAILURE, REP_HOST_UNREACHABLE, REP_SUCCEEDED, SOCKS5_VERSION,
    USERPASS_STATUS_SUCCESS, USERPASS_VERSION,
};
use crate::{validate_credential_part, Socks5OutboundAuth};
use alloc::{vec, vec::Vec};
use zero_core::{Address, Error};
use zero_traits::AsyncSocket;

#[derive(Clone, Copy)]
pub(crate) enum Progress {
    Negotiating,
    Authenticating,
    Submitting,
    Submitted,
    Confirmed,
}

pub(crate) async fn connect_target<S: AsyncSocket>(
    stream: &mut S,
    target: &Address,
    port: u16,
    auth: Option<Socks5OutboundAuth<'_>>,
) -> Result<(), Error> {
    negotiate_auth(stream, auth).await?;
    let request = build_connect_request(target, port)?;
    submit(stream, &request, &mut |_| {}).await
}
pub(crate) async fn connect_prepared<S: AsyncSocket, O: FnMut(Progress) + Send>(
    stream: &mut S,
    request: &[u8],
    auth: Option<Socks5OutboundAuth<'_>>,
    mut observe: O,
) -> Result<(), Error> {
    negotiate_validated_auth(stream, auth, &mut observe).await?;
    submit(stream, request, &mut observe).await
}
async fn submit<S: AsyncSocket, O: FnMut(Progress) + Send>(
    stream: &mut S,
    request: &[u8],
    observe: &mut O,
) -> Result<(), Error> {
    observe(Progress::Submitting);
    stream
        .write_all(request)
        .await
        .map_err(|_| Error::Io("failed to write SOCKS5 outbound connect request"))?;
    observe(Progress::Submitted);
    let _ = read_response(stream).await?;
    observe(Progress::Confirmed);
    Ok(())
}
pub(crate) fn build_connect_request(target: &Address, port: u16) -> Result<Vec<u8>, Error> {
    if port == 0 {
        return Err(Error::Config("target port is required"));
    }
    let mut request = vec![SOCKS5_VERSION, CMD_CONNECT, 0x00];
    write_address(&mut request, target)?;

    request.extend_from_slice(&port.to_be_bytes());

    Ok(request)
}

pub(super) async fn negotiate_auth<S>(
    stream: &mut S,
    auth: Option<Socks5OutboundAuth<'_>>,
) -> Result<(), Error>
where
    S: AsyncSocket,
{
    if let Some(auth) = auth {
        validate_outbound_auth(auth)?;
    }
    negotiate_validated_auth(stream, auth, &mut |_| {}).await
}
async fn negotiate_validated_auth<S: AsyncSocket, O: FnMut(Progress) + Send>(
    stream: &mut S,
    auth: Option<Socks5OutboundAuth<'_>>,
    observe: &mut O,
) -> Result<(), Error> {
    observe(Progress::Negotiating);
    match auth {
        Some(_) => {
            stream
                .write_all(&[SOCKS5_VERSION, 0x01, METHOD_USERNAME_PASSWORD])
                .await
                .map_err(|_| Error::Io("failed to write SOCKS5 outbound auth negotiation"))?;
        }
        None => {
            stream
                .write_all(&[SOCKS5_VERSION, 0x01, METHOD_NO_AUTH])
                .await
                .map_err(|_| Error::Io("failed to write SOCKS5 outbound auth negotiation"))?;
        }
    }

    let mut selected = [0_u8; 2];
    read_exact(stream, &mut selected).await?;
    if selected[0] != SOCKS5_VERSION {
        return Err(Error::Protocol("invalid SOCKS5 outbound auth version"));
    }

    match (auth, selected[1]) {
        (None, METHOD_NO_AUTH) => Ok(()),
        (Some(auth), METHOD_USERNAME_PASSWORD) => {
            observe(Progress::Authenticating);
            authenticate_username_password(stream, auth).await
        }
        _ => Err(Error::Unsupported(
            "SOCKS5 upstream auth method is not supported",
        )),
    }
}

async fn authenticate_username_password<S>(
    stream: &mut S,
    auth: Socks5OutboundAuth<'_>,
) -> Result<(), Error>
where
    S: AsyncSocket,
{
    let username = auth.username.as_bytes();
    let password = auth.password.as_bytes();
    let mut request = Vec::with_capacity(3 + username.len() + password.len());
    request.push(USERPASS_VERSION);
    request.push(username.len() as u8);
    request.extend_from_slice(username);
    request.push(password.len() as u8);
    request.extend_from_slice(password);
    stream
        .write_all(&request)
        .await
        .map_err(|_| Error::Io("failed to write SOCKS5 username/password credentials"))?;

    let mut response = [0_u8; 2];
    read_exact(stream, &mut response).await?;
    if response[0] != USERPASS_VERSION {
        return Err(Error::Protocol(
            "invalid SOCKS5 username/password auth response version",
        ));
    }
    if response[1] != USERPASS_STATUS_SUCCESS {
        return Err(Error::Unsupported(
            "SOCKS5 upstream username/password authentication failed",
        ));
    }
    Ok(())
}

fn validate_outbound_auth(auth: Socks5OutboundAuth<'_>) -> Result<(), Error> {
    validate_credential_part(auth.username, "username")?;
    validate_credential_part(auth.password, "password")
}

pub(super) async fn read_response<S>(stream: &mut S) -> Result<(Address, u16), Error>
where
    S: AsyncSocket,
{
    let mut header = [0_u8; 4];
    read_exact(stream, &mut header).await?;

    if header[0] != SOCKS5_VERSION {
        return Err(Error::Protocol("invalid SOCKS5 outbound response version"));
    }

    if header[1] != REP_SUCCEEDED {
        return Err(match header[1] {
            REP_GENERAL_FAILURE => Error::Route("SOCKS5 upstream general failure"),
            REP_CONNECTION_NOT_ALLOWED => Error::Route("SOCKS5 upstream rejected connection"),
            REP_HOST_UNREACHABLE => Error::Route("SOCKS5 upstream host unreachable"),
            REP_COMMAND_NOT_SUPPORTED => {
                Error::Unsupported("SOCKS5 upstream command is not supported")
            }
            REP_ADDRESS_TYPE_NOT_SUPPORTED => {
                Error::Unsupported("SOCKS5 upstream address type is not supported")
            }
            _ => Error::Protocol("SOCKS5 upstream returned an unknown reply"),
        });
    }

    let address = read_address(stream, header[3]).await?;

    let mut port = [0_u8; 2];
    read_exact(stream, &mut port).await?;

    Ok((address, u16::from_be_bytes(port)))
}
