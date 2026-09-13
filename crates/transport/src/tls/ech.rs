use std::future::Future;
use std::io;
use std::pin::Pin;

use base64::Engine;
use zero_traits::{ClientTlsOptions, EchConfigSource, EchForceQuery};

mod hpke;

pub type EchConfigResolveFuture =
    Pin<Box<dyn Future<Output = io::Result<Option<Vec<u8>>>> + Send + 'static>>;

/// Runtime-owned DNS ECH material provider. Implementations own cache and
/// egress policy; TLS transport only consumes the returned config-list bytes.
pub trait EchConfigResolver: Send + Sync {
    fn resolve(
        &self,
        server: String,
        query_name: String,
        force_query: EchForceQuery,
    ) -> EchConfigResolveFuture;
}

#[derive(Debug, Default)]
pub struct UnavailableEchConfigResolver;

impl EchConfigResolver for UnavailableEchConfigResolver {
    fn resolve(
        &self,
        _server: String,
        _query_name: String,
        _force_query: EchForceQuery,
    ) -> EchConfigResolveFuture {
        Box::pin(async {
            Err(io::Error::new(
                io::ErrorKind::Unsupported,
                "ECH DNS resolver is unavailable",
            ))
        })
    }
}

pub async fn prepare_options(
    options: &mut ClientTlsOptions,
    configured_server_name: Option<&str>,
    default_server_name: &str,
    resolver: &dyn EchConfigResolver,
) -> io::Result<()> {
    let Some(EchConfigSource::Dns { query_name, server }) =
        options.ech.source().map_err(invalid)?
    else {
        return Ok(());
    };
    let query_name = query_name
        .or(configured_server_name.filter(|value| !value.is_empty()))
        .unwrap_or(default_server_name);
    ztls::settings::ech_query_name(query_name).map_err(invalid)?;
    let material = resolver
        .resolve(
            server.to_owned(),
            query_name.to_owned(),
            options.ech.force_query,
        )
        .await?;
    // Some(empty) is the explicit half/none result: the DNS lookup completed,
    // and this connection must proceed without ECH.
    options.ech.prepared_config_list = Some(material.unwrap_or_default());
    Ok(())
}

pub(super) fn config_list(options: &ClientTlsOptions) -> io::Result<Option<Vec<u8>>> {
    match options.ech.source().map_err(invalid)? {
        None => Ok(None),
        Some(EchConfigSource::Static(value)) => {
            let bytes = base64::engine::general_purpose::STANDARD
                .decode(value)
                .map_err(|_| invalid("ECH config list must be valid base64"))?;
            if bytes.is_empty() {
                return Err(invalid("ECH config list must not be empty"));
            }
            Ok(Some(bytes))
        }
        Some(EchConfigSource::Dns { .. }) => match &options.ech.prepared_config_list {
            Some(bytes) if bytes.is_empty() => Ok(None),
            Some(bytes) => Ok(Some(bytes.clone())),
            None => Err(invalid(
                "DNS-backed ECH was not prepared before TLS carrier creation",
            )),
        },
    }
}

fn invalid(error: impl ToString) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidInput, error.to_string())
}

pub(super) use hpke::supported_suites;
