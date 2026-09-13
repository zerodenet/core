mod certificates;
mod client;
mod context;
mod ech;
mod ffi;
mod ocsp;
mod parameters;
pub(crate) mod quic;
mod server;
mod stream;

pub(crate) use client::{connect, use_openssl_client};
pub(crate) use context::use_openssl_server;
pub(crate) use context::OpenSslServerContext;
pub use context::TlsAcceptor;
pub(crate) use stream::OpenSslTlsStream;

fn openssl_version(
    value: &str,
    default: openssl::ssl::SslVersion,
) -> std::io::Result<openssl::ssl::SslVersion> {
    Ok(match value {
        "" => default,
        "1.0" => openssl::ssl::SslVersion::TLS1,
        "1.1" => openssl::ssl::SslVersion::TLS1_1,
        "1.2" => openssl::ssl::SslVersion::TLS1_2,
        "1.3" => openssl::ssl::SslVersion::TLS1_3,
        _ => {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "unsupported TLS version",
            ))
        }
    })
}

fn openssl_cipher_name(name: &str) -> Option<&'static str> {
    Some(match name {
        "TLS_AES_128_GCM_SHA256" => "TLS_AES_128_GCM_SHA256",
        "TLS_AES_256_GCM_SHA384" => "TLS_AES_256_GCM_SHA384",
        "TLS_CHACHA20_POLY1305_SHA256" => "TLS_CHACHA20_POLY1305_SHA256",
        "TLS_ECDHE_ECDSA_WITH_AES_128_GCM_SHA256" => "ECDHE-ECDSA-AES128-GCM-SHA256",
        "TLS_ECDHE_ECDSA_WITH_AES_256_GCM_SHA384" => "ECDHE-ECDSA-AES256-GCM-SHA384",
        "TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256" => "ECDHE-RSA-AES128-GCM-SHA256",
        "TLS_ECDHE_RSA_WITH_AES_256_GCM_SHA384" => "ECDHE-RSA-AES256-GCM-SHA384",
        "TLS_ECDHE_ECDSA_WITH_CHACHA20_POLY1305_SHA256"
        | "TLS_ECDHE_ECDSA_WITH_CHACHA20_POLY1305" => "ECDHE-ECDSA-CHACHA20-POLY1305",
        "TLS_ECDHE_RSA_WITH_CHACHA20_POLY1305_SHA256" | "TLS_ECDHE_RSA_WITH_CHACHA20_POLY1305" => {
            "ECDHE-RSA-CHACHA20-POLY1305"
        }
        "TLS_ECDHE_ECDSA_WITH_AES_128_CBC_SHA" => "ECDHE-ECDSA-AES128-SHA",
        "TLS_ECDHE_ECDSA_WITH_AES_256_CBC_SHA" => "ECDHE-ECDSA-AES256-SHA",
        "TLS_ECDHE_RSA_WITH_AES_128_CBC_SHA" => "ECDHE-RSA-AES128-SHA",
        "TLS_ECDHE_RSA_WITH_AES_256_CBC_SHA" => "ECDHE-RSA-AES256-SHA",
        _ => return None,
    })
}

fn openssl_group_name(name: &str) -> Option<&'static str> {
    Some(match name {
        "X25519" | "x25519" => "X25519",
        "P256" | "p256" | "secp256r1" | "CurveP256" | "curvep256" => "P-256",
        "P384" | "p384" | "secp384r1" | "CurveP384" | "curvep384" => "P-384",
        "P521" | "p521" | "secp521r1" | "CurveP521" | "curvep521" => "P-521",
        "X25519MLKEM768" | "x25519mlkem768" => "X25519MLKEM768",
        "SecP256r1MLKEM768" | "secp256r1mlkem768" => "SecP256r1MLKEM768",
        "SecP384r1MLKEM1024" | "secp384r1mlkem1024" => "SecP384r1MLKEM1024",
        _ => return None,
    })
}
