use std::{
    ffi::{c_int, c_void},
    io,
    sync::OnceLock,
};

use openssl::{
    ex_data::Index,
    ssl::{SslContext, SslContextBuilder, SslVersion},
};
use openssl_sys::{SSL, SSL_CTX};

use super::{openssl_cipher_name, openssl_group_name, openssl_version};

pub(super) fn apply(
    builder: &mut SslContextBuilder,
    parameters: &zero_traits::TlsParameters,
) -> io::Result<()> {
    let mut min = openssl_version(&parameters.min_version, SslVersion::TLS1_2)?;
    let max = openssl_version(&parameters.max_version, SslVersion::TLS1_3)?;
    let configured_min =
        ztls::settings::version(&parameters.min_version, 0x303).map_err(io::Error::other)?;
    let configured_max =
        ztls::settings::version(&parameters.max_version, 0x304).map_err(io::Error::other)?;
    let tls13 = [
        "TLS_AES_128_GCM_SHA256",
        "TLS_AES_256_GCM_SHA384",
        "TLS_CHACHA20_POLY1305_SHA256",
    ];
    let mut legacy = Vec::new();
    if parameters.cipher_suites.is_empty() {
        legacy.extend([
            "ECDHE-ECDSA-AES128-GCM-SHA256",
            "ECDHE-RSA-AES128-GCM-SHA256",
            "ECDHE-ECDSA-CHACHA20-POLY1305",
            "ECDHE-RSA-CHACHA20-POLY1305",
            "ECDHE-ECDSA-AES256-GCM-SHA384",
            "ECDHE-RSA-AES256-GCM-SHA384",
            "ECDHE-ECDSA-AES128-SHA",
            "ECDHE-RSA-AES128-SHA",
            "ECDHE-ECDSA-AES256-SHA",
            "ECDHE-RSA-AES256-SHA",
        ]);
    } else {
        for name in &parameters.cipher_suites {
            let suite = openssl_cipher_name(name)
                .ok_or_else(|| invalid("TLS cipher suite unavailable in OpenSSL"))?;
            if matches!(
                name.as_str(),
                "TLS_AES_128_GCM_SHA256"
                    | "TLS_AES_256_GCM_SHA384"
                    | "TLS_CHACHA20_POLY1305_SHA256"
            ) {
                // Go exposes these identifiers but does not make TLS 1.3
                // cipher suites configurable. Keep its three safe defaults.
            } else {
                legacy.push(suite);
            }
        }
        if legacy.is_empty() {
            if configured_max < 0x304 {
                return Err(invalid(
                    "configured TLS cipher suites exclude every enabled protocol version",
                ));
            }
            if configured_min < 0x304 {
                min = SslVersion::TLS1_3;
            }
        }
    }
    builder
        .set_min_proto_version(Some(min))
        .map_err(io::Error::other)?;
    builder
        .set_max_proto_version(Some(max))
        .map_err(io::Error::other)?;
    if ztls::settings::uses_legacy_versions(parameters).map_err(io::Error::other)? {
        allow_legacy_protocol_versions(builder)?;
    }
    builder
        .set_ciphersuites(&tls13.join(":"))
        .map_err(io::Error::other)?;
    if !legacy.is_empty() {
        builder
            .set_cipher_list(&legacy.join(":"))
            .map_err(io::Error::other)?;
    }
    if !parameters.curve_preferences.is_empty() {
        let groups = parameters
            .curve_preferences
            .iter()
            .map(|name| {
                openssl_group_name(name).ok_or_else(|| invalid("TLS group unavailable in OpenSSL"))
            })
            .collect::<io::Result<Vec<_>>>()?;
        builder
            .set_groups_list(&groups.join(":"))
            .map_err(io::Error::other)?;
    }
    Ok(())
}

struct SecurityPolicy {
    previous: Option<super::ffi::SecurityCallback>,
    previous_extra: usize,
}

static SECURITY_POLICY_INDEX: OnceLock<Index<SslContext, Box<SecurityPolicy>>> = OnceLock::new();

fn allow_legacy_protocol_versions(builder: &mut SslContextBuilder) -> io::Result<()> {
    builder.set_security_level(1);
    let previous = unsafe { super::ffi::SSL_CTX_get_security_callback(builder.as_ptr()) };
    let previous_extra =
        unsafe { super::ffi::SSL_CTX_get0_security_ex_data(builder.as_ptr()) } as usize;
    let index = match SECURITY_POLICY_INDEX.get() {
        Some(index) => *index,
        None => {
            let created = SslContext::new_ex_index().map_err(io::Error::other)?;
            let _ = SECURITY_POLICY_INDEX.set(created);
            *SECURITY_POLICY_INDEX
                .get()
                .expect("OpenSSL security policy index was initialized")
        }
    };
    let policy = Box::new(SecurityPolicy {
        previous,
        previous_extra,
    });
    let policy_pointer = (&*policy as *const SecurityPolicy).cast_mut().cast();
    // The inner allocation remains stable when the context takes ownership.
    builder.set_ex_data(index, policy);
    unsafe {
        super::ffi::SSL_CTX_set_security_callback(builder.as_ptr(), Some(legacy_security_callback));
        super::ffi::SSL_CTX_set0_security_ex_data(builder.as_ptr(), policy_pointer);
    }
    Ok(())
}

unsafe extern "C" fn legacy_security_callback(
    ssl: *const SSL,
    context: *const SSL_CTX,
    operation: c_int,
    bits: c_int,
    nid: c_int,
    other: *mut c_void,
    extra: *mut c_void,
) -> c_int {
    const SSL_SECOP_VERSION: c_int = 9;
    const SSL_SECOP_SIGALG_SUPPORTED: c_int = 11 | (5 << 16);
    const SSL_SECOP_SIGALG_SHARED: c_int = 12 | (5 << 16);
    const SSL_SECOP_SIGALG_CHECK: c_int = 13 | (5 << 16);
    const TLS1_VERSION: c_int = 0x0301;
    const TLS1_1_VERSION: c_int = 0x0302;
    if operation == SSL_SECOP_VERSION && matches!(nid, TLS1_VERSION | TLS1_1_VERSION) {
        return 1;
    }
    let version = if ssl.is_null() {
        0
    } else {
        openssl_sys::SSL_version(ssl)
    };
    if matches!(version, TLS1_VERSION | TLS1_1_VERSION)
        && matches!(
            operation,
            SSL_SECOP_SIGALG_SUPPORTED | SSL_SECOP_SIGALG_SHARED | SSL_SECOP_SIGALG_CHECK
        )
        && matches!(nid, openssl_sys::NID_sha1 | openssl_sys::NID_md5_sha1)
        && matches!(bits, 64 | 67)
        && !other.is_null()
    {
        let signature = std::slice::from_raw_parts(other.cast::<u8>(), 2);
        if matches!(signature, [0, 0] | [2, 1] | [2, 3]) {
            return 1;
        }
    }
    let Some(policy) = (extra as *const SecurityPolicy).as_ref() else {
        return 0;
    };
    let Some(previous) = policy.previous else {
        return 0;
    };
    previous(
        ssl,
        context,
        operation,
        bits,
        nid,
        other,
        policy.previous_extra as *mut c_void,
    )
}

fn invalid(message: impl Into<String>) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidInput, message.into())
}
