//! Inbound credential grammar, shared by validation-only config consumers.
use super::{validate_cipher, validate_password, validate_user_count, CipherKind};
use alloc::{collections::BTreeSet, string::String, vec::Vec};

pub fn validate_inbound_users<'a>(
    cipher: &str,
    password: &str,
    identity: Option<&str>,
    users: impl IntoIterator<Item = &'a str>,
) -> Result<(), String> {
    let method = validate_cipher(cipher)?;
    let users = users.into_iter().collect::<Vec<_>>();
    validate_user_count(cipher, users.len())?;
    if let Some(identity) = identity {
        if !matches!(
            method,
            CipherKind::Blake3Aes128Gcm | CipherKind::Blake3Aes256Gcm
        ) {
            return Err("`shadowsocks.identity_password` requires a 2022 AES cipher".into());
        }
        if !password.is_empty() {
            return Err(
                "`shadowsocks` inbound cannot configure both `password` and `identity_password`"
                    .into(),
            );
        }
        single_key(cipher, identity, "identity_password")?;
    }
    if users.is_empty() {
        // Empty native registries remain disabled. An explicit user with an
        // empty v1 password represents that valid upstream credential.
        return if password.is_empty() || identity.is_some() {
            Ok(())
        } else {
            single_key(cipher, password, "single-user password")
        };
    }
    if !password.is_empty() {
        return Err("`shadowsocks` inbound cannot configure both `password` and `users`".into());
    }
    if method.is_blake3() && identity.is_none() {
        return Err("`shadowsocks` 2022 multi-user inbound requires `identity_password` as the SIP023 server identity PSK".into());
    }
    let mut passwords = BTreeSet::new();
    for user in users {
        single_key(cipher, user, "user password")?;
        if identity == Some(user) {
            return Err(
                "`shadowsocks` SIP023 server identity PSK must differ from every user PSK".into(),
            );
        }
        if !passwords.insert(user) {
            return Err("`shadowsocks` inbound contains duplicate user password".into());
        }
    }
    Ok(())
}

fn single_key(cipher: &str, password: &str, field: &str) -> Result<(), String> {
    if validate_cipher(cipher)?.is_blake3() && password.contains(':') {
        return Err(alloc::format!(
            "`shadowsocks` {field} must contain exactly one 2022 PSK"
        ));
    }
    validate_password(cipher, password)
}
