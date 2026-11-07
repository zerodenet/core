//! Existing token comparison borrows the actual request header.
use super::*;
/// Authenticate the HTTP request and return an `AuthContext`.
///
/// - No auth configured → admin access for local-only control.
/// - Valid token → permissions from the matching token entry.
/// - Invalid / missing token → empty permissions (handler gates will reject).
pub(super) struct HeldAuth {
    context: AuthContext,
    _subject: Option<zero_runtime::configuration::ControlBudgetPermit>,
}
impl std::ops::Deref for HeldAuth {
    type Target = AuthContext;
    fn deref(&self) -> &Self::Target {
        &self.context
    }
}
fn plain(context: AuthContext) -> io::Result<HeldAuth> {
    Ok(HeldAuth {
        context,
        _subject: None,
    })
}
pub(super) fn authenticate(
    request: &HttpRequest,
    auth: Option<&HttpServerAuth>,
    budget: &Arc<zero_runtime::configuration::ControlBudget>,
) -> io::Result<HeldAuth> {
    let Some(auth) = auth else {
        return plain(AuthContext {
            subject: None,
            permissions: vec![
                Permission::Read,
                Permission::Control,
                Permission::Config,
                Permission::Admin,
            ],
        });
    };

    if auth.is_empty() {
        return plain(AuthContext {
            subject: None,
            permissions: vec![
                Permission::Read,
                Permission::Control,
                Permission::Config,
                Permission::Admin,
            ],
        });
    }

    // Look for a Bearer token or X-Zero-Api-Key header.
    let presented = request.headers.iter().find_map(|(name, value)| {
        if name.eq_ignore_ascii_case("authorization") {
            value.strip_prefix("Bearer ")
        } else if name.eq_ignore_ascii_case("x-zero-api-key") {
            Some(value.as_str())
        } else {
            None
        }
    });

    let Some(token) = presented else {
        return plain(AuthContext {
            subject: None,
            permissions: vec![],
        });
    };

    // Constant-time comparison to avoid timing side-channels.
    for entry in &auth.tokens {
        let match_len = entry.key.len().min(token.len());
        let eq = {
            use std::cmp::Ordering;
            entry.key.as_bytes()[..match_len]
                .iter()
                .zip(token.as_bytes()[..match_len].iter())
                .fold(Ordering::Equal, |acc, (a, b)| match (acc, a.cmp(b)) {
                    (Ordering::Equal, Ordering::Equal) => Ordering::Equal,
                    _ => Ordering::Less,
                })
                == Ordering::Equal
                && entry.key.len() == token.len()
        };
        if eq {
            let subject = budget
                .reserve(zero_runtime::configuration::ControlCharge::RequestBytes(
                    entry.name.len(),
                ))
                .map_err(input::capacity)?;
            return Ok(HeldAuth {
                context: AuthContext {
                    subject: Some(entry.name.clone()),
                    permissions: entry.permissions.clone(),
                },
                _subject: Some(subject),
            });
        }
    }

    plain(AuthContext {
        subject: None,
        permissions: vec![],
    })
}
