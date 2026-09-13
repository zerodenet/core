use super::*;
use crate::profile::OwnedGrpcProfile;
use zero_traits::GrpcTransportProfile;

pub(super) fn legacy(names: &[String]) -> OwnedGrpcProfile {
    OwnedGrpcProfile {
        service_names: names.to_vec(),
        ..Default::default()
    }
}
pub(super) fn service_path(name: &str, multi: bool) -> String {
    if !name.starts_with('/') {
        return format!(
            "/{}/{}",
            escape(name),
            if multi { "TunMulti" } else { "Tun" }
        );
    }
    let last = name.rfind('/').unwrap().max(1);
    let service = name[1..last]
        .split('/')
        .map(escape)
        .collect::<Vec<_>>()
        .join("/");
    let methods: Vec<_> = name[name.rfind('/').unwrap() + 1..].split('|').collect();
    let method = if multi && methods.len() > 1 {
        methods[1]
    } else {
        methods[0]
    };
    format!("/{service}/{}", escape(method))
}
fn escape(value: &str) -> String {
    let mut escaped = String::new();
    for byte in value.bytes() {
        if byte.is_ascii_alphanumeric() || b"-_.~$&+:=@".contains(&byte) {
            escaped.push(byte as char);
        } else {
            use std::fmt::Write;
            write!(escaped, "%{byte:02X}").unwrap();
        }
    }
    escaped
}
pub(super) fn request(
    profile: &(impl GrpcTransportProfile + ?Sized),
    default_authority: &str,
) -> Result<Request<()>, RuntimeError> {
    let path = service_path(
        choose_grpc_service_name(profile.service_names())?,
        profile.multi_mode(),
    );
    let authority = profile
        .authority()
        .filter(|value| !value.is_empty())
        .unwrap_or(default_authority);
    let uri = http::Uri::builder()
        .scheme("https")
        .authority(authority)
        .path_and_query(path)
        .build()
        .map_err(io::Error::other)?;
    let mut request = Request::builder()
        .method(Method::POST)
        .uri(uri)
        .header("content-type", "application/grpc")
        .header("te", "trailers");
    if let Some(agent) = crate::browser::grpc_user_agent(profile.user_agent().unwrap_or("chrome")) {
        request = request.header("user-agent", agent);
    }
    request
        .body(())
        .map_err(|error| io::Error::other(error).into())
}
pub(super) fn matches(profile: &OwnedGrpcProfile, request: &Request<h2::RecvStream>) -> bool {
    request.method() == Method::POST
        && request
            .headers()
            .get("content-type")
            .is_some_and(|value| value.as_bytes().starts_with(b"application/grpc"))
        && profile.service_names.iter().any(|name| {
            [false, true]
                .into_iter()
                .any(|multi| service_path(name, multi) == request.uri().path())
        })
}
