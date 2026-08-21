use std::io;
use std::time::Duration;

const DNS_TIMEOUT: Duration = Duration::from_secs(5);

pub(crate) struct DohDnsResolver {
    url: String,
    authority: String,
    port: u16,
    bootstrap: Vec<std::net::IpAddr>,
    egress: zero_platform_tokio::EgressInterfaceControl,
    client: std::sync::Mutex<ClientState>,
}

struct ClientState {
    interface: Option<zero_platform_tokio::EgressInterface>,
    client: reqwest::Client,
}

impl DohDnsResolver {
    pub(crate) fn new(
        host: String,
        port: u16,
        path: String,
        bootstrap: Vec<std::net::IpAddr>,
        server_name: Option<String>,
        egress: zero_platform_tokio::EgressInterfaceControl,
    ) -> io::Result<Self> {
        let bootstrap = if bootstrap.is_empty() {
            host.parse().map(|ip| vec![ip]).unwrap_or_default()
        } else {
            bootstrap
        };
        let authority = server_name.unwrap_or(host);
        let formatted = if authority.parse::<std::net::Ipv6Addr>().is_ok() {
            format!("[{authority}]")
        } else {
            authority.clone()
        };
        let url = format!("https://{formatted}:{port}{path}");
        let interface = selected_interface(&egress);
        let client = build_client(interface.as_ref(), &authority, port, &bootstrap)?;
        Ok(Self {
            url,
            authority,
            port,
            bootstrap,
            egress,
            client: std::sync::Mutex::new(ClientState { interface, client }),
        })
    }

    pub(crate) async fn exchange(&self, query: &[u8]) -> io::Result<Vec<u8>> {
        let response = self
            .client()?
            .post(&self.url)
            .header("Content-Type", "application/dns-message")
            .header("Accept", "application/dns-message")
            .body(query.to_vec())
            .send()
            .await
            .map_err(|error| io::Error::other(format!("DoH request failed: {error}")))?;
        if !response.status().is_success() {
            return Err(io::Error::other(format!(
                "DoH server returned HTTP {}",
                response.status()
            )));
        }
        let body = response
            .bytes()
            .await
            .map_err(|error| io::Error::other(format!("DoH read failed: {error}")))?;
        if body.len() > crate::message::MAX_DNS_MESSAGE_SIZE {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "DoH response exceeds DNS message limit",
            ));
        }
        Ok(body.to_vec())
    }

    fn client(&self) -> io::Result<reqwest::Client> {
        let interface = selected_interface(&self.egress);
        let mut state = self.client.lock().expect("DoH client lock poisoned");
        if state.interface != interface {
            state.client = build_client(
                interface.as_ref(),
                &self.authority,
                self.port,
                &self.bootstrap,
            )?;
            state.interface = interface;
        }
        Ok(state.client.clone())
    }
}

fn build_client(
    interface: Option<&zero_platform_tokio::EgressInterface>,
    authority: &str,
    port: u16,
    bootstrap: &[std::net::IpAddr],
) -> io::Result<reqwest::Client> {
    let builder = reqwest::Client::builder().timeout(DNS_TIMEOUT);
    let addrs = bootstrap
        .iter()
        .map(|ip| std::net::SocketAddr::new(*ip, port))
        .collect::<Vec<_>>();
    let builder = if addrs.is_empty() || authority.parse::<std::net::IpAddr>().is_ok() {
        builder
    } else {
        builder.resolve_to_addrs(authority, &addrs)
    };
    #[cfg(any(
        target_os = "linux",
        target_os = "macos",
        target_os = "android",
        target_os = "ios"
    ))]
    let builder = match interface {
        Some(interface) => builder.interface(interface.name()),
        None => builder,
    };
    #[cfg(not(any(
        target_os = "linux",
        target_os = "macos",
        target_os = "android",
        target_os = "ios"
    )))]
    let _ = interface;
    builder
        .build()
        .map_err(|error| io::Error::other(format!("failed to build DoH client: {error}")))
}

#[cfg(any(
    target_os = "linux",
    target_os = "macos",
    target_os = "android",
    target_os = "ios"
))]
fn selected_interface(
    control: &zero_platform_tokio::EgressInterfaceControl,
) -> Option<zero_platform_tokio::EgressInterface> {
    control.current()
}

#[cfg(not(any(
    target_os = "linux",
    target_os = "macos",
    target_os = "android",
    target_os = "ios"
)))]
fn selected_interface(
    _control: &zero_platform_tokio::EgressInterfaceControl,
) -> Option<zero_platform_tokio::EgressInterface> {
    None
}
