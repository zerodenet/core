use std::net::{IpAddr, Ipv4Addr, SocketAddr};

use rcgen::{
    BasicConstraints, CertificateParams, ExtendedKeyUsagePurpose, IsCa, Issuer, KeyPair,
    KeyUsagePurpose,
};
use tonic::transport::{Certificate, ClientTlsConfig, Endpoint, Identity};
use zero_api::{
    CommandRequest, CommandResponse, CommandService, EventFilter, EventReplay, EventSource,
    EventStream, QueryRequest, QueryResponse, QueryService, RawApiEvent,
};
use zero_grpc::{GrpcServerAuth, GrpcServerSecurity, GrpcServerTls};

#[derive(Clone)]
struct UnusedService;

struct EmptyEventStream;

impl EventStream for EmptyEventStream {
    fn recv(&self) -> Option<RawApiEvent> {
        None
    }

    fn try_recv(&self) -> Option<RawApiEvent> {
        None
    }
}

impl QueryService for UnusedService {
    fn query(&self, _request: QueryRequest) -> zero_api::ApiResult<QueryResponse> {
        unreachable!("transport security tests do not issue API queries")
    }
}

impl CommandService for UnusedService {
    fn execute(&self, _command: CommandRequest) -> zero_api::ApiResult<CommandResponse> {
        unreachable!("transport security tests do not issue API commands")
    }
}

impl EventSource for UnusedService {
    type Stream = EmptyEventStream;

    fn subscribe(&self, _filter: EventFilter) -> zero_api::ApiResult<Self::Stream> {
        Ok(EmptyEventStream)
    }

    fn latest(&self, _limit: usize, _filter: EventFilter) -> zero_api::ApiResult<Vec<RawApiEvent>> {
        Ok(Vec::new())
    }

    fn since(
        &self,
        sequence: u64,
        _limit: usize,
        _filter: EventFilter,
    ) -> zero_api::ApiResult<EventReplay> {
        Ok(EventReplay {
            core_instance_id: "test-core".to_owned(),
            requested_after: sequence,
            actual_from: sequence.saturating_add(1),
            has_gap: false,
            events: Vec::new(),
        })
    }
}

#[tokio::test]
async fn remote_plaintext_requires_explicit_opt_in() {
    let address = SocketAddr::new(IpAddr::V4(Ipv4Addr::UNSPECIFIED), 0);
    let security = GrpcServerSecurity {
        auth: Some(GrpcServerAuth::single_admin("secret".to_owned())),
        ..Default::default()
    };
    let result = zero_grpc::spawn(UnusedService, address, security).await;
    let error = match result {
        Ok(server) => {
            server.shutdown().await;
            panic!("remote plaintext must require explicit opt-in")
        }
        Err(error) => error,
    };
    assert!(error.to_string().contains("explicit insecure opt-in"));
}

#[tokio::test]
async fn explicit_remote_plaintext_with_bearer_can_start() {
    let address = SocketAddr::new(IpAddr::V4(Ipv4Addr::UNSPECIFIED), 0);
    let server = zero_grpc::spawn(
        UnusedService,
        address,
        GrpcServerSecurity {
            auth: Some(GrpcServerAuth::single_admin("secret".to_owned())),
            allow_insecure_remote: true,
            ..Default::default()
        },
    )
    .await
    .expect("explicit remote plaintext");
    server.shutdown().await;
}

#[tokio::test]
async fn native_tls_accepts_a_trusted_client() {
    let certified = rcgen::generate_simple_self_signed(vec!["localhost".to_owned()])
        .expect("generate server certificate");
    let cert_pem = certified.cert.pem();
    let server = zero_grpc::spawn(
        UnusedService,
        "127.0.0.1:0".parse().expect("listen address"),
        GrpcServerSecurity {
            tls: Some(GrpcServerTls::new(
                cert_pem.as_bytes().to_vec(),
                certified.signing_key.serialize_pem().into_bytes(),
            )),
            ..Default::default()
        },
    )
    .await
    .expect("spawn TLS server");

    connect_tls(
        server.local_addr(),
        ClientTlsConfig::new()
            .ca_certificate(Certificate::from_pem(cert_pem))
            .domain_name("localhost"),
    )
    .await
    .expect("trusted TLS client");
    server.shutdown().await;
}

#[tokio::test]
async fn mtls_requires_a_client_certificate_signed_by_the_configured_ca() {
    let material = test_mtls_material();
    let server = zero_grpc::spawn(
        UnusedService,
        "127.0.0.1:0".parse().expect("listen address"),
        GrpcServerSecurity {
            tls: Some(
                GrpcServerTls::new(
                    material.server_cert_pem.as_bytes().to_vec(),
                    material.server_key_pem.as_bytes().to_vec(),
                )
                .with_client_ca(material.ca_cert_pem.as_bytes().to_vec()),
            ),
            ..Default::default()
        },
    )
    .await
    .expect("spawn mTLS server");

    let base_tls = ClientTlsConfig::new()
        .ca_certificate(Certificate::from_pem(material.ca_cert_pem.clone()))
        .domain_name("localhost");
    assert!(
        connect_tls(server.local_addr(), base_tls.clone())
            .await
            .is_err(),
        "mTLS server must reject clients without a certificate"
    );
    connect_tls(
        server.local_addr(),
        base_tls.identity(Identity::from_pem(
            material.client_cert_pem,
            material.client_key_pem,
        )),
    )
    .await
    .expect("client certificate signed by configured CA");
    server.shutdown().await;
}

async fn connect_tls(
    address: SocketAddr,
    tls: ClientTlsConfig,
) -> Result<(), Box<dyn std::error::Error>> {
    let channel = Endpoint::from_shared(format!("https://{address}"))?
        .tls_config(tls)?
        .connect()
        .await?;
    let mut client = tonic::client::Grpc::new(channel);
    client.ready().await?;
    let result: Result<tonic::Response<WireQueryResponse>, tonic::Status> = client
        .unary(
            tonic::Request::new(WireQueryRequest {
                payload: b"null".to_vec(),
            }),
            tonic::codegen::http::uri::PathAndQuery::from_static("/zero.api.v1.Control/Query"),
            tonic::codec::ProstCodec::default(),
        )
        .await;
    match result {
        Err(status) if status.code() == tonic::Code::InvalidArgument => Ok(()),
        Ok(_) => Ok(()),
        Err(status) => Err(Box::new(status)),
    }
}

#[derive(Clone, PartialEq, prost::Message)]
struct WireQueryRequest {
    #[prost(bytes = "vec", tag = "1")]
    payload: Vec<u8>,
}

#[derive(Clone, PartialEq, prost::Message)]
struct WireQueryResponse {
    #[prost(bytes = "vec", tag = "1")]
    payload: Vec<u8>,
}

struct MtlsMaterial {
    ca_cert_pem: String,
    server_cert_pem: String,
    server_key_pem: String,
    client_cert_pem: String,
    client_key_pem: String,
}

fn test_mtls_material() -> MtlsMaterial {
    let mut ca_params = CertificateParams::new(Vec::new()).expect("CA parameters");
    ca_params.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
    ca_params.key_usages = vec![
        KeyUsagePurpose::DigitalSignature,
        KeyUsagePurpose::KeyCertSign,
        KeyUsagePurpose::CrlSign,
    ];
    let ca_key = KeyPair::generate().expect("CA key");
    let ca_cert = ca_params.self_signed(&ca_key).expect("CA certificate");
    let issuer = Issuer::new(ca_params, ca_key);

    let server_key = KeyPair::generate().expect("server key");
    let mut server_params =
        CertificateParams::new(vec!["localhost".to_owned()]).expect("server parameters");
    server_params
        .extended_key_usages
        .push(ExtendedKeyUsagePurpose::ServerAuth);
    let server_cert = server_params
        .signed_by(&server_key, &issuer)
        .expect("server certificate");

    let client_key = KeyPair::generate().expect("client key");
    let mut client_params =
        CertificateParams::new(vec!["zero-controller".to_owned()]).expect("client parameters");
    client_params
        .extended_key_usages
        .push(ExtendedKeyUsagePurpose::ClientAuth);
    let client_cert = client_params
        .signed_by(&client_key, &issuer)
        .expect("client certificate");

    MtlsMaterial {
        ca_cert_pem: ca_cert.pem(),
        server_cert_pem: server_cert.pem(),
        server_key_pem: server_key.serialize_pem(),
        client_cert_pem: client_cert.pem(),
        client_key_pem: client_key.serialize_pem(),
    }
}
