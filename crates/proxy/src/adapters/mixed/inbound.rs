//! Builtin first-byte multiplexing; both branches use standard accepted roles.
use std::sync::Arc;
use zero_platform_tokio::{
    contract::{
        component::*,
        context::{HandshakeContext, InboundConnectionFacts},
        inbound::{erase_inbound, tcp, AcceptedRequest, InboundHandshake},
        lifecycle::{erase_lifecycle, BuildError},
        ChannelError, Impact, Reason, Stage,
    },
    TcpRelayStream, TokioSocket,
};
use zero_traits::AsyncSocket;
type Options = socks5::component::listener::InboundOptions;
pub(crate) fn component_options(
    inbound: &zero_config::InboundConfig,
) -> Result<Options, zero_config::ConfigError> {
    let zero_config::InboundProtocolConfig::Mixed { socks5_users } = &inbound.protocol else {
        return Err(zero_config::ConfigError::InvalidInbound(
            "mixed factory received another protocol".into(),
        ));
    };
    crate::adapters::socks5::inbound::options(inbound, socks5_users)
}
pub(crate) struct Factory;
impl ComponentFactory for Factory {
    type Options = Options;
    fn describe(&self, options: &Options) -> ComponentSpec {
        socks5::component::listener::InboundFactory.describe(options)
    }
    fn build(
        &self,
        options: Arc<Options>,
        context: BuildContext,
    ) -> Result<ComponentInstance, BuildError> {
        let resources = context.resources.clone();
        let (lifecycle, source) = tcp::source(
            options.listen.clone(),
            context.resources,
            context.cancel,
            move |socket| Connection {
                socket,
                options: options.clone(),
                resources: resources.clone(),
            },
        );
        Ok(ComponentInstance {
            lifecycle: erase_lifecycle(lifecycle),
            roles: ComponentRoles {
                inbound: Some(erase_inbound(source)),
                ..Default::default()
            },
        })
    }
}
struct Connection {
    socket: TokioSocket,
    options: Arc<Options>,
    resources: Arc<dyn ResourceAccess>,
}
impl InboundHandshake for Connection {
    async fn complete(
        mut self,
        context: HandshakeContext,
    ) -> Result<AcceptedRequest, ChannelError> {
        let facts = InboundConnectionFacts::from_stream(&self.socket);
        let mut first = [0];
        if self.socket.read(&mut first).await.map_err(|source| {
            ChannelError::new(Stage::Handshake, Reason::Io, Impact::Session).with_source(source)
        })? == 0
        {
            return Err(ChannelError::new(
                Stage::Handshake,
                Reason::Revoked,
                Impact::Session,
            ));
        }
        let prefixed = crate::transport::PrefixedSocket::from_byte(self.socket, first[0]);
        let io = match facts.local_endpoint {
            Some(local) => TcpRelayStream::with_local_addr(prefixed, local),
            None => TcpRelayStream::new(prefixed),
        };
        if socks5::is_socks5_greeting_byte(first[0]) {
            socks5::component::inbound::Connection::new(
                io,
                facts,
                self.options.acceptor(),
                self.resources,
            )
            .complete(context)
            .await
        } else {
            Ok(AcceptedRequest::Multiplex(Box::new(
                http::component::Requests::new(io).with_connection_facts(facts),
            )))
        }
    }
}

pub(crate) fn component_options_in_runtime(
    inbound: &zero_config::InboundConfig,
    runtime: &zero_config::RuntimeOptionsConfig,
) -> Result<socks5::component::listener::InboundOptions, zero_config::ConfigError> {
    component_options(inbound)?
        .with_inbound_work_limits(
            std::num::NonZeroUsize::new(runtime.effective_inbound_max_tasks_per_carrier(2))
                .unwrap(),
            runtime.effective_inbound_max_resources_per_carrier(2),
        )
        .map_err(|source| zero_config::ConfigError::InvalidInbound(source.to_string()))
}
