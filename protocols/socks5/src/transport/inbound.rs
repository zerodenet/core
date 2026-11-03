use super::{Socks5InboundAcceptor, Socks5InboundUserRef};
use zero_core::InboundClientResponse;
use zero_traits::AsyncSocket;

impl Socks5InboundAcceptor {
    pub fn from_options_refs<'a, I>(users: I) -> Self
    where
        I: IntoIterator<Item = Socks5InboundUserRef<'a>>,
    {
        Self::new(crate::Socks5InboundTcpAcceptor::from_config_users(
            users.into_iter().map(|user| {
                (
                    user.username,
                    user.password,
                    user.principal_key,
                    user.up_bps,
                    user.down_bps,
                )
            }),
        ))
    }

    fn new(protocol: crate::Socks5InboundTcpAcceptor) -> Self {
        Self { protocol }
    }

    pub async fn accept_command<S>(
        &self,
        stream: &mut S,
    ) -> Result<crate::Socks5Request, zero_core::Error>
    where
        S: AsyncSocket,
    {
        self.protocol.accept_command(stream).await
    }

    pub async fn accept_command_facts<S>(
        &self,
        stream: &mut S,
    ) -> Result<crate::Socks5CommandFacts, zero_core::Error>
    where
        S: AsyncSocket,
    {
        self.protocol.accept_command_facts(stream).await
    }
}

impl<S> InboundClientResponse<S> for Socks5InboundAcceptor
where
    S: AsyncSocket,
{
    async fn send_ok(&self, client: &mut S) -> Result<(), zero_core::Error> {
        self.protocol.send_success(client).await
    }

    async fn send_blocked(&self, client: &mut S) -> Result<(), zero_core::Error> {
        self.protocol.send_blocked(client).await
    }

    async fn send_upstream_failure(&self, client: &mut S) -> Result<(), zero_core::Error> {
        self.protocol.send_upstream_failure(client).await
    }
}
