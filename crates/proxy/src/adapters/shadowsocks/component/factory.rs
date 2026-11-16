//! Select the protocol's actual roles, never a fake Stream for a UDP original.
use super::*;
use zero_platform_tokio::contract::{
    component::{BuildContext, ComponentFactory, ComponentInstance, ComponentSpec},
    lifecycle::BuildError,
};
pub(crate) enum Options {
    Stream(Arc<stateless::Options<StreamOptions>>),
    Datagram(Arc<shadowsocks::component::udp::DatagramOptions>),
}
pub(crate) struct Factory;
impl ComponentFactory for Factory {
    type Options = Options;
    fn describe(&self, options: &Options) -> ComponentSpec {
        match options {
            Options::Stream(options) => {
                stateless::Factory(shadowsocks::component::StreamFactory).describe(options)
            }
            Options::Datagram(options) => {
                shadowsocks::component::udp::DatagramFactory.describe(options)
            }
        }
    }
    fn build(
        &self,
        options: Arc<Options>,
        context: BuildContext,
    ) -> Result<ComponentInstance, BuildError> {
        match options.as_ref() {
            Options::Stream(options) => stateless::Factory(shadowsocks::component::StreamFactory)
                .build(options.clone(), context),
            Options::Datagram(options) => {
                shadowsocks::component::udp::DatagramFactory.build(options.clone(), context)
            }
        }
    }
}
