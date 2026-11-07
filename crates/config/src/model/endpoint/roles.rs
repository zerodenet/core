//! Read-only role facts from the same canonical/legacy endpoint projection.
use super::*;
#[derive(Clone, Copy, Debug)]
pub struct EndpointRoleBindingRef<'a> {
    endpoint_prefix: &'static str,
    endpoint_tag: &'a str,
    role_prefix: &'static str,
    role_tag: &'a str,
    inbound: bool,
}
impl EndpointRoleBindingRef<'_> {
    pub fn matches_endpoint(&self, id: &str) -> bool {
        id.strip_prefix(self.endpoint_prefix) == Some(self.endpoint_tag)
    }
    pub fn matches_role(&self, inbound: bool, tag: &str) -> bool {
        self.inbound == inbound && tag.strip_prefix(self.role_prefix) == Some(self.role_tag)
    }
}
impl RuntimeConfig {
    /// Borrow the exact roles that endpoint_bindings materializes, with no cloned
    /// strings, protocol config, role authority or secondary normalized inventory.
    pub fn borrowed_endpoint_roles(
        &self,
    ) -> impl Iterator<Item = EndpointRoleBindingRef<'_>> + Clone {
        let canonical = self.endpoints.iter().flat_map(|endpoint| {
            [
                Some(EndpointRoleBindingRef {
                    endpoint_prefix: "endpoint:",
                    endpoint_tag: &endpoint.tag,
                    role_prefix: "",
                    role_tag: &endpoint.tag,
                    inbound: false,
                }),
                endpoint.listen.as_ref().map(|_| EndpointRoleBindingRef {
                    endpoint_prefix: "endpoint:",
                    endpoint_tag: &endpoint.tag,
                    role_prefix: "endpoint/",
                    role_tag: &endpoint.tag,
                    inbound: true,
                }),
            ]
            .into_iter()
            .flatten()
        });
        let outbound = self
            .outbounds
            .iter()
            .filter(move |outbound| {
                !self
                    .endpoints
                    .iter()
                    .any(|endpoint| endpoint.tag == outbound.tag)
            })
            .flat_map(|outbound| {
                let OutboundProtocolConfig::Wireguard { inbound_tag, .. } = &outbound.protocol
                else {
                    return [None, None].into_iter().flatten();
                };
                let (prefix, endpoint) = inbound_tag
                    .as_deref()
                    .map_or(("legacy:outbound:", outbound.tag.as_str()), |tag| {
                        ("legacy:inbound:", tag)
                    });
                [
                    Some(EndpointRoleBindingRef {
                        endpoint_prefix: prefix,
                        endpoint_tag: endpoint,
                        role_prefix: "",
                        role_tag: &outbound.tag,
                        inbound: false,
                    }),
                    inbound_tag.as_deref().map(|tag| EndpointRoleBindingRef {
                        endpoint_prefix: prefix,
                        endpoint_tag: endpoint,
                        role_prefix: "",
                        role_tag: tag,
                        inbound: true,
                    }),
                ]
                .into_iter()
                .flatten()
            });
        let inbound=self.inbounds.iter().filter(move |inbound|{
   matches!(inbound.protocol,InboundProtocolConfig::Wireguard{..})
    &&!self.endpoints.iter().any(|endpoint|endpoint.listen.is_some()&&inbound.tag.strip_prefix("endpoint/")==Some(endpoint.tag.as_str()))
    &&!self.outbounds.iter().filter(move |outbound|!self.endpoints.iter().any(|endpoint|endpoint.tag==outbound.tag)).any(|outbound|matches!(&outbound.protocol,OutboundProtocolConfig::Wireguard{inbound_tag:Some(tag),..} if tag==&inbound.tag))
  }).map(|inbound|EndpointRoleBindingRef{endpoint_prefix:"legacy:inbound:",endpoint_tag:&inbound.tag,role_prefix:"",role_tag:&inbound.tag,inbound:true});
        canonical.chain(outbound).chain(inbound)
    }
}

#[cfg(test)]
mod tests;
