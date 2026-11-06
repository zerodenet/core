pub(crate) trait UpstreamAssociationTarget: Clone + PartialEq {
    fn outbound_tag(&self) -> &str;

    fn log_parts(&self) -> (&str, Option<(&str, u16)>);
}

pub(crate) use crate::inventory::DatagramAssociationTarget;
