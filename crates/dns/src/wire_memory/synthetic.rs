//! One measured wire output, before its Vec is allocated.
use super::*;
impl DnsWireResponse {
    pub(crate) fn error(
        query: &[u8],
        code: u8,
        truncated: bool,
        memory: Option<&DnsWireMemory>,
    ) -> io::Result<Self> {
        let mut buffer = WireBuffer::new(crate::message::error_response_size(query), memory)?;
        buffer.build(|bytes| {
            crate::message::build_error_response_into(query, code, truncated, bytes)
        });
        buffer.freeze()
    }
    pub(crate) fn addresses(
        query: &[u8],
        addresses: &[zero_traits::IpAddress],
        ttl: u32,
        memory: Option<&DnsWireMemory>,
    ) -> io::Result<Self> {
        let mut buffer = WireBuffer::new(
            crate::message::address_response_size(query, addresses)?,
            memory,
        )?;
        buffer.build(|bytes| {
            crate::message::build_address_response_into(query, addresses, ttl, bytes)
        });
        buffer.freeze()
    }
    pub(crate) fn fit_udp(self, query: &[u8], memory: Option<&DnsWireMemory>) -> io::Result<Self> {
        let limit = crate::message::parse_question(query)
            .map(|q| q.udp_payload_size)
            .unwrap_or(512);
        if self.len() <= limit {
            return Ok(self);
        }
        let code = self.get(3).copied().unwrap_or(0) & 0x0f;
        // Preserve the input backing/credit during creation of the TC result.
        // Actual output memory can coexist until the old allocation is freed.
        Self::error(query, code, true, memory)
    }
    pub(crate) fn query(
        domain: &str,
        kind: u16,
        memory: Option<&DnsWireMemory>,
    ) -> io::Result<Self> {
        let domain = crate::message::normalize_domain(domain)?;
        let capacity = domain
            .len()
            .checked_add(29)
            .ok_or_else(|| memory_error(PacketMemoryError::Overflow))?;
        let mut buffer = WireBuffer::new(capacity, memory)?;
        let mut outcome = Ok(());
        buffer.build(|bytes| outcome = crate::message::build_query_into(&domain, kind, bytes));
        outcome?;
        buffer.freeze()
    }
}

impl DnsWireResponse {
    pub(crate) fn with_id(self, id: [u8; 2], memory: Option<&DnsWireMemory>) -> io::Result<Self> {
        if self.get(..2) == Some(id.as_slice()) {
            return Ok(self);
        }
        let mut changed = self.mutable_copy(memory)?;
        if let Some(target) = changed.as_mut().get_mut(..2) {
            target.copy_from_slice(&id);
        }
        changed.freeze()
    }
    pub(crate) fn apply_policy(
        self,
        query: &[u8],
        policy: crate::message::ResponseAddressPolicy,
        memory: Option<&DnsWireMemory>,
    ) -> io::Result<Self> {
        if policy.allow_ipv4 && policy.allow_ipv6 && !policy.suppress_real_addresses {
            crate::message::parse_response(query, &self)?;
            return Ok(self);
        }
        let mut changed = self.mutable_copy(memory)?;
        crate::message::apply_response_address_policy_in_place(
            query,
            changed.as_mut(),
            policy,
            memory,
        )?;
        changed.freeze()
    }
}
