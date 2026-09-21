use core::{net::IpAddr, str::FromStr};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct IpNetwork {
    address: IpAddr,
    prefix_len: u8,
}

impl IpNetwork {
    pub const fn address(self) -> IpAddr {
        self.address
    }

    pub const fn prefix_len(self) -> u8 {
        self.prefix_len
    }

    pub fn network_address(self) -> IpAddr {
        match self.address {
            IpAddr::V4(address) => {
                let mask = if self.prefix_len == 0 {
                    0
                } else {
                    u32::MAX << (32 - self.prefix_len)
                };
                IpAddr::V4((u32::from(address) & mask).into())
            }
            IpAddr::V6(address) => {
                let mask = if self.prefix_len == 0 {
                    0
                } else {
                    u128::MAX << (128 - self.prefix_len)
                };
                IpAddr::V6((u128::from(address) & mask).into())
            }
        }
    }

    pub fn contains(self, candidate: IpAddr) -> bool {
        candidate.is_ipv4() == self.address.is_ipv4()
            && Self {
                address: candidate,
                prefix_len: self.prefix_len,
            }
            .network_address()
                == self.network_address()
    }

    pub(super) fn same_prefix(self, other: Self) -> bool {
        self.prefix_len == other.prefix_len && self.network_address() == other.network_address()
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NetworkError {
    MissingPrefix,
    InvalidAddress,
    InvalidPrefix,
}

pub fn parse_network(value: &str) -> Result<IpNetwork, NetworkError> {
    let (address, prefix) = value.split_once('/').ok_or(NetworkError::MissingPrefix)?;
    let address = IpAddr::from_str(address).map_err(|_| NetworkError::InvalidAddress)?;
    let prefix_len = prefix
        .parse::<u8>()
        .map_err(|_| NetworkError::InvalidPrefix)?;
    let max = if address.is_ipv4() { 32 } else { 128 };
    if prefix_len > max {
        return Err(NetworkError::InvalidPrefix);
    }
    Ok(IpNetwork {
        address,
        prefix_len,
    })
}
