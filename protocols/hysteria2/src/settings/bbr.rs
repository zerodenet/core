//! Protocol-owned names for the pinned reference BBR profiles.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum BbrProfile {
    Conservative,
    #[default]
    Standard,
    Aggressive,
}
impl BbrProfile {
    pub fn parse(value: &str) -> Result<Self, &'static str> {
        match value.to_ascii_lowercase().as_str() {
            "" | "standard" => Ok(Self::Standard),
            "conservative" => Ok(Self::Conservative),
            "aggressive" => Ok(Self::Aggressive),
            _ => Err("hysteria2 BBR profile must be conservative, standard or aggressive"),
        }
    }
}
