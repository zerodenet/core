#[derive(Debug, Clone, Copy)]
pub struct MieruInboundUserRef<'a> {
    pub username: &'a str,
    pub password: &'a str,
    pub principal_key: Option<&'a str>,
}

#[derive(Debug, Clone, Copy)]
pub struct MieruOutboundOptionsRef<'a> {
    pub username: &'a str,
    pub password: &'a str,
}
