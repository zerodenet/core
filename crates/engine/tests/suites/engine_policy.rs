//! Related integration cases share one executable; feature gates stay on cases.

#[path = "../device_limit.rs"]
mod device_limit;
#[path = "../policy_reload.rs"]
mod policy_reload;
#[path = "../quota.rs"]
mod quota;
#[path = "../session_cancellation.rs"]
mod session_cancellation;
