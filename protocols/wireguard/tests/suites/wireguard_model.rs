//! Related integration cases share one executable; feature gates stay on cases.

#[cfg(feature = "routing")]
#[path = "../routing.rs"]
mod routing;
#[cfg(feature = "validation")]
#[path = "../validation.rs"]
mod validation;
