//! Related integration cases share one executable; feature gates stay on cases.

#[path = "../reality_aead_tests.rs"]
mod reality_aead_tests;
#[path = "../reality_auth_tests.rs"]
mod reality_auth_tests;
#[path = "../reality_policy.rs"]
mod reality_policy;
#[path = "../reality_reader_writer_tests.rs"]
mod reality_reader_writer_tests;
#[path = "../reality_records_tests.rs"]
mod reality_records_tests;
#[path = "../reality_spider.rs"]
mod reality_spider;
#[path = "../reality_tls13_keys_tests.rs"]
mod reality_tls13_keys_tests;
#[path = "../reality_tls13_messages_tests.rs"]
mod reality_tls13_messages_tests;
#[path = "../reality_util_tests.rs"]
mod reality_util_tests;
