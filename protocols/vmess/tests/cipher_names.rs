#![cfg(feature = "validation")]

use vmess::VmessCipher;

#[test]
fn private_extension_and_xray_zero_have_distinct_names() {
    assert_eq!(
        VmessCipher::from_name("zero-plus"),
        Some(VmessCipher::ZeroPlus)
    );
    assert_eq!(VmessCipher::ZeroPlus.name(), "zero-plus");
    assert_eq!(VmessCipher::from_name("zero"), Some(VmessCipher::Zero));
    assert_eq!(VmessCipher::from_name("none"), Some(VmessCipher::None));
}
