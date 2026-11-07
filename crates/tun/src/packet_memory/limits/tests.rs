use super::*;

#[test]
fn revoked_read_owner_does_not_allow_partial_policy_change_while_its_real_buffer_is_retiring() {
    let root = Arc::new(PacketReturnTotals::new_with_transmit(
        PacketMemoryLimit {
            frames: 1,
            bytes: 64,
        },
        PacketMemoryLimit {
            frames: 2,
            bytes: 128,
        },
        PacketMemoryLimit {
            frames: 1,
            bytes: 64,
        },
    ));
    let mut grants = root.native_grants(&[64, 64]).unwrap();
    let buffer = grants[0].try_buffer().unwrap();
    for grant in &grants {
        grant.revoke();
    }
    grants.clear();
    assert_eq!(
        root.native().occupied,
        PacketMemoryLimit {
            frames: 1,
            bytes: 64
        }
    );
    let before = (root.returns(), root.native(), root.transmit());
    let returns = PacketMemoryLimit {
        frames: 2,
        bytes: 128,
    };
    let native = PacketMemoryLimit {
        frames: 1,
        bytes: 32,
    };
    let transmit = PacketMemoryLimit {
        frames: 2,
        bytes: 128,
    };
    assert_eq!(
        root.install_limits(returns, native, transmit),
        Err(PacketMemoryError::Busy)
    );
    assert_eq!((root.returns(), root.native(), root.transmit()), before);
    // Actual Rust allocation destruction refunds memory; owner revocation was
    // insufficient. Neither operation is an OS/native or Scope-close receipt.
    drop(buffer);
    root.install_limits(returns, native, transmit).unwrap();
    assert_eq!(root.returns().limit, returns);
    assert_eq!(root.native().limit, native);
    assert_eq!(root.transmit().limit, transmit);
}
