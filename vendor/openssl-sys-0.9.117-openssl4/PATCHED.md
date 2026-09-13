# OpenSSL 4 vendored-source pin

This is `openssl-sys` 0.9.117 from rust-openssl tag `openssl-sys-v0.9.117`
(commit `db9c9e2f5db2ad7b45fd894e8d297ee15bfd0c7c`, MIT license).
The only source change is the `openssl-src` build dependency pin from `300.2.0`
to exact `400.0.1`, which packages OpenSSL 4.0.2. The reviewed upstream OpenSSL
4.0.2 source tag resolves to commit
`f089acdf4bc7ba94a79f4bf6eb7362c3e7d14aa9` and retains the upstream Apache-2.0
license through `openssl-src`.
