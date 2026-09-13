# HTTP content detection reference

`http-sniff-go1.26.1.tsv` contains 889 input/output vectors generated with the
Go 1.26.1 `net/http.DetectContentType` implementation. This is the Go version
selected by Xray-core v26.3.27, commit
`d2758a023cd7f4174a5a5fa4ff66e487d4342ba0`.

Each line is a hex-encoded input, a tab, and the reference MIME type. Inputs cover
every signature family, truncated prefixes, HTML tag terminators and whitespace,
all single-byte values, MP4 box constraints, and the 512-byte inspection limit.

Regenerate from the repository root:

```sh
GOTOOLCHAIN=go1.26.1 go run scripts/prepare-http-sniff-vectors.go
```

SHA-256: `e2ee6ac0aa3bfabf3b18c6ad10e4d17786fe980d0781f679358a5d8a9cee41c4`.

The file server also uses Go's 64 built-in extension mappings, including
case-insensitive lookup. Host-installed MIME database overrides are not imported
into this deterministic table and are not claimed as matching across systems.
