// Run from the pinned Xray v26.3.27 source tree with Go 1.26.1.
package main

import (
    "crypto/ed25519"
    "crypto/rand"
    "crypto/x509"
    "crypto/x509/pkix"
    "math/big"
    "os"
    "path/filepath"
    "time"
    "golang.org/x/crypto/ocsp"
)

func main() {
    issuerKey := ed25519.NewKeyFromSeed(make([]byte, 32))
    seed := make([]byte, 32); seed[0] = 1
    leafKey := ed25519.NewKeyFromSeed(seed)
    start := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
    issuer := &x509.Certificate{SerialNumber: big.NewInt(1), Subject: pkix.Name{CommonName: "OCSP fixture issuer"}, NotBefore: start, NotAfter: start.AddDate(10, 0, 0), IsCA: true, BasicConstraintsValid: true, KeyUsage: x509.KeyUsageCertSign}
    issuerDER, err := x509.CreateCertificate(rand.Reader, issuer, issuer, issuerKey.Public(), issuerKey); if err != nil { panic(err) }
    issuer, err = x509.ParseCertificate(issuerDER); if err != nil { panic(err) }
    leaf := &x509.Certificate{SerialNumber: big.NewInt(128), Subject: pkix.Name{CommonName: "OCSP fixture leaf"}, NotBefore: start, NotAfter: start.AddDate(1, 0, 0), DNSNames: []string{"fixture.test"}, OCSPServer: []string{"http://ocsp.test/status"}, IssuingCertificateURL: []string{"http://issuer.test/ca.der"}}
    leafDER, err := x509.CreateCertificate(rand.Reader, leaf, issuer, leafKey.Public(), issuerKey); if err != nil { panic(err) }
    leaf, err = x509.ParseCertificate(leafDER); if err != nil { panic(err) }
    request, err := ocsp.CreateRequest(leaf, issuer, nil); if err != nil { panic(err) }
    for name, data := range map[string][]byte{"issuer.der": issuerDER, "leaf.der": leafDER, "request.der": request} {
        if err := os.WriteFile(filepath.Join(os.Args[1], name), data, 0644); err != nil { panic(err) }
    }
}
