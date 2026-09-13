// Run from Xray v26.3.27: go run /path/prepare-clienthello-vectors.go /path/output
// uTLS aa6edf4b11af82e110eea845bb2983d30138d651 (BSD-3-Clause).
package main

import (
	"encoding/json"
	"fmt"
	tls "github.com/refraction-networking/utls"
	"os"
	"path/filepath"
	"runtime/debug"
)

type entry struct {
	Name    string
	ID      tls.ClientHelloID
	Shuffle bool
}
type metadata struct {
	Name       string
	Padding    bool
	Shuffle    bool
	ECHLengths []uint16
	ECHCiphers [][2]uint16
}

func main() {
	info, ok := debug.ReadBuildInfo()
	if !ok {
		panic("missing Go build information")
	}
	pinned := false
	for _, dep := range info.Deps {
		if dep.Path == "github.com/refraction-networking/utls" && dep.Version == "v1.8.3-0.20260301010127-aa6edf4b11af" && dep.Replace == nil {
			pinned = true
		}
	}
	if !pinned {
		panic("run with the exact pinned uTLS module")
	}

	entries := []entry{
		{"chrome-70", tls.HelloChrome_70, false}, {"chrome-72", tls.HelloChrome_72, false},
		{"firefox-63", tls.HelloFirefox_63, false}, {"firefox-65", tls.HelloFirefox_65, false},
		{"chrome-83", tls.HelloChrome_83, false}, {"chrome-87", tls.HelloChrome_87, false},
		{"chrome-96", tls.HelloChrome_96, false}, {"chrome-100", tls.HelloChrome_100, false},
		{"chrome-102", tls.HelloChrome_102, false}, {"chrome-106", tls.HelloChrome_106_Shuffle, true},
		{"chrome-120", tls.HelloChrome_120, true}, {"chrome-131", tls.HelloChrome_131, true},
		{"chrome-133", tls.HelloChrome_133, true},
		{"firefox-99", tls.HelloFirefox_99, false}, {"firefox-102", tls.HelloFirefox_102, false},
		{"firefox-105", tls.HelloFirefox_105, false}, {"firefox-120", tls.HelloFirefox_120, false}, {"firefox-148", tls.HelloFirefox_148, false},
		{"ios-13", tls.HelloIOS_13, false}, {"ios-14", tls.HelloIOS_14, false},
		{"edge-85", tls.HelloEdge_85, false}, {"edge-106", tls.HelloEdge_106, false},
		{"safari-16.0", tls.HelloSafari_16_0, false}, {"safari-26.3", tls.HelloSafari_26_3, false},
		{"360-11.0", tls.Hello360_11_0, false}, {"qq-11.1", tls.HelloQQ_11_1, false},
	}
	var out []metadata
	for _, e := range entries {
		spec, err := tls.UTLSIdToSpec(e.ID)
		if err != nil {
			panic(err)
		}
		m := metadata{Name: e.Name, Shuffle: e.Shuffle}
		for _, ext := range spec.Extensions {
			switch x := ext.(type) {
			case *tls.UtlsPaddingExtension:
				m.Padding = true
			case *tls.GREASEEncryptedClientHelloExtension:
				m.ECHLengths = x.CandidatePayloadLens
				for _, c := range x.CandidateCipherSuites {
					m.ECHCiphers = append(m.ECHCiphers, [2]uint16{c.KdfId, c.AeadId})
				}
			}
		}
		conn := tls.UClient(nil, &tls.Config{ServerName: "example.com"}, tls.HelloCustom)
		if err = conn.ApplyPreset(&spec); err != nil {
			panic(fmt.Errorf("%s: %w", e.Name, err))
		}
		if err = conn.BuildHandshakeState(); err != nil {
			panic(err)
		}
		if err = os.WriteFile(filepath.Join(os.Args[1], e.Name+".bin"), conn.HandshakeState.Hello.Raw, 0644); err != nil {
			panic(err)
		}
		out = append(out, m)
	}
	data, _ := json.MarshalIndent(out, "", "  ")
	if err := os.WriteFile(filepath.Join(os.Args[1], "manifest.json"), data, 0644); err != nil {
		panic(err)
	}
}
