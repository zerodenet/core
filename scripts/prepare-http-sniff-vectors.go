//go:build ignore

// Run with GOTOOLCHAIN=go1.26.1 go run scripts/prepare-http-sniff-vectors.go.
// Go 1.26.1 is the standard-library baseline selected by Xray v26.3.27.
package main

import (
	"bytes"
	"encoding/hex"
	"fmt"
	"net/http"
	"os"
	"runtime"
	"strings"
)

func main() {
	if runtime.Version() != "go1.26.1" {
		panic("requires Go 1.26.1")
	}
	seeds := [][]byte{
		{}, []byte("ordinary text"), []byte("\x00"), []byte("<HTMLish>"),
		[]byte("<html"), []byte("<html\t>"), []byte("<!--comment-->"),
		[]byte(" <?xml version=\"1.0\"?>"), []byte("<?XML"),
		[]byte("%PDF-1.7"), []byte("%!PS-Adobe-3.0"),
		{0xfe, 0xff, 0x01, 0x02}, {0xff, 0xfe, 0x01, 0x02}, {0xef, 0xbb, 0xbf, 0},
		{0, 0, 1, 0}, {0, 0, 2, 0}, []byte("BM"), []byte("GIF87a"), []byte("GIF89a"),
		[]byte("RIFFabcdWEBPVP8"), []byte("\x89PNG\r\n\x1a\n"), []byte("\xff\xd8\xff"),
		[]byte("FORMabcdAIFF"), []byte("ID3"), []byte("OggS\x00"), []byte("MThd\x00\x00\x00\x06"),
		[]byte("RIFFabcdAVI "), []byte("RIFFabcdWAVE"),
		[]byte("\x00\x00\x00\x18ftypisommp4Xmp42abcd"),
		[]byte("\x00\x00\x00\x0cftypmp42"), []byte("\x00\x00\x00\x10ftypisommp42"),
		[]byte("\x00\x00\x00\x0dftypmp42x"), []byte("\xff\xff\xff\xfcftypmp42"),
		[]byte("\x1a\x45\xdf\xa3"), append(bytes.Repeat([]byte{0x71}, 34), 'L', 'P'),
		[]byte("\x00\x01\x00\x00"), []byte("OTTO"), []byte("ttcf"), []byte("wOFF"), []byte("wOF2"),
		[]byte("\x1f\x8b\x08"), []byte("PK\x03\x04"), []byte("Rar!\x1a\x07\x00"),
		[]byte("Rar!\x1a\x07\x01\x00"), []byte("\x00asm"),
		append(bytes.Repeat([]byte{'x'}, 512), 0), append(bytes.Repeat([]byte{'x'}, 511), 0),
	}
	for _, tag := range []string{"<!DOCTYPE HTML", "<html", "<head", "<script", "<iframe", "<h1", "<div", "<font", "<table", "<a", "<style", "<title", "<b", "<body", "<br", "<p", "<!--"} {
		for _, ws := range []string{"", "\t\n\x0c\r ", "\x0b"} {
			seeds = append(seeds, []byte(ws+tag+">"), []byte(ws+tag+" "), []byte(ws+tag+"x"))
		}
	}
	for b := 0; b < 256; b++ {
		seeds = append(seeds, []byte{byte(b)})
	}
	type vector struct {
		Hex  string `json:"hex"`
		MIME string `json:"mime"`
	}
	seen := map[string]bool{}
	vectors := []vector{}
	add := func(data []byte) {
		input := hex.EncodeToString(data)
		if !seen[input] {
			seen[input] = true
			vectors = append(vectors, vector{input, http.DetectContentType(data)})
		}
	}
	for _, seed := range seeds {
		add(seed)
		if len(seed) < 40 {
			for length := 0; length < len(seed); length++ {
				add(seed[:length])
			}
		}
	}
	var output strings.Builder
	for _, vector := range vectors {
		fmt.Fprintf(&output, "%s\t%s\n", vector.Hex, vector.MIME)
	}
	if err := os.WriteFile("crates/transport/tests/fixtures/http-sniff-go1.26.1.tsv", []byte(output.String()), 0644); err != nil {
		panic(err)
	}
	fmt.Printf("%s: %d vectors\n", runtime.Version(), len(vectors))
}
