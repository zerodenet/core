// Fixed-version official Mieru TCP-underlay interoperability probe.
// Uses the reference implementation's exported protocol and UDP APIs.
package main

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net"
	"os"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	api "github.com/enfein/mieru/v3/apis/common"
	"github.com/enfein/mieru/v3/pkg/cipher"
	"github.com/enfein/mieru/v3/pkg/protocol"
)

type countedDialer struct{ count atomic.Int32 }

var referenceVersion = "unverified"
var referenceRevision = "unverified"

func (d *countedDialer) DialContext(ctx context.Context, network, addr string) (net.Conn, error) {
	d.count.Add(1)
	return (&net.Dialer{Timeout: 5 * time.Second}).DialContext(ctx, network, addr)
}
func (d *countedDialer) ListenPacket(ctx context.Context, network, laddr, raddr string) (net.PacketConn, error) {
	d.count.Add(1)
	return (&net.ListenConfig{}).ListenPacket(ctx, network, "127.0.0.1:0")
}
func check(err error) {
	if err != nil {
		panic(err)
	}
}
func main() {
	if len(os.Args) == 4 && os.Args[1] == "server" {
		runServer(os.Args[2], os.Args[3])
		return
	}
	mode := "tcp"
	if len(os.Args) == 5 {
		mode = os.Args[4]
		os.Args = os.Args[:4]
	}

	if len(os.Args) != 4 {
		panic("usage: probe server-port tcp-echo-port udp-echo-port")
	}
	target, _ := strconv.Atoi(os.Args[2])
	udpPort, _ := strconv.Atoi(os.Args[3])
	block, err := cipher.BlockCipherFromPassword(cipher.HashPassword([]byte("mux-password"), []byte("mux-user")), mode == "udp")
	check(err)
	block.SetBlockContext(cipher.BlockContext{UserName: "mux-user"})
	dialer := &countedDialer{}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var u protocol.Underlay
	if mode == "udp" {
		u, err = protocol.NewPacketUnderlay(ctx, dialer, net.DefaultResolver, "udp", "127.0.0.1:"+os.Args[1], 1400, block, nil)
	} else {
		u, err = protocol.NewStreamUnderlay(ctx, dialer, net.DefaultResolver, nil, "tcp", "127.0.0.1:"+os.Args[1], 1500, block, nil)
	}
	check(err)
	defer u.Close()
	go u.RunEventLoop(ctx)
	session := func(id uint32) *protocol.Session {
		s := protocol.NewSession(id, true, 1500, nil, nil)
		check(u.AddSession(s, nil))
		check(s.SetDeadline(time.Now().Add(60 * time.Second)))
		return s
	}
	slow := session(1)
	_, err = slow.Write([]byte{5})
	check(err) // Must not hold up other session handshakes.
	start := make(chan struct{})
	var ready sync.WaitGroup
	ready.Add(6)
	var wg sync.WaitGroup
	errors := make(chan error, 6)
	for i := 0; i < 6; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			s := session(uint32(100 + i))
			defer s.Close()
			if os.Getenv("MIERU_INTEROP_TRACE") != "" {
				done := make(chan struct{})
				defer close(done)
				go func() {
					ticker := time.NewTicker(5 * time.Second)
					defer ticker.Stop()
					for {
						select {
						case <-done:
							return
						case <-ticker.C:
							fmt.Fprintf(os.Stderr, "SESSION %s\n", s.ToSessionInfo())
						}
					}
				}()
			}
			cmd := byte(1)
			port := target
			if i >= 4 {
				cmd = 3
				port = 0
			}
			request := []byte{5, cmd, 0, 1, 127, 0, 0, 1, byte(port >> 8), byte(port)}
			if _, err := s.Write(request); err != nil {
				panic(err)
			}
			reply := make([]byte, 10)
			_, err := io.ReadFull(s, reply)
			check(err)
			if reply[0] != 5 || reply[1] != 0 {
				panic(fmt.Sprintf("bad reply %v", reply))
			}
			ready.Done()
			<-start
			for round := 0; round < 2; round++ {
				size := 1048577
				if i >= 4 {
					size = 1600
				}
				payload := bytes.Repeat([]byte{byte(i*13 + round + 1)}, size)
				response := make([]byte, size)
				if i < 4 {
					_, err = s.Write(payload)
					if err != nil {
						errors <- err
						return
					}
					_, err = io.ReadFull(s, response)
				} else {
					packet := api.NewUDPAssociateWrapper(api.NewPacketOverStreamTunnel(s))
					_, err = packet.WriteTo(payload, &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1), Port: udpPort})
					if err != nil {
						errors <- err
						return
					}
					var n int
					n, _, err = packet.ReadFrom(response)
					if n != size {
						errors <- fmt.Errorf("session %d round %d UDP length %d != %d: %v", i, round, n, size, err)
						return
					}
				}
				if err != nil {
					errors <- err
					return
				}
				if !bytes.Equal(response, payload) {
					errors <- fmt.Errorf("session %d round %d corrupt", i, round)
					return
				}
			}
		}(i)
	}
	ready.Wait()
	check(slow.Close())
	close(start)
	wg.Wait()
	close(errors)
	for err := range errors {
		check(err)
	}
	// Existing sessions have closed. Open another on the same underlay.
	s := session(999)
	defer s.Close()
	_, err = s.Write([]byte{5, 1, 0, 1, 127, 0, 0, 1, byte(target >> 8), byte(target)})
	check(err)
	reply := make([]byte, 10)
	_, err = io.ReadFull(s, reply)
	check(err)
	_, err = s.Write([]byte("after-close"))
	check(err)
	response := make([]byte, 11)
	_, err = io.ReadFull(s, response)
	check(err)
	if string(response) != "after-close" {
		panic("new session corrupted")
	}
	if dialer.count.Load() != 1 {
		panic("expected exactly one TCP underlay")
	}
	fmt.Printf("PASS: official %s (%s); one %s underlay; 4 TCP + 2 UDP; two rounds; stalled and closed session isolation; later reuse\n", referenceVersion, referenceRevision, mode)
}
