package main

import (
	"context"
	"fmt"
	api "github.com/enfein/mieru/v3/apis/common"
	"github.com/enfein/mieru/v3/pkg/appctl/appctlpb"
	"github.com/enfein/mieru/v3/pkg/common"
	"github.com/enfein/mieru/v3/pkg/protocol"
	"google.golang.org/protobuf/proto"
	"io"
	"net"
	"strconv"
	"sync"
	"sync/atomic"
	"time"
)

type referenceFactory struct{ count atomic.Int32 }
type referenceListener struct {
	net.Listener
	factory *referenceFactory
}

func (l *referenceListener) Accept() (net.Conn, error) {
	c, e := l.Listener.Accept()
	if e == nil {
		fmt.Printf("UNDERLAY tcp %d\n", l.factory.count.Add(1))
	}
	return c, e
}
func (f *referenceFactory) Listen(ctx context.Context, network, address string) (net.Listener, error) {
	l, e := (&net.ListenConfig{}).Listen(ctx, network, address)
	if e != nil {
		return nil, e
	}
	return &referenceListener{l, f}, nil
}

type referencePackets struct {
	net.PacketConn
	seen    sync.Map
	factory *referenceFactory
}

func (p *referencePackets) ReadFrom(b []byte) (int, net.Addr, error) {
	n, a, e := p.PacketConn.ReadFrom(b)
	if e == nil {
		if _, loaded := p.seen.LoadOrStore(a.String(), true); !loaded {
			fmt.Printf("UNDERLAY udp %d\n", p.factory.count.Add(1))
		}
	}
	return n, a, e
}
func (f *referenceFactory) ListenPacket(ctx context.Context, network, address string) (net.PacketConn, error) {
	p, e := (&net.ListenConfig{}).ListenPacket(ctx, network, address)
	if e != nil {
		return nil, e
	}
	return &referencePackets{PacketConn: p, factory: f}, nil
}
func runServer(mode, port string) {
	n, e := strconv.Atoi(port)
	check(e)
	var local net.Addr = &net.TCPAddr{IP: net.IPv4(127, 0, 0, 1), Port: n}
	transport := common.StreamTransport
	if mode == "udp" {
		local = &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1), Port: n}
		transport = common.PacketTransport
	}
	factory := &referenceFactory{}
	mux := protocol.NewMux(false).SetServerUsers(map[string]*appctlpb.User{"mux-user": {Name: proto.String("mux-user"), Password: proto.String("mux-password")}}).
		SetEndpoints([]protocol.UnderlayProperties{protocol.NewUnderlayProperties(1400, transport, local, nil)}).
		SetStreamListenerFactory(factory).SetPacketListenerFactory(factory)
	check(mux.Start())
	defer mux.Close()
	fmt.Println("READY")
	fmt.Printf("REFERENCE %s %s\n", referenceVersion, referenceRevision)
	for {
		s, e := mux.Accept()
		if e != nil {
			return
		}
		go func() {
			defer s.Close()
			if e := serveReferenceSession(s); e != nil {
				fmt.Printf("SESSION_END %v\n", e)
			}
		}()
	}
}
func serveReferenceSession(s net.Conn) error {
	s.SetDeadline(time.Now().Add(90 * time.Second))
	header := make([]byte, 4)
	if _, e := io.ReadFull(s, header); e != nil {
		return e
	}
	if header[0] != 5 {
		return fmt.Errorf("bad socks request")
	}
	var host string
	switch header[3] {
	case 1:
		b := make([]byte, 4)
		if _, e := io.ReadFull(s, b); e != nil {
			return e
		}
		host = net.IP(b).String()
	case 4:
		b := make([]byte, 16)
		if _, e := io.ReadFull(s, b); e != nil {
			return e
		}
		host = net.IP(b).String()
	case 3:
		b := make([]byte, 1)
		if _, e := io.ReadFull(s, b); e != nil {
			return e
		}
		h := make([]byte, int(b[0]))
		if _, e := io.ReadFull(s, h); e != nil {
			return e
		}
		host = string(h)
	default:
		return fmt.Errorf("bad socks address")
	}
	port := make([]byte, 2)
	if _, e := io.ReadFull(s, port); e != nil {
		return e
	}
	if _, e := s.Write([]byte{5, 0, 0, 1, 0, 0, 0, 0, 0, 0}); e != nil {
		return e
	}
	if header[1] == 1 {
		target, e := net.DialTimeout("tcp", net.JoinHostPort(host, strconv.Itoa(int(port[0])*256+int(port[1]))), 5*time.Second)
		if e != nil {
			return e
		}
		defer target.Close()
		done := make(chan struct{})
		go func() {
			io.Copy(target, s)
			if t, ok := target.(*net.TCPConn); ok {
				t.CloseWrite()
			}
			close(done)
		}()
		_, e = io.Copy(s, target)
		return e
	}
	if header[1] != 3 {
		return fmt.Errorf("bad socks command")
	}
	target, e := net.ListenUDP("udp", nil)
	if e != nil {
		return e
	}
	defer target.Close()
	packets := api.NewUDPAssociateWrapper(api.NewPacketOverStreamTunnel(s))
	buffer := make([]byte, 65535)
	for {
		n, addr, e := packets.ReadFrom(buffer)
		if e != nil {
			return e
		}
		peer, e := net.ResolveUDPAddr("udp", addr.String())
		if e != nil {
			return e
		}
		if _, e = target.WriteToUDP(buffer[:n], peer); e != nil {
			return e
		}
		target.SetReadDeadline(time.Now().Add(5 * time.Second))
		n, source, e := target.ReadFromUDP(buffer)
		if e != nil {
			return e
		}
		if _, e = packets.WriteTo(buffer[:n], source); e != nil {
			return e
		}
	}
}
