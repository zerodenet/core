// A privilege-free wireguard-go peer with a userspace TCP/UDP echo stack.
// The Rust interop test supplies fixed test keys through the environment.
package main

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/netip"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"golang.zx2c4.com/wireguard/conn"
	"golang.zx2c4.com/wireguard/device"
	"golang.zx2c4.com/wireguard/tun/netstack"
)

func checkClientEcho(network *netstack.Net, endpoint netip.AddrPort) error {
	payload := []byte("wireguard-go-to-zero-payload")
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	tcp, err := network.DialContextTCPAddrPort(ctx, endpoint)
	if err != nil {
		return fmt.Errorf("TCP dial %s: %w", endpoint, err)
	}
	defer tcp.Close()
	_ = tcp.SetDeadline(time.Now().Add(15 * time.Second))
	if _, err := tcp.Write(payload); err != nil {
		return fmt.Errorf("TCP write %s: %w", endpoint, err)
	}
	received := make([]byte, len(payload))
	if _, err := io.ReadFull(tcp, received); err != nil {
		return fmt.Errorf("TCP read %s: %w", endpoint, err)
	}
	if !bytes.Equal(received, payload) {
		return fmt.Errorf("TCP payload mismatch for %s", endpoint)
	}
	udp, err := network.DialUDPAddrPort(netip.AddrPort{}, endpoint)
	if err != nil {
		return fmt.Errorf("UDP dial %s: %w", endpoint, err)
	}
	defer udp.Close()
	_ = udp.SetDeadline(time.Now().Add(15 * time.Second))
	if _, err := udp.Write(payload); err != nil {
		return fmt.Errorf("UDP write %s: %w", endpoint, err)
	}
	count, err := udp.Read(received)
	if err != nil {
		return fmt.Errorf("UDP read %s: %w", endpoint, err)
	}
	if !bytes.Equal(received[:count], payload) {
		return fmt.Errorf("UDP payload mismatch for %s", endpoint)
	}
	return nil
}

func required(name string) string {
	value := os.Getenv(name)
	if value == "" {
		panic(name + " is required")
	}
	return value
}

func valueOr(name, fallback string) string {
	if value := os.Getenv(name); value != "" {
		return value
	}
	return fallback
}

func dnsAnswer(query []byte, address netip.Addr) []byte {
	if len(query) < 17 || query[4] != 0 || query[5] != 1 || !address.Is4() {
		return nil
	}
	end := 12
	for end < len(query) {
		length := int(query[end])
		end++
		if length == 0 {
			break
		}
		if length > 63 || end+length > len(query) {
			return nil
		}
		end += length
	}
	if end+4 > len(query) || query[end] != 0 || query[end+1] != 1 || query[end+2] != 0 || query[end+3] != 1 {
		return nil
	}
	end += 4
	response := make([]byte, 12, end+16)
	copy(response[:2], query[:2])
	response[2], response[3] = 0x81, 0x80
	response[5], response[7] = 1, 1
	response = append(response, query[12:end]...)
	response = append(response, 0xc0, 0x0c, 0, 1, 0, 1, 0, 0, 0, 60, 0, 4)
	return append(response, address.AsSlice()...)
}

func main() {
	privateKey := required("WG_PRIVATE_HEX")
	peerPublicKey := required("WG_PEER_PUBLIC_HEX")
	clientMode := os.Getenv("WG_MODE") == "client"
	port, err := strconv.Atoi(required("WG_PORT"))
	if err != nil || port < 1 || port > 65535 {
		panic("invalid WG_PORT")
	}
	echoPort, err := strconv.Atoi(required("WG_ECHO_PORT"))
	if err != nil || echoPort < 1 || echoPort > 65535 {
		panic("invalid WG_ECHO_PORT")
	}
	addresses := []netip.Addr{
		netip.MustParseAddr(valueOr("WG_ADDR4", "10.77.0.2")),
		netip.MustParseAddr(valueOr("WG_ADDR6", "fd77::2")),
	}
	if address := os.Getenv("WG_DNS_ADDR4"); address != "" {
		addresses = append(addresses, netip.MustParseAddr(address))
	}
	tun, network, err := netstack.CreateNetTUN(addresses, nil, 1420)
	if err != nil {
		panic(err)
	}
	peer := device.NewDevice(tun, conn.NewDefaultBind(), device.NewLogger(device.LogLevelError, "wireguard-go-reference: "))
	defer peer.Close()
	configuration := strings.Join([]string{
		"private_key=" + privateKey,
		fmt.Sprintf("listen_port=%d", port),
		"public_key=" + peerPublicKey,
	}, "\n")
	if clientMode {
		configuration += "\nallowed_ip=0.0.0.0/0\nallowed_ip=::/0\nendpoint=" + required("WG_ENDPOINT") + "\npersistent_keepalive_interval=1\n"
	} else {
		configuration += "\nallowed_ip=" + valueOr("WG_ALLOWED4", "10.77.0.1/32") +
			"\nallowed_ip=" + valueOr("WG_ALLOWED6", "fd77::1/128") +
			"\npersistent_keepalive_interval=0\n"
	}
	if err := peer.IpcSet(configuration); err != nil {
		panic(err)
	}
	if !clientMode {
		for _, address := range addresses {
			endpoint := netip.AddrPortFrom(address, uint16(echoPort))
			tcp, err := network.ListenTCPAddrPort(endpoint)
			if err != nil {
				panic(err)
			}
			defer tcp.Close()
			go func() {
				for {
					stream, err := tcp.Accept()
					if err != nil {
						return
					}
					go func() {
						defer stream.Close()
						_, _ = io.Copy(stream, stream)
					}()
				}
			}()
			udp, err := network.ListenUDPAddrPort(endpoint)
			if err != nil {
				panic(err)
			}
			defer udp.Close()
			go func() {
				buffer := make([]byte, 65535)
				for {
					count, sender, err := udp.ReadFrom(buffer)
					if err != nil {
						return
					}
					_, _ = udp.WriteTo(buffer[:count], sender)
				}
			}()
		}
		if dnsAddress := os.Getenv("WG_DNS_ADDR4"); dnsAddress != "" {
			endpoint := netip.AddrPortFrom(netip.MustParseAddr(dnsAddress), 53)
			dns, err := network.ListenUDPAddrPort(endpoint)
			if err != nil {
				panic(err)
			}
			defer dns.Close()
			answer := netip.MustParseAddr(valueOr("WG_DNS_ANSWER4", "10.77.0.2"))
			go func() {
				buffer := make([]byte, 4096)
				for {
					count, sender, err := dns.ReadFrom(buffer)
					if err != nil {
						return
					}
					if response := dnsAnswer(buffer[:count], answer); response != nil {
						_, _ = dns.WriteTo(response, sender)
					}
				}
			}()
		}
	}
	if err := peer.Up(); err != nil {
		panic(err)
	}
	if clientMode {
		for _, name := range []string{"WG_TARGET4", "WG_TARGET6"} {
			value := os.Getenv(name)
			if value == "" {
				continue
			}
			endpoint := netip.MustParseAddrPort(value)
			if err := checkClientEcho(network, endpoint); err != nil {
				panic(err)
			}
		}
		fmt.Println("SUCCESS")
		return
	}
	fmt.Println("READY")
	stop := make(chan os.Signal, 1)
	signal.Notify(stop, syscall.SIGINT, syscall.SIGTERM)
	<-stop
}
