package main
import("fmt";"net";"os";"path/filepath";"time";"github.com/xtls/xray-core/transport/internet/finalmask/xdns")
type packet struct{p []byte;a net.Addr}
type socket struct{read,write chan packet;done chan struct{}}
func newSocket()*socket{return &socket{make(chan packet,16),make(chan packet,16),make(chan struct{})}}
func(c *socket)ReadFrom(p []byte)(int,net.Addr,error){select{case r:=<-c.read:return copy(p,r.p),r.a,nil;case<-c.done:return 0,nil,net.ErrClosed}}
func(c *socket)WriteTo(p []byte,a net.Addr)(int,error){c.write<-packet{append([]byte(nil),p...),a};return len(p),nil}
func(c *socket)Close()error{close(c.done);return nil}
func(c *socket)LocalAddr()net.Addr{return &net.UDPAddr{IP:net.IPv4(127,0,0,1),Port:9000}}
func(c *socket)SetDeadline(time.Time)error{return nil}
func(c *socket)SetReadDeadline(time.Time)error{return nil}
func(c *socket)SetWriteDeadline(time.Time)error{return nil}
func main(){dir:=os.Args[1];os.MkdirAll(dir,0755);raw:=newSocket();client,e:=xdns.NewConnClient(&xdns.Config{Domain:"t.example.com"},raw);if e!=nil{panic(e)};p:=make([]byte,100);for i:=range p{p[i]=byte(i)};client.WriteTo(p,raw.LocalAddr());q:=<-raw.write;os.WriteFile(filepath.Join(dir,"xdns-query.bin"),q.p,0644);incoming:=newSocket();server,e:=xdns.NewConnServer(&xdns.Config{Domain:"t.example.com"},incoming);if e!=nil{panic(e)};incoming.read<-q;buf:=make([]byte,2048);n,addr,e:=server.ReadFrom(buf);if e!=nil||n!=100{panic(fmt.Sprint(n,e))};r:=make([]byte,900);for i:=range r{r[i]=byte(i)};server.WriteTo(r,addr);response:=<-incoming.write;os.WriteFile(filepath.Join(dir,"xdns-response.bin"),response.p,0644);fmt.Println(len(q.p),len(response.p));client.Close();server.Close()}
