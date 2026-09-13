package main
import("net";"os";"time";"path/filepath";"fmt";"github.com/xtls/xray-core/transport/internet/finalmask/sudoku")
type capture struct {wire []byte}
func(c *capture)WriteTo(p []byte,a net.Addr)(int,error){c.wire=append([]byte(nil),p...);return len(p),nil}
func(c *capture)ReadFrom(p []byte)(int,net.Addr,error){return copy(p,c.wire),&net.UDPAddr{},nil}
func(c *capture)Close()error{return nil}
func(c *capture)LocalAddr()net.Addr{return &net.UDPAddr{}}
func(c *capture)SetDeadline(time.Time)error{return nil}
func(c *capture)SetReadDeadline(time.Time)error{return nil}
func(c *capture)SetWriteDeadline(time.Time)error{return nil}
func main(){
 payload:=make([]byte,256);for i:=range payload{payload[i]=byte(i)}
 cases:=[]struct{name string;c *sudoku.Config}{
  {"entropy",&sudoku.Config{Password:"reference"}},
  {"ascii",&sudoku.Config{Password:"reference",Ascii:"ascii",PaddingMin:50,PaddingMax:50}},
  {"custom",&sudoku.Config{Password:"reference",CustomTable:"xxppvvvv"}},
  {"rotation",&sudoku.Config{Password:"reference",CustomTables:[]string{"xxppvvvv","xpxpvvvv"},PaddingMin:100,PaddingMax:100}},
 }
 for _,tc:=range cases{raw:=&capture{};conn,err:=sudoku.NewUDPConn(raw,tc.c);if err!=nil{panic(err)};if _,err=conn.WriteTo(payload,&net.UDPAddr{});err!=nil{panic(err)};if err=os.WriteFile(filepath.Join(os.Args[1],tc.name+".bin"),raw.wire,0644);err!=nil{panic(err)};fmt.Println(tc.name,len(raw.wire))}
}
