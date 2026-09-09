// Run inside apernet/quic-go 184d081eef3e. No reference algorithm is replaced.
package quic

import (
	"encoding/csv"
	"fmt"
	"os"
	"strconv"
	"testing"
	"time"

	"github.com/apernet/quic-go/internal/monotime"
	"github.com/apernet/quic-go/internal/protocol"
	"github.com/apernet/quic-go/internal/utils"
)

func TestZeroReceiveWindowReference(t *testing.T) {
	input, err := os.Open(os.Getenv("ZERO_WINDOW_EVENTS"))
	if err != nil {
		t.Fatal(err)
	}
	defer input.Close()
	rows, err := csv.NewReader(input).ReadAll()
	if err != nil {
		t.Fatal(err)
	}
	out, err := os.Create(os.Getenv("ZERO_WINDOW_EXPECTED"))
	if err != nil {
		t.Fatal(err)
	}
	defer out.Close()
	w := csv.NewWriter(out)
	w.Write([]string{"case", "event", "size", "limit", "update", "epoch_read"})
	var c receiveFlowController
	var scenario string
	for i, row := range rows[1:] {
		n := make([]int64, 5)
		for j := range n {
			n[j], err = strconv.ParseInt(row[j+1], 10, 64)
			if err != nil {
				t.Fatal(err)
			}
		}
		if scenario != row[0] {
			scenario = row[0]
			c = receiveFlowController{receiveWindow: protocol.ByteCount(n[3]), receiveWindowSize: protocol.ByteCount(n[3]), maxReceiveWindowSize: protocol.ByteCount(n[4]), rttStats: &utils.RTTStats{}}
			c.startNewAutoTuningEpoch(monotime.Time(time.Second))
		}
		c.rttStats.SetInitialRTT(time.Duration(n[2]))
		c.addBytesRead(protocol.ByteCount(n[1]))
		update := c.getWindowUpdate(monotime.Time(time.Second + time.Duration(n[0])))
		w.Write([]string{scenario, fmt.Sprint(i), fmt.Sprint(c.receiveWindowSize), fmt.Sprint(c.receiveWindow), fmt.Sprint(update), fmt.Sprint(c.epochStartOffset)})
	}
	w.Flush()
	if err := w.Error(); err != nil {
		t.Fatal(err)
	}
}
