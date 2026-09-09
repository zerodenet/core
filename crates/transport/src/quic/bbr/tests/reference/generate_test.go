// Copy into pinned core/internal/congestion/bbr, then run TestZeroReferenceTrace.
package bbr

import (
	"encoding/csv"
	"fmt"
	"github.com/apernet/quic-go/congestion"
	"github.com/apernet/quic-go/monotime"
	"os"
	"strconv"
	"testing"
	"time"
)

type zeroClock struct{}

func (zeroClock) Now() monotime.Time { return monotime.Time(time.Second) }

type zeroRTT struct{ min time.Duration }

func (r *zeroRTT) MinRTT() time.Duration                { return r.min }
func (r *zeroRTT) LatestRTT() time.Duration             { return r.min }
func (r *zeroRTT) SmoothedRTT() time.Duration           { return r.min }
func (*zeroRTT) MeanDeviation() time.Duration           { return 0 }
func (*zeroRTT) MaxAckDelay() time.Duration             { return 0 }
func (*zeroRTT) PTO(bool) time.Duration                 { return time.Second }
func (*zeroRTT) UpdateRTT(time.Duration, time.Duration) {}
func (*zeroRTT) SetMaxAckDelay(time.Duration)           {}
func (*zeroRTT) SetInitialRTT(time.Duration)            {}

func TestZeroReferenceTrace(t *testing.T) {
	input, err := os.Open(os.Getenv("ZERO_BBR_EVENTS"))
	if err != nil {
		t.Fatal(err)
	}
	defer input.Close()
	events, err := csv.NewReader(input).ReadAll()
	if err != nil {
		t.Fatal(err)
	}
	output, err := os.Create(os.Getenv("ZERO_BBR_EXPECTED"))
	if err != nil {
		t.Fatal(err)
	}
	defer output.Close()
	w := csv.NewWriter(output)
	defer w.Flush()
	w.Write([]string{"profile", "event", "mode", "window", "pacing", "bandwidth", "round", "full", "recovery", "min_rtt_us", "ack_height"})
	for _, profile := range []Profile{ProfileStandard, ProfileConservative, ProfileAggressive} {
		b := newBbrSender(zeroClock{}, 1200, 38400, 24000000, profile)
		rtt := &zeroRTT{}
		b.SetRTTStatsProvider(rtt)
		var acks []congestion.AckedPacketInfo
		var lost []congestion.LostPacketInfo
		var removed congestion.ByteCount
		for i, row := range events[1:] {
			n := make([]int64, 4)
			for j := range n {
				n[j], err = strconv.ParseInt(row[j+1], 10, 64)
				if err != nil {
					t.Fatal(err)
				}
			}
			now := monotime.Time(time.Second + time.Duration(n[0])*time.Microsecond)
			switch row[0] {
			case "S":
				b.OnPacketSent(now, congestion.ByteCount(n[3]), congestion.PacketNumber(n[1]), congestion.ByteCount(n[2]), true)
			case "A":
				acks = append(acks, congestion.AckedPacketInfo{PacketNumber: congestion.PacketNumber(n[1]), BytesAcked: congestion.ByteCount(n[2])})
				removed += congestion.ByteCount(n[2])
			case "L":
				lost = append(lost, congestion.LostPacketInfo{PacketNumber: congestion.PacketNumber(n[1]), BytesLost: congestion.ByteCount(n[2])})
				removed += congestion.ByteCount(n[2])
			case "M":
				b.SetMaxDatagramSize(congestion.ByteCount(n[1]))
			case "F":
				rtt.min = time.Duration(n[3]) * time.Microsecond
				before := b.mode
				b.OnCongestionEventEx(congestion.ByteCount(n[2])+removed, now, acks, lost)
				// Fix only the randomized entry offset for reproducible cross-language traces.
				if b.mode == bbrModeProbeBw && before != bbrModeProbeBw {
					b.cycleCurrentOffset = 2
					b.pacingGain = 1
					b.calculatePacingRate(0)
				}
				w.Write([]string{string(profile), fmt.Sprint(i), fmt.Sprint(b.mode), fmt.Sprint(b.GetCongestionWindow()), fmt.Sprint(b.bandwidthForPacer()), fmt.Sprint(b.bandwidthEstimate()), fmt.Sprint(b.roundTripCount), fmt.Sprint(b.isAtFullBandwidth), fmt.Sprint(b.recoveryState), fmt.Sprint(b.minRtt.Microseconds()), fmt.Sprint(b.sampler.MaxAckHeight())})
				acks = nil
				lost = nil
				removed = 0
			}
		}
	}
}
