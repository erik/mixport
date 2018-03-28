package exports

import (
	"bytes"
	"encoding/csv"
	"fmt"
	"github.com/erik/mixport/mixpanel"
	"io/ioutil"
	"strconv"
	"testing"
)

func TestCSVColumnStreamer(t *testing.T) {
	columns := [][]string{
		{"a0", "b0", "c0", "d0"},
		{"a1", "b1", "d1"},
		{"aa2", "a2", "b2", "c2", "d2"},
		{"aa3", "a3", "b3", "d3"},
	}

	expected_rows := [][]string{
		{"a", "b", "", "d"},
		{"a", "b", "d"},
		{"", "a", "b", "", "d"},
		{"", "a", "b", "d"},
	}

	output := make([]*bytes.Buffer, 4)
	defs := make(map[string]EventColumnDef)

	expected := make([]string, 4)
	for i := 0; i < 4; i++ {
		output[i] = bytes.NewBuffer(nil)

		defs[strconv.Itoa(i)] = NewEventColumnDef(output[i], columns[i])

		buf := new(bytes.Buffer)

		w := csv.NewWriter(buf)
		w.Write(columns[i])
		w.Write(expected_rows[i])
		w.Flush()

		expected[i] = buf.String()
	}

	events := make([]mixpanel.EventData, 5)

	for i := 0; i < 5; i++ {
		events[i] = make(mixpanel.EventData)
		events[i]["event"] = fmt.Sprintf("%d", i)
		events[i][fmt.Sprintf("a%d", i)] = "a"
		events[i][fmt.Sprintf("b%d", i)] = "b"
		events[i][fmt.Sprintf("c%d", i)] = nil
		events[i][fmt.Sprintf("d%d", i)] = "d"
	}

	records := make(chan mixpanel.EventData, 5)
	for _, ev := range events {
		records <- ev
	}
	close(records)

	CSVColumnStreamer(defs, records)

	for i, ex := range expected {
		if !bytes.Equal(output[i].Bytes(), []byte(ex)) {
			t.Errorf("got (%s), expected(%s)", output[i].Bytes(), ex)
		}
	}
}

func BenchmarkCSVColumnStreamer(b *testing.B) {
	columns := [][]string{
		{"a0", "b0", "c0", "d0"},
		{"a1", "b1", "d1"},
		{"aa2", "a2", "b2", "c2", "d2"},
		{"aa3", "a3", "b3", "d3"},
	}

	defs := make(map[string]EventColumnDef)
	for i := 0; i < 4; i++ {
		defs[strconv.Itoa(i)] = NewEventColumnDef(ioutil.Discard, columns[i])
	}

	// This allocates a ton of memory.
	events := make([]mixpanel.EventData, b.N)

	for i := 0; i < b.N; i++ {
		events[i] = make(mixpanel.EventData)
		events[i]["event"] = fmt.Sprintf("%d", b.N%5)
		events[i][fmt.Sprintf("a%d", b.N%5)] = "a"
		events[i][fmt.Sprintf("b%d", b.N%5)] = "b"
		events[i][fmt.Sprintf("c%d", b.N%5)] = nil
		events[i][fmt.Sprintf("d%d", b.N%5)] = "d"
	}

	records := make(chan mixpanel.EventData, b.N)
	for _, ev := range events {
		records <- ev
	}
	close(records)

	b.ResetTimer()
	CSVColumnStreamer(defs, records)
}
