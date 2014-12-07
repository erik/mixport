package exports

import (
	"bytes"
	"encoding/csv"
	"fmt"
	"github.com/erik/mixport/mixpanel"
	"io/ioutil"
	"testing"
)

func TestCSVStreamer(t *testing.T) {
	var expected, output bytes.Buffer
	expected.Write([]byte("event_id,key,value\n"))

	records := make(chan mixpanel.EventData, 4)

	for i := 0; i < 3; i++ {
		event := make(mixpanel.EventData)
		event[mixpanel.EventIDKey] = fmt.Sprintf("%d", i)
		event["foo"] = "bar,baz"

		records <- event
		expected.Write([]byte(fmt.Sprintf("%d,foo,\"bar,baz\"\n", i)))
	}

	// Ensure nils become empty strings
	event := make(mixpanel.EventData)
	event[mixpanel.EventIDKey] = "niltype"
	event["nil"] = nil

	records <- event

	// The way empty strings are handled changed between go 1.3 and 1.4, so
	// can't rely on a static string.
	w := csv.NewWriter(&expected)
	w.Write([]string{"niltype", "nil", ""})
	w.Flush()

	close(records)

	CSVStreamer(&output, records)

	if !bytes.Equal(output.Bytes(), expected.Bytes()) {
		t.Errorf("got (%s), expected(%s)", output.Bytes(), expected.Bytes())
	}
}

func BenchmarkCSVStreamer(b *testing.B) {
	records := make(chan mixpanel.EventData, b.N)

	event := make(mixpanel.EventData)
	event[mixpanel.EventIDKey] = "id"
	event["foo"] = "bar,baz"

	for i := 0; i < b.N; i++ {
		records <- event
	}

	close(records)

	b.ResetTimer()
	CSVStreamer(ioutil.Discard, records)
}
