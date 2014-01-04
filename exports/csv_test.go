package exports

import (
	"bytes"
	"fmt"
	"github.com/boredomist/mixport/mixpanel"
	"io/ioutil"
	"testing"
)

func TestCSVStreamer(t *testing.T) {
	var expected, output bytes.Buffer
	expected.Write([]byte("event_id,key,value"))

	records := make(chan mixpanel.EventData)

	go func() {
		for i := 0; i < 3; i++ {
			event := make(mixpanel.EventData)
			event[mixpanel.EventIDKey] = fmt.Sprintf("%d", i)
			event["foo"] = "bar,baz"

			records <- event
			expected.Write([]byte(fmt.Sprintf("\n%d,foo,\"bar,baz\"", i)))
		}

		// Ensure nils become empty strings
		event := make(mixpanel.EventData)
		event[mixpanel.EventIDKey] = "niltype"
		event["nil"] = nil

		records <- event
		expected.Write([]byte("\nniltype,nil,\"\"\n"))

		close(records)
	}()

	CSVStreamer(&output, records)

	if !bytes.Equal(output.Bytes(), expected.Bytes()) {
		t.Errorf("got (%s), expected(%s)", output.Bytes(), expected.Bytes())
	}
}

func BenchmarkCSVStreamer(b *testing.B) {
	records := make(chan mixpanel.EventData)

	go func() {
		event := make(mixpanel.EventData)
		event[mixpanel.EventIDKey] = "id"
		event["foo"] = "bar,baz"

		for i := 0; i < b.N; i++ {
			records <- event
		}

		close(records)
	}()

	CSVStreamer(ioutil.Discard, records)
}
