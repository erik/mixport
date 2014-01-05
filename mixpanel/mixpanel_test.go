package mixpanel

import (
	"fmt"
	"strings"
	"testing"
)

func TestAddSignature(t *testing.T) {
	// TODO: write me
}

func TestMakeArgs(t *testing.T) {
	// TODO: write me
}

func TestTransformEventData(t *testing.T) {
	mix := New("product", "", "")
	input := strings.NewReader(`
{"event": "a0", "properties": {"a": null, "b": "b0", "c": true, "d": ["foo"]}}
{"event": "a1", "properties": {"a": null, "b": "b1", "c": true, "d": ["foo"]}}
{"event": "a2", "properties": {"a": null, "b": "b2", "c": true, "d": ["foo"]}}`)

	output := make(chan EventData)

	go mix.TransformEventData(input, output)

	for i := 0; i < 3; i++ {
		event := <-output

		expected := []struct {
			Name  string
			Value interface{}
		}{
			{"event", fmt.Sprintf("a%d", i)},
			{"a", nil},
			{"b", fmt.Sprintf("b%d", i)},
			{"c", true},
		}

		for _, e := range expected {
			if v, ok := event[e.Name]; !ok || v != e.Value {
				t.Errorf("bad value: expected %s=(%v) got %s=(%v)", e.Name, e.Value,
					e.Name, v)
			}
		}

	}

	if _, ok := <-output; ok {
		t.Error("expected output chan to be closed here")
	}
}

func TestExportDate(t *testing.T) {
	// TODO: write me
}

func BenchmarkTransformEventData(b *testing.B) {
	mix := New("product", "", "")
	input := strings.NewReader(
		strings.Repeat(`{"event": "a2", "properties": {"a": null, "b": "b2", "c": true, "d": ["foo"]}}`, b.N))
	output := make(chan EventData)

	b.ResetTimer()

	go mix.TransformEventData(input, output)
	for _ = range output {
	}
}
