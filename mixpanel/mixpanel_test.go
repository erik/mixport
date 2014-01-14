package mixpanel

import (
	"fmt"
	"strings"
	"testing"
	"time"
)

func TestAddSignature(t *testing.T) {
	// TODO: write me
}

func TestMakeArgs(t *testing.T) {
	mix := New("product", "key", "secret")
	date, _ := time.Parse("2006-01-02", "1999-12-31")
	args := mix.makeArgs(date)

	expected := [][]string{
		{"format", "json"},
		{"api_key", "key"},
		{"from_date", "1999-12-31"},
		{"to_date", "1999-12-31"},
	}

	for _, pair := range expected {
		if args[pair[0]][0] != pair[1] {
			t.Errorf("Expected %v, got %s", pair, args[pair[0]][0])
		}
	}
}

func TestTransformEventData(t *testing.T) {
	mix := New("product", "", "")
	input := strings.NewReader(`
{"event": "a0", "properties": {"a": null, "b": "b0", "c": true, "d": ["foo"]}}
{"event": "a1", "properties": {"a": null, "b": "b1", "c": true, "d": ["foo"]}}
{"event": "a2", "properties": {"a": null, "b": "b2", "c": true, "d": ["foo"]}}`)

	output := make(chan EventData)

	go func() {
		if num, err := mix.TransformEventData(input, output); err != nil {
			t.Errorf("raised error: %v", err)
		} else if num != 3 {
			t.Errorf("expected to see 3 records, saw %d", num)
		}
	}()

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
	close(output)
}

func TestTransformEventDataApiError(t *testing.T) {
	mix := New("product", "", "")
	input := strings.NewReader(`{"error": "some api error"}`)

	output := make(chan EventData)

	if num, err := mix.TransformEventData(input, output); err == nil {
		t.Error("Expected error on bad json")
	} else if err.Error() != "product: API error: some api error" {
		t.Errorf("Bad error string: '%s'", err.Error())
	} else if num != 0 {
		t.Errorf("Expected 0 records, got %d", num)
	}
}

func TestTransformEventDataBadJson(t *testing.T) {
	mix := New("product", "", "")
	input := strings.NewReader(`{"event": "a", "properties": {"a": "1"}}
{"event": "bad_json"`)

	output := make(chan EventData, 1)

	if num, err := mix.TransformEventData(input, output); err == nil {
		t.Error("Expected error on bad json")
	} else if num != 1 {
		t.Errorf("Expected 1 record, got %d", num)
	}

	event := <-output
	expected := []struct {
		Name  string
		Value interface{}
	}{
		{"event", "a"},
		{"a", "1"},
	}

	for _, e := range expected {
		if v, ok := event[e.Name]; !ok || v != e.Value {
			t.Errorf("bad value: expected %s=(%v) got %s=(%v)", e.Name, e.Value,
				e.Name, v)
		}
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
	for i := 0; i < b.N; i++ {
		<-output
	}
	close(output)
}
