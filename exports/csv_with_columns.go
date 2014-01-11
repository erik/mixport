package exports

import (
	"encoding/csv"
	"fmt"
	"github.com/boredomist/mixport/mixpanel"
	"io"
)

// TODO: Rename this file to something better. "csv_with_columns" is overly
//       verbose and not terribly descriptive.

// EventDef ... TODO: write me
type EventDef struct {
	writer  *csv.Writer
	columns []string
}

// NewEventDef ... TODO: write me
func NewEventDef(w io.Writer, columns []string) *EventDef {
	return &EventDef{
		writer:  csv.NewWriter(w),
		columns: columns,
	}
}

// CSVColumnStreamer ... TODO: write me
func CSVColumnStreamer(defs map[string]*EventDef, records <-chan mixpanel.EventData) {

	for _, def := range defs {
		// Write the column names as CSV header
		def.writer.Write(def.columns)
	}

	for record := range records {
		event := record["event"].(string)

		// We simply ignore events we don't have column definitions for.
		if def, ok := defs[event]; ok {
			cols := make([]string, len(def.columns))

			for i, col := range def.columns {
				switch value := record[col]; value.(type) {
				case nil:
					cols[i] = ""
				default:
					cols[i] = fmt.Sprintf("%v", value)
				}
			}

			def.writer.Write(cols)
		}
	}

	// Flush any buffered data to the underlying io.Writer
	for _, def := range defs {
		def.writer.Flush()
	}
}
