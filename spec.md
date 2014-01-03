# mixpanel-export v2

This version of the exporter will make use of Amazon Kinesis to transform the
JSON blobs Mixpanel returns its raw data export API into SQL with effectively
no manual intervention.

Basically, this should be set and forget.

## Steps

### Exporter

1. Process event data from Mixpanel line by line (received in the form
   `{"event": ..., "properties": {"k": v, ...}}`)
2. Transform data into simplified `{"k": v, ...}`
2. Attach `{"product": "PRODUCTNAME"}` and a UUID event_id to the JSON blob.
3. Move JSON blob into Kinesis stream/

### Transformer

1. Receive record(s) from Kinesis.
2. Decompose record into multiple SQL rows based on `key: value` mappings,
   using the `event_id` field as the Id.

   i.e.:

   ```python
       for key, value in record:
           INSERT INTO mixpanel (id, key, value) VALUES (record['event_id'], key, value)
   ```
