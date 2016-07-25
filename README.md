# mixport [![Build Status](https://travis-ci.org/erik/mixport.png?branch=master)](https://travis-ci.org/erik/mixport)

`mixport` utilizes Mixpanel's
[data export API](https://mixpanel.com/docs/api-documentation/exporting-raw-data-you-inserted-into-mixpanel)
to download event data and make it far easier to consume and store.

Mixpanel exposes this data as a bunch of JSON blobs in the form `{"event":
"EventName", "properties": {...}}`, where the `properties` map contains all of
the arbitrary dimensions attached to the data point. There isn't a defined
schema, property keys and event types can come and go, data types can
change. Such is JSON.

This does however make storing the data in a SQL database somewhat of a chore,
since you can really only try to guess the valid types (I wrote and maintained
this application as the precursor to `mixport`, believe me, this isn't fun or
very logical).

`mixport` is really only concerned with the export portion of the problem, but
can do some streaming transforms on the data to make it more digestible.

## Building

*Requires Go >= 1.1 to compile.*

Using `go get`:

```bash
$ go get github.com/erik/mixport
```

## Usage

First, let's set up a configuration file:

```bash
$ cp mixport.conf{.example,}
$ $EDITOR mixport.conf
```

There are several comments in the example configuration which should give a
decent explanation of what's required and available to configure.

Let's start off by just downloading yesterday's data for all the products
specified in the configuration:

```bash
$ ./mixport
```

That's it! If your configuration isn't in `.` or is named something other than
`mixport.conf`, you need to specify it with `-c path/to/config`.

If you want to just export a specific subset of the products defined in the
configuration, the `--products` flags will be useful:

```bash
$ ./mixport --products=Product1,Product2,...,ProductN
```

If you want to download data for a single day (that isn't yesterday), use the
`-d` flag:

```bash
$ ./mixport -d 2013/12/31
```

If you want to download multiple days of data, try this:

```bash
$ ./mixport -r 2013/12/31-2014/02/06
```

And that's about all you need to know to get started.

For a full listing of command arguments available, use `./mixport --help`.

## Export formats

There are currently 3 exportable formats:

### Schemaless CSV

This format is meant to ultimately be COPY friendly for SQL databases and gets
around the problems mentioned above about data types and column inconsistency
by being schemaless. The output is simply: `event_id,key,value`

So for example, a data point input of `{"event": "Foo", "properties": {"bar":
"baz", ...}}` becomes this:

```CSV
event_id,key,value
some_UUID,event,Foo
some_UUID,bar,baz
...
```

If you can insert this data into a SQL table where the `event_id`, `key`, and
`value` columns are all `VARCHAR` (or equivalent). You can then `GROUP BY` the
`event_id` to get a row.

This subverts a bunch of advantages to SQL, so make sure you know this is the
best way.

Note that this exploded format does get pretty big on disk, and hugely benefits
from GZIP compression.

### CSV with columns

Like schemaless CSV, this format is intended to be COPY friendly for SQL
databases. Unlike the previous format, however, this export requires that
the columns must be explicitly specified for each event.

Instead of writing to a single file for each product, this export function will
create an output file for each event of each product.

The configuration for this is in the form of a single JSON file, specified by
the `columns` variable under the `columns` configuration section.

The format for this file should be a single map in the following format:

```javascript
{
  "product1": {
    "event1": ["col1", "col2", "col3", ... ],
    ...
  },
  ...
}
```

As an example, let's say we have this configuration:

```javascript
{
  "productA": {
    "foo": ["a", "b", "c"]
    "bar": ["a", "b"]
  }
}
```

Now we see the following series of events instances for `productA`:

```javascript
{"event": "foo", "properties": {"a": 1, "c": 3}}
{"event": "bar", "properties": {"a": 1, "b": 2, "c": 3}}
{"event": "baz", "properties": {"a": 1, "b": 2, "c": 3}}
```

For `foo`, the `"b"` property is missing from the event data, so the column is
assigned a null value:

```csv
a,b,c
1,,3
```

For `bar`, the `"c"` property is not explicitly listed as a column in the
configuration, so it is not included in the output:

```csv
a,b
1,2
```

And because we don't have a definition for the `baz` event type, the event is
dropped completely.

### Flattened JSON

So as I've just explained, the input is in the form `{"event": "Foo",
"properties": {"bar": "baz", ...}}`.

This export basically folds the 'properties' map up into the top level:

`{"event": "Foo", "bar": "baz", ... }`

It's possible to preserve the original hierarchy by writing a new (or modifying
this) export writer, but I don't really see why you would want to.

Like the CSV export, this is compressed by GZIP very efficiently. 85-90%
compression ratio is typical for data I've looked at.

## Mixpanel to X without hitting disk

`mixport` can write to [named pipes](http://en.wikipedia.org/wiki/Named_pipe)
instead of regular files, which can be incredibly useful to avoid temporary
files.

The basic formula is to set `fifo=true` in the `[json]` or `[csv]` sections of
the configuration file and then just pretend that the named pipe has all of the
data already written to it, like so:

```bash
$ ./mixport &
$ for f in /mixport/output/dir/*; do psql -c "COPY TABLE ... FROM $f" & done
```

If you want to pipe things to S3 without hitting disk, I've had success with
the [s3stream gem](https://github.com/kindkid/s3stream):

```bash
$ ./mixport &
$ for f in /mixport/output/dir/*; do s3stream store mybucket $f < $f & done
```

Neat.
