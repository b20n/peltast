# Avern: metrics for operators

## Schema

Conceptually, Avern's schema is very similar to OpenTSDB's: users can store
datapoints for as many metrics as they want, at whatever rate they may desire.
Each datapoint can further be configured with an arbitrary number of key-value
pairs ("tags") by which that datapoint can be further specified. For example, a
typical datapoint might look like the following (pretty-printed in a JSON-like
format for readability):

    {
        "metric": "network.bytes",
        "timestamp": 1364604206,
        "value": 120041,
        "tags": {
            "host": "db1.cluster001",
            "interface": "eth0",
            "direction": "in"
        }
    }

The resulting on-disk layout of data will be ordered by metric name, then
timestamp, then tag pairs. In other words, _all_ data for a single metric is
stored sequentially on disk. This means that it's important to choose metric
names wisely: query performance depends on it.

A rule of thumb: anything that will generally be queried with a wildcard
operator should be in a tag, not the metric name. If you frequently examine disk
space across all of your hosts at once, don't put host names in the metric name.

It's certainly possible to specify all tag information in the metric name (as
one would in Graphite,) but query performance will likely suffer. Furthermore,
each metric is managed by a process in the Erlang VM, so it's probably best not
to have too many millions of them.

## Storage intervals

Here again Avern takes the OpenTSDB approach: no rollup, aggregation, or
downsampling of inserted data is ever done. Each datapoint is stored with its
original timestamp forever.

It should be observed that Avern doesn't have a distributed database in which to
store its data, so users probably shouldn't keep their data forever.

## Retention: in-memory vs on-disk

Each metric is configured with two retention window: one for data in-memory
and one for data on-disk. The system will regularly delete data that falls out
of each window.

Either of the in-memory or on-disk windows can be set to zero. In the disk case,
this will cause the system to only buffer writes in memory, for example to
provide a fast high-resolution dataset. If memory retention is set to zero, the
system will still consume the RAM necessary to optimize write patterns, but it
won't retain data past that point.

A combination of nonzero values for both disk and memory retention periods can
be used to ensure low-load reads for recent data. Avern will always attempt to
fulfill queries from cache before reading from disk.

TODO: delete data from inactive metrics.

## Query interface

### WAT

