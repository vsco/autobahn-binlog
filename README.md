# autobahn-binlog

`autobahn-binlog` is a Go package for tailing MySQL v5.5.x binary replication streams.
It can be used to create a real-time stream of immutable events in [Apache Kafka](http://kafka.apache.org), write data directly to other databases, or something else.

There are two common ways to capture database changes without doing dual writes on the application side: (1) polling for changes and (2) direct log integration with the database. Option (1), polling, is straightforward to implement but has sizeable downsides — repeated polling imposes unnecessary load on our databases, and can cause lossiness (long-running transactions might cause commits out of timestamp order, multiple changes to a row in one polling period get coalesced into one, and row deletions are hard to capture). Option (2) solves these problems by interacting directly with internal database replication protocols, which capture each change and have lower overhead than polling. This library implements option (2) by creating and tapping into a MySQL-replication-protocol network stream.

## Installation

```
go get github.com/vsco/autobahn-binlog
```

## Requirements

- [Go](http://golang.org/doc/install)
- MySQL v5.5.x
  - with row-based replication on (`binlog_format = row`)
