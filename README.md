Redis-Compatible Server (Java)

A small, educational Redis-compatible server. I focused on clarity and correctness while implementing the core protocol plus several extensions.


---

Contents

Replication

Transactions

Streams

RDB Persistence

Geospatial Commands

Sorted Sets

Pub/Sub

Lists



---

Replication

Built: primary↔replica replication with Redis-style handshake, full resync (RDB bootstrap), live command streaming, per-replica ACKs, and a basic WAIT.

Scope

Primary/replica roles with INFO replication reporting (role:master / role:slave, static master_replid, offset).

FULLRESYNC on every replica connect (no backlog/partial resync yet).

RDB snapshot transfer (raw bytes) → then stream write commands.

Multiple replicas, each tracked independently.

WAIT via REPLCONF GETACK *.


Handshake I support

1. Replica connects and PING → primary replies +PONG.


2. Replica sends REPLCONF listening-port <p> and REPLCONF capa eof.


3. Replica issues PSYNC ? -1.


4. Primary replies FULLRESYNC <replid> <offset>, streams RDB bytes, then switches to command stream.



Full sync (RDB)

RDB is raw bytes (not RESP-framed).

Replica loads keys/expiries, then resumes RESP parsing.


Live propagation

Primary mirrors write operations (SET, INCR, list/stream mutations) to replicas in the same RESP form, preserving order.


Offsets & ACKs

Primary keeps a monotonically increasing replication offset and per-replica last-ACKed offset.

Replicas send REPLCONF ACK <offset>.

WAIT triggers REPLCONF GETACK * and counts replicas that reached the target offset (or until timeout).


WAIT semantics (basic)

Captures current offset after the caller’s writes.

Prompts fresh ACKs; blocks until N replicas catch up or timeout.

Designed not to stall the replication stream.


Multiple replicas

Primary tracks {socket, lastAck} per replica; WAIT counts distinct replicas.


Health & reconnection

Replica reconnect → full resync (no partial resync/backlog).


Edge behavior

RDB is never treated as a RESP Bulk String.

Control messages like REPLCONF GETACK * are not writes.

Pub/Sub subscribers are isolated from normal command flow.

Only write-affecting commands are replicated.


Config (relevant flags)

--replicaof <host> <port>

--dir <path>, --dbfilename <name> (for primary to serve RDB)

Log level is adjustable.


Limits / next

No backlog or partial resync; every reconnect triggers a full sync.



---

Transactions

Built: connection-scoped transactions with MULTI / EXEC / DISCARD, QUEUED behavior, and basic guards (no WATCH/UNWATCH yet).

Scope

MULTI enters transactional mode.

Commands are queued (reply QUEUED).

EXEC runs them in order and returns an array of replies.

DISCARD clears the queue and exits MULTI.

Guards: EXEC/DISCARD without MULTI → error.

Nested MULTI treated as a no-op.

No WATCH/UNWATCH (optimistic concurrency).


Behavior

Begin: MULTI sets per-connection tx flag and queue.

Queue: argv preserved; validation on execute.

Commit: EXEC returns each command’s reply (errors show up per element, no global EXECABORT).

Abort: DISCARD drops the queue.

Subscribed clients: only subscription-safe commands allowed; tx commands rejected in that mode.


Atomicity / isolation

Sequential on the same connection; no global isolation/locks.

Replication: write commands executed by EXEC are propagated in the same order.


Limits / next

No WATCH/UNWATCH / EXECABORT.

For stricter isolation, add a commit section or move to an event-loop model.



---

Streams

Built: minimal Streams with XADD, XRANGE, XREAD [BLOCK], and TYPE → stream, plus simple blocking.

Scope & behavior

Model: stream key → entries keyed by ms-seq ID with field→value map.

ID rules: strictly increasing; reject 0-0.

* → currentTimeMillis-0.

ms-* auto-fills seq: increments if last entry has same ms, else 0.

IDs ≤ last are rejected.


XRANGE start end:

- as 0-0, + as “now-0”.

If seq omitted, normalize to start-ms-0 / a large tail for end.

Returns [id, [field, value, ...]] pairs in range.


XREAD STREAMS key id:

Non-blocking returns entries with IDs strictly greater than id.

BLOCK <ms> uses per-stream lock/notify; wakes on XADD or timeout.

$ means “from current last ID forward”.

On timeout, returns a null array.


TYPE key → stream.

Replication: XADD is included in the replication stream.


Notes

Writers notify blocked readers; no busy-wait.

Ordering by (ms, seq) tuple.


Limits / next

No consumer groups (XGROUP, XREADGROUP, XACK, XCLAIM).

No MAXLEN trimming.

In-memory only (beyond initial bootstrap rules).



---

RDB Persistence

Built: initial dataset load from an RDB file at startup with per-key expiries.

What I support

Flags: --dir <path>, --dbfilename <name>.

Minimal subset:

DB selector FE (ignored; everything goes to the single in-memory DB)

Aux FB (skipped)

String key/value entries 00

Millisecond expiries FC attached to the following key

End marker FF


Variable-length length decoding (1-, 2-, 5-byte forms).

TTL:

FC → absolute ms expiry

Without TTL → far-future expiry (effectively persistent here)


After load, regular reads/writes work; expiries enforced on access and via sweeps.


Deliberate limits

Read-only persistence (bootstrap only; no rewrite on shutdown).

Only string values parsed from RDB.

No special encodings (LZF/int).

Single logical DB.


Notes

Missing/invalid file → start empty.

Plays well with replication: primary can serve a small bootstrap RDB during FULLRESYNC then stream commands.



---

Geospatial Commands

Built: lean GEO feature set using Morton (Z-order) encoding and Haversine filtering.

Data model

Each geo key stores members keyed by a 64-bit Morton code from (lon, lat) (26 bits each; bit-interleaved).

Positions decode back to approximate coordinates (cell center).

Distances computed via Haversine (meters).


Commands

GEOADD key lon lat member

Bounds: lon ∈ [-180, 180], lat ∈ [-85.05112878, 85.05112878].

Upsert at encoded score; returns added/updated count.


GEOPOS key member [...] → decoded coords or nil per member.

GEODIST key member1 member2 → meters (string).

GEOSEARCH key FROMLONLAT <lon> <lat> BYRADIUS <radius-m>

Scans and filters by radius in meters; returns member names.



Choices

Encoding keeps inserts ordered and scans fast; precise radius check via Haversine post-filter.

Re-inserts move a member to the latest location.

Natural structure order; distances/geohashes not returned.


Limits / next

No FROMMEMBER, no unit switching (km/mi/ft), no COUNT, no ASC|DESC.

Potential precision collisions at extreme densities are handled by member sets under a score.

No GEO persistence in RDB (bootstrap covers strings only).



---

Sorted Sets

Built: minimal ZSET with ZADD, ZRANGE, ZRANK, ZCARD, ZSCORE, ZREM.

Semantics

ZADD adds/updates; returns 1 (new) or 0 (updated).

Order: ascending by score; tie-break lexicographically by member.

ZRANGE start end with negative indices; returns members only.

ZRANK is 0-based; missing → nil.

ZSCORE returns score as string; missing → nil.

ZCARD size; ZREM removes (1/0).


Implementation

TreeMap<Double, TreeSet<String>> (score → ordered members) + membership checks to move members on update.

ZADD / ZREM ~ O(log n); ZRANGE iterates the selected window.


Limits / next

No WITHSCORES, ZRANGEBYSCORE, or range deletes.

No ZSET persistence in RDB.

Consider a skip-list for closer Redis-like range slicing.



---

Pub/Sub

Built: SUBSCRIBE, UNSUBSCRIBE, PUBLISH with connection-scoped subscriptions.

Behavior

Subscribed connections enter subscribed mode; only SUBSCRIBE, UNSUBSCRIBE, PING, QUIT, RESET are allowed (others error).

PUBLISH fans out ["message", <channel>, <payload>] to subscribers and returns the delivery count.

UNSUBSCRIBE emits the standard 3-field reply (unsubscribe, channel, remaining count).


Implementation

Per-connection channel sets; messages written directly to subscriber sockets.

Normal command processing is isolated from subscribed connections.


Limits / next

No PSUBSCRIBE/PUNSUBSCRIBE, no PUBSUB introspection, no sharded channels.

Pub/Sub messages are transient (not replicated).



---

Lists

Built: LPUSH, RPUSH, LLEN, LRANGE, LPOP [count], BLPOP (single key) with simple blocking.

Behavior

LPUSH / RPUSH return the new length.

LPOP pops head; with count returns array; nil if empty.

BLPOP key timeout blocks until an element arrives or timeout; nil on timeout.

LRANGE start end supports negatives; out-of-range → empty array.

LLEN returns current length.


Implementation

In-memory list per key; per-key waiter queue + lock/notify wakes the oldest blocked BLPOP first.

List mutations are included in the replication stream.


Limits / next

No right-side ops (RPOP/BRPOP), no multi-key BLPOP, no RPOPLPUSH/LMOVE.

Current array-backed list: LPUSH on large lists shifts elements; switching to a deque would improve head ops.

No trimming/eviction or RDB persistence for lists.



---

> Goal: keep the implementation small and readable, matching Redis’ surface behavior for the covered commands while leaving room to extend (partial resync, groups, trimming, richer ZSET ranges, etc.).










