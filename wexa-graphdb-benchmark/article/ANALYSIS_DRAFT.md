# Analysis draft (section 9) - edit this into your own voice before pasting into README.md

CognoDB's numbers only make sense once you separate batched operations from
single-row ones. Every point query - 1-hop (252.7ms), 2-hop (253.0ms), 3-hop
(252.8ms), point lookup (252.0ms), aggregation (279.6ms) - clusters around
250-280ms regardless of what the query actually computes. A single-hop lookup
and a full label aggregation costing almost exactly the same amount of time
only makes sense if a fixed cost is dominating the measurement rather than
query complexity. The 1-client mixed-workload throughput confirms it exactly:
4.0 qps is 1000ms / 252ms - CognoDB is bottlenecked by one network round trip
per query, not by the database engine. This machine is in Bengaluru; CognoDB's
instance is in us-east4 - a ~250ms transcontinental round trip lines up almost
exactly with the observed latency floor.

Ingest tells the opposite-looking story for the same reason. CognoDB loaded
the full dataset in 71.9s, faster than local Neo4j's 102.4s, despite paying
the same network cost. The difference is batching: ingest sends 1,000 rows
per `UNWIND` call, so the round-trip cost is amortized across a thousand rows
instead of paid once per row, the way every point query in this benchmark is.
Same root cause, opposite-looking effect, depending on whether the workload
is batched or not - which is itself worth stating plainly: this benchmark
mostly measures network topology and batching strategy for CognoDB, not the
query engine underneath it.

Neo4j's own numbers point to a second, independent problem: a wide gap
between p50 and p95 at almost every read workload (1-hop 17.7ms -> 104.1ms,
aggregation 97.6ms -> 196.9ms) - the signature of JVM garbage-collection
pauses, not query cost. This is the same 512MB memory ceiling this benchmark
had to fight to keep Neo4j alive under in the first place (see the fairness
caveat in section 2) showing up a second time, independently, as tail
latency rather than an outright crash. It also plausibly explains why local
Neo4j ingests slower than remote CognoDB despite paying no network cost:
GC pressure under a tight heap competes with write throughput the same way
it competes with read latency.

Memgraph and ArangoDB were the cleanest performers in this benchmark, and
for different, specific reasons rather than a shared one. Memgraph's
in-memory model shows up directly in its ingest throughput (76,814 nodes/s,
53,700 rels/s vs. Neo4j's 989/2,375) and its flat, low read latencies -
there's no disk commit or page-cache layer to add variance. ArangoDB's
document-store bulk insert (`insert_many`) also substantially outpaces
Cypher's `UNWIND...CREATE` pattern (8,082 nodes/s vs. Neo4j's 989/s) despite
running on disk, not memory - plausibly because a bulk document insert has
less per-row overhead than constructing and matching a graph pattern for
every row, though this benchmark can't isolate that from other differences
in each engine's write path.

Kuzu's ingest numbers (188,293 nodes/s, 502,571 rels/s, 0.49s total) need a
caveat the raw multiplier doesn't convey on its own: this uses Kuzu's native
`COPY FROM` CSV bulk loader, a fundamentally different operation category
from the transactional batched inserts every other platform uses (even
though every platform here is already using its own best native bulk-load
method, per section 4). The gap partly reflects "bulk columnar import is
categorically faster than row-oriented transactional writes," not purely
"Kuzu the query engine is ~190x faster than Neo4j." Kuzu's mixed-workload
throughput is also flat across concurrency (758 -> 778 -> 782 qps from 1 to
40 clients) rather than scaling up the way ArangoDB's and Memgraph's do -
consistent with Kuzu serializing writes on a single shared embedded
connection rather than genuinely parallelizing across concurrent clients,
which was verified directly during adapter development, not just inferred
from this result.

Taken together, resource-capped free-tier benchmarking at this scale mostly
surfaces architecture and deployment-model effects - network topology,
batching strategy, in-memory vs. disk-backed storage, JVM overhead, embedded
vs. client-server concurrency model - more than it isolates raw query-engine
performance. That's a legitimate finding in its own right, not a limitation
to apologize for: it's closer to what a real application choosing between
these platforms under similar constraints would actually experience.
