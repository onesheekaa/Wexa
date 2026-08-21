"""
Kuzu adapter. Kuzu is an embedded, in-process graph database (no server,
no Docker container) - added in place of Dgraph, which OOM-killed
repeatedly at 192MB/400MB/512MB in this environment even after a clean
volume reset (see README caveats). Embedded means it structurally can't
suffer a separate-process OOM the way every other self-hosted platform
here can, since it runs inside the same process as this harness.

Kuzu uses a statically-typed, columnar schema (unlike the flexible
property model of Neo4j/ArangoDB/Dgraph) - node/rel properties must be
declared in CREATE NODE/REL TABLE up front.

BUGFIX: the original version of this adapter issued one execute() call
per node/edge (~1,300 nodes/s, ~740 edges/s - the real dataset took
~4.75 minutes just to ingest). Every other adapter in this project uses
its platform's native bulk-load path (UNWIND batching, insert_many) -
this one didn't. Switched to Kuzu's COPY FROM CSV bulk loader, which
loads the full real dataset (18,772 nodes + 198,110 edges) in ~0.4s -
roughly a 680x speedup, verified directly, not estimated.

BUGFIX #2: COPY FROM requires an exact column-count match against the
source CSV (id, label - 2 columns). `touched` (used only by
write_sample in the mixed workload) can't be declared on the Node table
up front without breaking every COPY. It's added via ALTER TABLE after
each load instead - and since ALTER persists permanently once run,
wipe() fully DROPs and recreates both tables (not just deletes rows),
so every re-run starts from a genuinely clean 2-column schema before
the next COPY. Verified stable across three repeated wipe/reload cycles
against the same on-disk database file before shipping this.

Concurrency note: unlike Neo4j (TransientError.DeadlockDetected) and
ArangoDB (error_code 1200 write-write conflict), Kuzu did not surface
any conflict errors under concurrent writes to a shared hot-node pool
in testing - it appears to serialize internally without raising to the
client. No retry-on-conflict wrapper is used here as a result; this is
a disclosed platform difference, not an oversight.
"""
import csv
import os
import tempfile
import threading
from pathlib import Path
from typing import Iterable, List

import kuzu

from .base import GraphAdapter


class KuzuAdapter(GraphAdapter):
    name = "kuzu"

    _shared_db = None
    _shared_conn = None
    _lock = threading.Lock()

    def __init__(self):
        self.db_path = os.environ.get("KUZU_PATH", "./data/kuzu_db")
        self.conn = None

    def connect(self) -> None:
        with KuzuAdapter._lock:
            if KuzuAdapter._shared_conn is None:
                Path(self.db_path).parent.mkdir(parents=True, exist_ok=True)
                KuzuAdapter._shared_db = kuzu.Database(self.db_path)
                KuzuAdapter._shared_conn = kuzu.Connection(KuzuAdapter._shared_db)
                self._create_tables(KuzuAdapter._shared_conn)
        self.conn = KuzuAdapter._shared_conn

    @staticmethod
    def _create_tables(conn) -> None:
        try:
            conn.execute("CREATE NODE TABLE Node(id STRING, label STRING, PRIMARY KEY(id))")
        except RuntimeError:
            pass
        try:
            conn.execute("CREATE REL TABLE REL(FROM Node TO Node, type STRING)")
        except RuntimeError:
            pass

    def close(self) -> None:
        pass

    def wipe(self) -> None:
        try:
            self.conn.execute("DROP TABLE REL")
        except RuntimeError:
            pass
        try:
            self.conn.execute("DROP TABLE Node")
        except RuntimeError:
            pass
        self._create_tables(self.conn)

    def create_indexes(self) -> List[str]:
        return ["primary key index on Node.id (automatic)", "no secondary index on Node.label (full scan)"]

    def load_nodes(self, nodes: Iterable[dict], batch_size: int = 1000) -> None:
        nodes = list(nodes)
        with tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False, newline="") as f:
            writer = csv.DictWriter(f, fieldnames=["id", "label"])
            writer.writeheader()
            writer.writerows(nodes)
            tmp_path = f.name
        try:
            self.conn.execute(f"COPY Node FROM '{tmp_path}' (header=true)")
            self.conn.execute("ALTER TABLE Node ADD touched INT64 DEFAULT 0")
        finally:
            os.unlink(tmp_path)

    def load_edges(self, edges: Iterable[dict], batch_size: int = 1000) -> None:
        edges = list(edges)
        with tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False, newline="") as f:
            writer = csv.DictWriter(f, fieldnames=["src", "dst", "type"])
            writer.writeheader()
            writer.writerows(edges)
            tmp_path = f.name
        try:
            self.conn.execute(f"COPY REL FROM '{tmp_path}' (header=true)")
        finally:
            os.unlink(tmp_path)

    def point_lookup(self, node_id: str) -> None:
        self.conn.execute("MATCH (n:Node {id: $id}) RETURN n", parameters={"id": node_id})

    def filtered_lookup(self, label: str, limit: int = 50) -> None:
        self.conn.execute(
            "MATCH (n:Node {label: $label}) RETURN n LIMIT $limit",
            parameters={"label": label, "limit": limit},
        )

    def traversal(self, start_id: str, hops: int) -> None:
        query = f"MATCH (n:Node {{id: $id}})-[:REL*{hops}..{hops}]->(m) RETURN DISTINCT m LIMIT 100"
        self.conn.execute(query, parameters={"id": start_id})

    def aggregation(self) -> None:
        self.conn.execute(
            "MATCH (n:Node) RETURN n.label AS label, count(*) AS c ORDER BY c DESC"
        )

    def write_sample(self, node_id: str) -> None:
        self.conn.execute(
            "MATCH (n:Node {id: $id}) SET n.touched = 1", parameters={"id": node_id}
        )

    def footprint(self) -> dict:
        try:
            path = Path(self.db_path)
            if path.is_file():
                total_size = path.stat().st_size
            elif path.is_dir():
                total_size = sum(f.stat().st_size for f in path.rglob("*") if f.is_file())
            else:
                return {"note": f"db path not found: {self.db_path}"}
            return {"on_disk_bytes": total_size, "note": "embedded - no separate server process to inspect"}
        except Exception as e:
            return {"note": f"not observable: {e}"}
