"""
Generic adapter for any database that speaks Bolt + Cypher: CognoDB,
Neo4j, and Memgraph all qualify, which is exactly why this one class
covers three of your five platforms. Same driver, same queries -
different connection details per instance.
"""
import os
import random
import time
from typing import Iterable, List, Optional

from neo4j import GraphDatabase
from neo4j.exceptions import TransientError

from .base import GraphAdapter


class BoltAdapter(GraphAdapter):
    # BUGFIX: mixed workload writes to a shared pool of only 200 sampled
    # node ids, so at concurrency=10/40 two threads legitimately hit the
    # same node at once - the DB correctly raises
    # Neo.TransientError.Transaction.DeadlockDetected, which means
    # "retry this, it will likely succeed." Only TransientError is
    # retried; anything else still raises immediately.
    MAX_RETRIES = 3
    BASE_BACKOFF_SECONDS = 0.05

    def __init__(self, name: str, uri_env: str, user_env: str, password_env: str,
                 database: Optional[str] = None):
        self.name = name
        self.uri = os.environ[uri_env]
        self.user = os.environ.get(user_env, "neo4j")
        self.password = os.environ[password_env]
        self.database = database
        self.driver = None

    def connect(self) -> None:
        self.driver = GraphDatabase.driver(self.uri, auth=(self.user, self.password))
        self.driver.verify_connectivity()

    def close(self) -> None:
        if self.driver:
            self.driver.close()

    def _run(self, query: str, **params):
        for attempt in range(self.MAX_RETRIES + 1):
            try:
                with self.driver.session(database=self.database) as session:
                    return list(session.run(query, **params))
            except TransientError:
                if attempt == self.MAX_RETRIES:
                    raise
                backoff = self.BASE_BACKOFF_SECONDS * (2 ** attempt) + random.uniform(0, 0.05)
                time.sleep(backoff)

    def wipe(self) -> None:
        self._run("MATCH (n) DETACH DELETE n")

    def create_indexes(self) -> List[str]:
        self._run("CREATE INDEX node_id_idx IF NOT EXISTS FOR (n:Node) ON (n.id)")
        self._run("CREATE INDEX node_label_idx IF NOT EXISTS FOR (n:Node) ON (n.label)")
        return ["node_id_idx on Node(id)", "node_label_idx on Node(label)"]

    def load_nodes(self, nodes: Iterable[dict], batch_size: int = 1000) -> None:
        batch = []
        for n in nodes:
            batch.append(n)
            if len(batch) >= batch_size:
                self._flush_nodes(batch)
                batch = []
        if batch:
            self._flush_nodes(batch)

    def _flush_nodes(self, batch):
        self._run(
            "UNWIND $rows AS row CREATE (n:Node {id: row.id, label: row.label})",
            rows=batch,
        )

    def load_edges(self, edges: Iterable[dict], batch_size: int = 1000) -> None:
        batch = []
        for e in edges:
            batch.append(e)
            if len(batch) >= batch_size:
                self._flush_edges(batch)
                batch = []
        if batch:
            self._flush_edges(batch)

    def _flush_edges(self, batch):
        self._run(
            "UNWIND $rows AS row "
            "MATCH (a:Node {id: row.src}), (b:Node {id: row.dst}) "
            "CREATE (a)-[:REL {type: row.type}]->(b)",
            rows=batch,
        )

    def point_lookup(self, node_id: str) -> None:
        self._run("MATCH (n:Node {id: $id}) RETURN n", id=node_id)

    def filtered_lookup(self, label: str, limit: int = 50) -> None:
        self._run("MATCH (n:Node {label: $label}) RETURN n LIMIT $limit", label=label, limit=limit)

    def traversal(self, start_id: str, hops: int) -> None:
        # BUGFIX: unbounded -[:REL*{hops}]-> applies DISTINCT/LIMIT only
        # after materializing every intermediate path, which explodes on
        # hub nodes - this dataset has a degree-504 node vs a median
        # degree of 9, and 3-hop from it took 22s and killed the CognoDB
        # connection before this fix. Capped-BFS instead: each hop's
        # frontier is limited to frontier_cap nodes before expanding
        # further. Disclosed sampling bias at hub nodes, applied
        # identically across every platform.
        query = self._build_bounded_traversal_query(hops)
        self._run(query, id=start_id)

    @staticmethod
    def _build_bounded_traversal_query(hops: int, frontier_cap: int = 50, final_limit: int = 100) -> str:
        lines = ["MATCH (n:Node {id: $id})"]
        prev_var = "n"
        next_var = "n"
        for level in range(1, hops + 1):
            next_var = f"h{level}"
            lines.append(
                f"CALL {{ WITH {prev_var} MATCH ({prev_var})-[:REL]->({next_var}) "
                f"RETURN DISTINCT {next_var} LIMIT {frontier_cap} }}"
            )
            if level < hops:
                lines.append(f"WITH collect(DISTINCT {next_var})[0..{frontier_cap}] AS frontier_{level}")
                lines.append(f"UNWIND frontier_{level} AS {next_var}_seed")
                prev_var = f"{next_var}_seed"
        lines.append(f"RETURN DISTINCT {next_var} AS m LIMIT {final_limit}")
        return "\n".join(lines)

    def aggregation(self) -> None:
        self._run("MATCH (n:Node) RETURN n.label AS label, count(*) AS c ORDER BY c DESC")

    def write_sample(self, node_id: str) -> None:
        self._run("MATCH (n:Node {id: $id}) SET n.touched = timestamp()", id=node_id)

    def footprint(self) -> dict:
        try:
            rows = self._run(
                "CALL apoc.meta.stats() YIELD nodeCount, relCount RETURN nodeCount, relCount"
            )
            return {"nodeCount": rows[0]["nodeCount"], "relCount": rows[0]["relCount"]}
        except Exception:
            # APOC isn't installed on every platform (e.g. Memgraph, and
            # possibly CognoDB) - fall back to plain Cypher counts
            try:
                rows = self._run("MATCH (n) RETURN count(n) AS c")
                return {"nodeCount": rows[0]["c"], "note": "counted via MATCH, APOC not available"}
            except Exception as e:
                return {"note": f"not observable: {e}"}
