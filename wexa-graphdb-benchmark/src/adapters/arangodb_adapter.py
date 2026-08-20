"""
ArangoDB adapter. ArangoDB is multi-model (documents + graph) and uses
AQL instead of Cypher - included specifically because it's a genuinely
different storage/query model from the three Bolt platforms, which
makes the comparison more interesting than four Cypher-speaking clones.
"""
import os
from typing import Iterable, List

from arango import ArangoClient

from .base import GraphAdapter


class ArangoAdapter(GraphAdapter):
    name = "arangodb"

    def __init__(self):
        self.url = os.environ["ARANGO_URI"]
        self.user = os.environ.get("ARANGO_USER", "root")
        self.password = os.environ["ARANGO_PASSWORD"]
        self.db_name = os.environ.get("ARANGO_DB", "benchmark")
        self.client = None
        self.db = None

    def connect(self) -> None:
        self.client = ArangoClient(hosts=self.url)
        sys_db = self.client.db("_system", username=self.user, password=self.password)
        if not sys_db.has_database(self.db_name):
            sys_db.create_database(self.db_name)
        self.db = self.client.db(self.db_name, username=self.user, password=self.password)
        if not self.db.has_collection("nodes"):
            self.db.create_collection("nodes")
        if not self.db.has_collection("edges"):
            self.db.create_collection("edges", edge=True)

    def close(self) -> None:
        pass  # python-arango is HTTP-per-request, nothing persistent to close

    def wipe(self) -> None:
        self.db.collection("nodes").truncate()
        self.db.collection("edges").truncate()

    def create_indexes(self) -> List[str]:
        nodes = self.db.collection("nodes")
        nodes.add_persistent_index(fields=["ext_id"], unique=True)
        nodes.add_persistent_index(fields=["label"])
        return ["persistent index on nodes.ext_id (unique)", "persistent index on nodes.label"]

    def load_nodes(self, nodes: Iterable[dict], batch_size: int = 1000) -> None:
        col = self.db.collection("nodes")
        batch = []
        for n in nodes:
            batch.append({"_key": n["id"], "ext_id": n["id"], "label": n["label"]})
            if len(batch) >= batch_size:
                col.insert_many(batch, overwrite=True)
                batch = []
        if batch:
            col.insert_many(batch, overwrite=True)

    def load_edges(self, edges: Iterable[dict], batch_size: int = 1000) -> None:
        col = self.db.collection("edges")
        batch = []
        for e in edges:
            batch.append({
                "_from": f"nodes/{e['src']}",
                "_to": f"nodes/{e['dst']}",
                "type": e["type"],
            })
            if len(batch) >= batch_size:
                col.insert_many(batch, overwrite=True)
                batch = []
        if batch:
            col.insert_many(batch, overwrite=True)

    def point_lookup(self, node_id: str) -> None:
        list(self.db.aql.execute(
            "FOR n IN nodes FILTER n.ext_id == @id RETURN n", bind_vars={"id": node_id}
        ))

    def filtered_lookup(self, label: str, limit: int = 50) -> None:
        list(self.db.aql.execute(
            "FOR n IN nodes FILTER n.label == @label LIMIT @limit RETURN n",
            bind_vars={"label": label, "limit": limit},
        ))

    def traversal(self, start_id: str, hops: int) -> None:
        list(self.db.aql.execute(
            f"FOR v IN {hops}..{hops} OUTBOUND @start edges RETURN DISTINCT v",
            bind_vars={"start": f"nodes/{start_id}"},
        ))

    def aggregation(self) -> None:
        list(self.db.aql.execute(
            "FOR n IN nodes COLLECT label = n.label WITH COUNT INTO c SORT c DESC RETURN {label, c}"
        ))

    def write_sample(self, node_id: str) -> None:
        list(self.db.aql.execute(
            "FOR n IN nodes FILTER n.ext_id == @id UPDATE n WITH {touched: DATE_NOW()} IN nodes",
            bind_vars={"id": node_id},
        ))

    def footprint(self) -> dict:
        try:
            stats = self.db.collection("nodes").statistics()
            return {"nodes_collection_stats": stats}
        except Exception as e:
            return {"note": f"not observable via driver: {e}. Use docker stats / Arango web UI instead."}
