"""
Dgraph adapter. Dgraph models data as (subject, predicate, object)
triples rather than labeled property nodes, so this maps our generic
node/edge schema onto three predicates: ext_id (indexed string),
label (indexed string), rel (uid edge list).

Heads-up: this is the least battle-tested adapter of the five (Dgraph's
mutation/query shape is the most different from the others). Smoke-test
it against a tiny slice of the dataset before your full run - if it's
eating time you don't have, swapping it for something simpler (e.g.
self-hosted JanusGraph or a second Neo4j-protocol platform) is a
defensible call you can explain in the interview.
"""
import json
import os
from typing import Iterable, List

import pydgraph

from .base import GraphAdapter


class DgraphAdapter(GraphAdapter):
    name = "dgraph"

    def __init__(self):
        self.grpc_addr = os.environ["DGRAPH_GRPC"]
        self.client_stub = None
        self.client = None
        self._uid_cache = {}

    def connect(self) -> None:
        self.client_stub = pydgraph.DgraphClientStub(self.grpc_addr)
        self.client = pydgraph.DgraphClient(self.client_stub)
        schema = """
        ext_id: string @index(exact) .
        label: string @index(exact) .
        rel: [uid] @reverse .
        touched: int .
        """
        self.client.alter(pydgraph.Operation(schema=schema))
        # BUGFIX: run_mixed_workload() in runner.py opens a *fresh* adapter
        # per worker thread and only calls connect() - it never calls
        # load_nodes(). _uid_cache was previously only populated inside
        # load_nodes(), so every write_sample() from a mixed-workload worker
        # found an empty cache and silently no-op'd (see write_sample below):
        # 100% of Dgraph's "writes" during the mixed workload were doing
        # nothing, which would have made Dgraph look artificially fastest.
        # Refresh here too. Wrapped in try/except because this also runs
        # right after wipe()'s drop_all, when there's nothing to cache yet.
        try:
            self._refresh_uid_cache()
        except Exception:
            pass

    def close(self) -> None:
        if self.client_stub:
            self.client_stub.close()

    def wipe(self) -> None:
        self.client.alter(pydgraph.Operation(drop_all=True))
        self._uid_cache = {}
        self.connect()  # drop_all wipes the schema too - reapply it

    def create_indexes(self) -> List[str]:
        # indexes are declared as part of the schema in connect(); nothing
        # further to do, this just reports what's active for the README
        return ["exact index on ext_id", "exact index on label"]

    def load_nodes(self, nodes: Iterable[dict], batch_size: int = 1000) -> None:
        txn = self.client.txn()
        try:
            batch = []
            for n in nodes:
                batch.append({"ext_id": n["id"], "label": n["label"]})
                if len(batch) >= batch_size:
                    txn.mutate(set_obj=batch)
                    batch = []
            if batch:
                txn.mutate(set_obj=batch)
            txn.commit()
        finally:
            txn.discard()
        self._refresh_uid_cache()

    def _refresh_uid_cache(self) -> None:
        query = "{ all(func: has(ext_id)) { uid ext_id } }"
        res = self.client.txn(read_only=True).query(query)
        data = json.loads(res.json)
        self._uid_cache = {row["ext_id"]: row["uid"] for row in data["all"]}

    def load_edges(self, edges: Iterable[dict], batch_size: int = 1000) -> None:
        txn = self.client.txn()
        try:
            batch = []
            for e in edges:
                src_uid = self._uid_cache.get(e["src"])
                dst_uid = self._uid_cache.get(e["dst"])
                if not src_uid or not dst_uid:
                    continue
                batch.append({"uid": src_uid, "rel": [{"uid": dst_uid}]})
                if len(batch) >= batch_size:
                    txn.mutate(set_obj=batch)
                    batch = []
            if batch:
                txn.mutate(set_obj=batch)
            txn.commit()
        finally:
            txn.discard()

    def point_lookup(self, node_id: str) -> None:
        query = 'query q($id: string) { q(func: eq(ext_id, $id)) { uid ext_id label } }'
        self.client.txn(read_only=True).query(query, variables={"$id": node_id})

    def filtered_lookup(self, label: str, limit: int = 50) -> None:
        query = ('query q($label: string, $limit: int) { '
                  'q(func: eq(label, $label), first: $limit) { uid ext_id label } }')
        self.client.txn(read_only=True).query(
            query, variables={"$label": label, "$limit": str(limit)}
        )

    def traversal(self, start_id: str, hops: int) -> None:
        inner = "uid"
        for _ in range(hops):
            inner = f"rel {{ {inner} }}"
        query = f'query q($id: string) {{ start(func: eq(ext_id, $id)) {{ {inner} }} }}'
        self.client.txn(read_only=True).query(query, variables={"$id": start_id})

    def aggregation(self) -> None:
        query = "{ byLabel(func: has(label)) @groupby(label) { count(uid) } }"
        self.client.txn(read_only=True).query(query)

    def write_sample(self, node_id: str) -> None:
        uid = self._uid_cache.get(node_id)
        if not uid:
            return
        txn = self.client.txn()
        try:
            txn.mutate(set_obj={"uid": uid, "touched": 1})
            txn.commit()
        finally:
            txn.discard()

    def footprint(self) -> dict:
        return {"note": "not observable via DQL; report from `docker exec dgraph-alpha du -sh /dgraph`"}