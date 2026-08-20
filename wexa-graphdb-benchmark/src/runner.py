"""
Entry point: loads one platform, runs the full workload suite required
by section 5.2 of the assignment, and writes results/<platform>.json.

Usage:
    python -m src.runner --platform cognodb
    python -m src.runner --platform neo4j
    python -m src.runner --platform memgraph
    python -m src.runner --platform arangodb
    python -m src.runner --platform dgraph
    python -m src.runner --platform all
"""
import argparse
import csv
import json
import random
import time
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

from dotenv import load_dotenv

from src.adapters.arangodb_adapter import ArangoAdapter
from src.adapters.bolt_adapter import BoltAdapter
from src.adapters.dgraph_adapter import DgraphAdapter
from src.metrics import Timer

load_dotenv()

ROOT_DIR = Path(__file__).resolve().parent.parent
DATA_DIR = ROOT_DIR / "data"
RESULTS_DIR = ROOT_DIR / "results"
RESULTS_DIR.mkdir(exist_ok=True)

# Tune these down if a free-tier instance is timing out - and say so in
# the README's caveats section rather than silently lowering the bar.
READ_ITERATIONS = 100
WARMUP_ITERATIONS = 20
MIXED_WORKLOAD_SECONDS = 30
MIXED_WORKLOAD_CLIENTS = [1, 10, 40]
MIXED_WRITE_RATIO = 0.2  # 20% writes, 80% reads


def build_adapter(platform: str):
    if platform == "cognodb":
        return BoltAdapter("cognodb", "COGNODB_URI", "COGNODB_USER", "COGNODB_PASSWORD")
    if platform == "neo4j":
        return BoltAdapter("neo4j", "NEO4J_URI", "NEO4J_USER", "NEO4J_PASSWORD")
    if platform == "memgraph":
        return BoltAdapter("memgraph", "MEMGRAPH_URI", "MEMGRAPH_USER", "MEMGRAPH_PASSWORD")
    if platform == "arangodb":
        return ArangoAdapter()
    if platform == "dgraph":
        return DgraphAdapter()
    raise ValueError(f"Unknown platform: {platform}")


def load_dataset():
    nodes, edges = [], []
    with open(DATA_DIR / "nodes.csv") as f:
        nodes = list(csv.DictReader(f))
    with open(DATA_DIR / "edges.csv") as f:
        edges = list(csv.DictReader(f))
    return nodes, edges


def run_ingest(adapter, nodes, edges) -> dict:
    adapter.wipe()
    index_report = adapter.create_indexes()

    t0 = time.perf_counter()
    adapter.load_nodes(nodes)
    node_elapsed = time.perf_counter() - t0

    t0 = time.perf_counter()
    adapter.load_edges(edges)
    edge_elapsed = time.perf_counter() - t0

    return {
        "index_report": index_report,
        "node_load_seconds": round(node_elapsed, 3),
        "edge_load_seconds": round(edge_elapsed, 3),
        "nodes_per_second": round(len(nodes) / node_elapsed, 1) if node_elapsed else None,
        "rels_per_second": round(len(edges) / edge_elapsed, 1) if edge_elapsed else None,
        "total_wall_clock_seconds": round(node_elapsed + edge_elapsed, 3),
        "node_count": len(nodes),
        "rel_count": len(edges),
    }


def sample_ids(nodes, n=200):
    random.seed(42)
    return random.sample([row["id"] for row in nodes], min(n, len(nodes)))


def run_traversals(adapter, start_ids) -> dict:
    results = {}
    for hops in (1, 2, 3):
        timer = Timer()
        for sid in start_ids[:WARMUP_ITERATIONS]:
            adapter.traversal(sid, hops)
        for sid in (start_ids * (READ_ITERATIONS // len(start_ids) + 1))[:READ_ITERATIONS]:
            with timer.measure():
                adapter.traversal(sid, hops)
        results[f"{hops}_hop"] = timer.percentiles()
    return results


def run_lookups(adapter, start_ids, labels) -> dict:
    point = Timer()
    ids_cycled = (start_ids * (READ_ITERATIONS // len(start_ids) + 1))
    for sid in ids_cycled[:WARMUP_ITERATIONS]:
        adapter.point_lookup(sid)
    for sid in ids_cycled[:READ_ITERATIONS]:
        with point.measure():
            adapter.point_lookup(sid)

    filtered = Timer()
    for _ in range(WARMUP_ITERATIONS):
        adapter.filtered_lookup(random.choice(labels))
    for _ in range(READ_ITERATIONS):
        with filtered.measure():
            adapter.filtered_lookup(random.choice(labels))

    return {"point_lookup": point.percentiles(), "filtered_lookup": filtered.percentiles()}


def run_aggregation(adapter) -> dict:
    timer = Timer()
    for _ in range(WARMUP_ITERATIONS):
        adapter.aggregation()
    for _ in range(READ_ITERATIONS):
        with timer.measure():
            adapter.aggregation()
    return timer.percentiles()


def run_mixed_workload(build_adapter_fn, start_ids) -> dict:
    """Each thread opens its own adapter/connection - most drivers aren't
    safe to share a single session across threads."""
    out = {}
    for concurrency in MIXED_WORKLOAD_CLIENTS:
        stop_at = time.perf_counter() + MIXED_WORKLOAD_SECONDS

        def worker():
            local_adapter = build_adapter_fn()
            local_adapter.connect()
            ops = 0
            while time.perf_counter() < stop_at:
                sid = random.choice(start_ids)
                if random.random() < MIXED_WRITE_RATIO:
                    local_adapter.write_sample(sid)
                else:
                    local_adapter.point_lookup(sid)
                ops += 1
            local_adapter.close()
            return ops

        t0 = time.perf_counter()
        with ThreadPoolExecutor(max_workers=concurrency) as pool:
            total_ops = sum(f.result() for f in [pool.submit(worker) for _ in range(concurrency)])
        elapsed = time.perf_counter() - t0

        out[f"{concurrency}_clients"] = {
            "total_ops": total_ops,
            "elapsed_seconds": round(elapsed, 2),
            "throughput_qps": round(total_ops / elapsed, 2),
            "write_ratio": MIXED_WRITE_RATIO,
        }
    return out


def run_platform(platform: str) -> None:
    print(f"=== {platform} ===")
    adapter = build_adapter(platform)
    adapter.connect()

    nodes, edges = load_dataset()
    labels = sorted({n["label"] for n in nodes})

    result = {"platform": platform, "timestamp": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())}
    result["ingest"] = run_ingest(adapter, nodes, edges)

    start_ids = sample_ids(nodes)
    result["traversals"] = run_traversals(adapter, start_ids)
    result["lookups"] = run_lookups(adapter, start_ids, labels)
    result["aggregation"] = run_aggregation(adapter)
    result["footprint"] = adapter.footprint()

    adapter.close()
    result["mixed_workload"] = run_mixed_workload(lambda: build_adapter(platform), start_ids)

    out_path = RESULTS_DIR / f"{platform}.json"
    out_path.write_text(json.dumps(result, indent=2))
    print(f"wrote {out_path}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--platform", required=True,
        choices=["cognodb", "neo4j", "memgraph", "arangodb", "dgraph", "all"],
    )
    args = parser.parse_args()

    platforms = (
        ["cognodb", "neo4j", "memgraph", "arangodb", "dgraph"]
        if args.platform == "all" else [args.platform]
    )
    for p in platforms:
        run_platform(p)
