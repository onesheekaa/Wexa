"""
Downloads a real public dataset and converts it into the nodes.csv /
edges.csv format every adapter expects.

Dataset: ca-AstroPh (SNAP) - a co-authorship network from arXiv
Astrophysics. ~18.7k nodes, ~198k directed edges once symmetrized,
which sits comfortably in the 100k-500k relationship range the
assignment asks for.
Source: https://snap.stanford.edu/data/ca-AstroPh.html
Cite as: J. Leskovec, A. Krevl. SNAP Datasets: Stanford Large Network
Dataset Collection. http://snap.stanford.edu/data (2014).

The dataset has no natural node "label" - we assign one from a small
fixed set (seeded, so it's reproducible) purely so the filtered-lookup
and aggregation workloads have something to filter/group on. This is
disclosed in the README, not hidden.

Run: python -m src.dataset
"""
import csv
import gzip
import random
import urllib.request
from pathlib import Path

DATA_DIR = Path(__file__).resolve().parent.parent / "data"
DATA_DIR.mkdir(exist_ok=True)

SOURCE_URL = "https://snap.stanford.edu/data/ca-AstroPh.txt.gz"
RAW_PATH = DATA_DIR / "ca-AstroPh.txt.gz"
LABELS = ["core", "collaborator", "prolific", "occasional"]


def download() -> None:
    if RAW_PATH.exists():
        print(f"already downloaded: {RAW_PATH}")
        return
    print(f"downloading {SOURCE_URL} ...")
    urllib.request.urlretrieve(SOURCE_URL, RAW_PATH)


def build_csvs() -> None:
    node_ids = set()
    seen_pairs = set()
    edges = []
    with gzip.open(RAW_PATH, "rt") as f:
        for line in f:
            if line.startswith("#"):
                continue
            src, dst = line.strip().split("\t")
            node_ids.add(src)
            node_ids.add(dst)
            # BUGFIX: ca-AstroPh.txt.gz stores each undirected collaboration
            # as TWO directed lines (src->dst and dst->src) - SNAP's standard
            # format for symmetrized graphs. Loading every line as-is
            # silently doubles the documented edge count and makes every
            # traversal query walk a much denser graph than intended.
            # Dedupe to one edge per undirected pair.
            pair = tuple(sorted((src, dst)))
            if pair in seen_pairs:
                continue
            seen_pairs.add(pair)
            edges.append((src, dst))

    random.seed(42)
    with open(DATA_DIR / "nodes.csv", "w", newline="") as f:
        w = csv.writer(f)
        w.writerow(["id", "label"])
        for nid in sorted(node_ids):
            w.writerow([nid, random.choice(LABELS)])

    with open(DATA_DIR / "edges.csv", "w", newline="") as f:
        w = csv.writer(f)
        w.writerow(["src", "dst", "type"])
        for src, dst in edges:
            w.writerow([src, dst, "COAUTHOR"])

    print(f"nodes: {len(node_ids)}, edges: {len(edges)}")


if __name__ == "__main__":
    download()
    build_csvs()
