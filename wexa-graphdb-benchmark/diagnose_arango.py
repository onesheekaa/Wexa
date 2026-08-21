import csv, os, time
from pathlib import Path
from dotenv import load_dotenv
from arango import ArangoClient

load_dotenv()
DATA_DIR = Path("data")

with open(DATA_DIR / "nodes.csv") as f:
    nodes = list(csv.DictReader(f))
with open(DATA_DIR / "edges.csv") as f:
    edges = list(csv.DictReader(f))

degree = {}
for e in edges:
    degree[e["src"]] = degree.get(e["src"], 0) + 1
    degree[e["dst"]] = degree.get(e["dst"], 0) + 1
hub_id = max(degree, key=degree.get)
print(f"hub node: id={hub_id}, degree={degree[hub_id]}")
print()

url = os.environ["ARANGO_URI"]
user = os.environ.get("ARANGO_USER", "root")
pwd = os.environ["ARANGO_PASSWORD"]
db_name = os.environ.get("ARANGO_DB", "benchmark")

client = ArangoClient(hosts=url)
db = client.db(db_name, username=user, password=pwd)

def build_query(hops, final_limit=100):
    return (
        f"FOR v IN {hops}..{hops} OUTBOUND @start edges "
        'OPTIONS { bfs: true, uniqueVertices: "global" } '
        f"LIMIT {final_limit} "
        "RETURN DISTINCT v"
    )

for hops in (1, 2, 3):
    query = build_query(hops)
    t0 = time.perf_counter()
    try:
        result = list(db.aql.execute(query, bind_vars={"start": f"nodes/{hub_id}"}))
        print(f"hop={hops}: OK in {time.perf_counter()-t0:.2f}s, {len(result)} rows")
    except Exception as e:
        print(f"hop={hops}: FAILED after {time.perf_counter()-t0:.2f}s - {type(e).__name__}: {e}")
        break
