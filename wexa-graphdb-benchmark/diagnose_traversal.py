import csv, os, time
from pathlib import Path
from dotenv import load_dotenv
from neo4j import GraphDatabase

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
print(f"Highest-degree node: id={hub_id}, degree={degree[hub_id]}")
print(f"median degree: {sorted(degree.values())[len(degree)//2]}")
print()

uri = os.environ["COGNODB_URI"]
user = os.environ.get("COGNODB_USER", "cognodb")
pwd = os.environ["COGNODB_PASSWORD"]
driver = GraphDatabase.driver(uri, auth=(user, pwd))
driver.verify_connectivity()

for label, start_id in [("random/average node", nodes[100]["id"]), ("highest-degree hub", hub_id)]:
    print(f"=== {label} (id={start_id}) ===")
    for hops in (1, 2, 3):
        query = f"MATCH (n:Node {{id: $id}})-[:REL*{hops}]->(m) RETURN DISTINCT m LIMIT 100"
        t0 = time.perf_counter()
        try:
            with driver.session() as s:
                result = list(s.run(query, id=start_id))
            print(f"  hop={hops}: OK in {time.perf_counter()-t0:.2f}s, {len(result)} rows")
        except Exception as e:
            print(f"  hop={hops}: FAILED after {time.perf_counter()-t0:.2f}s - {type(e).__name__}: {e}")
            break
    print()

driver.close()
