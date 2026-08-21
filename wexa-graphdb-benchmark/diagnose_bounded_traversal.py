import os, time
from dotenv import load_dotenv
from neo4j import GraphDatabase

load_dotenv()

def build_query(hops, frontier_cap=50, final_limit=100):
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

uri = os.environ["COGNODB_URI"]
user = os.environ.get("COGNODB_USER", "cognodb")
pwd = os.environ["COGNODB_PASSWORD"]
driver = GraphDatabase.driver(uri, auth=(user, pwd))
driver.verify_connectivity()

hub_id = "53213"  # the degree-504 node from your last run

for hops in (1, 2, 3):
    query = build_query(hops)
    t0 = time.perf_counter()
    try:
        with driver.session() as s:
            result = list(s.run(query, id=hub_id))
        print(f"hop={hops}: OK in {time.perf_counter()-t0:.2f}s, {len(result)} rows")
    except Exception as e:
        print(f"hop={hops}: FAILED after {time.perf_counter()-t0:.2f}s - {type(e).__name__}: {e}")
        break

driver.close()
