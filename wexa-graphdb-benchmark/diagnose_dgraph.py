import os, time
from dotenv import load_dotenv
import pydgraph

load_dotenv()

def build_query(hops, frontier_cap=50):
    inner = "uid"
    for _ in range(hops):
        inner = f"rel (first: {frontier_cap}) {{ {inner} }}"
    return f"query q($id: string) {{ start(func: eq(ext_id, $id)) {{ {inner} }} }}"

client_stub = pydgraph.DgraphClientStub(os.environ["DGRAPH_GRPC"])
client = pydgraph.DgraphClient(client_stub)

hub_id = "53213"  # the degree-504 node from earlier

for hops in (1, 2, 3):
    query = build_query(hops)
    t0 = time.perf_counter()
    try:
        res = client.txn(read_only=True).query(query, variables={"$id": hub_id})
        elapsed = time.perf_counter() - t0
        print(f"hop={hops}: OK in {elapsed:.2f}s")
    except Exception as e:
        elapsed = time.perf_counter() - t0
        print(f"hop={hops}: FAILED after {elapsed:.2f}s - {type(e).__name__}: {e}")
        break

client_stub.close()
