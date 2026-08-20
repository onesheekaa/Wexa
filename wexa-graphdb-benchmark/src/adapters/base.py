"""
Abstract interface every graph-database adapter must implement.

Each adapter speaks its own native query language internally (Cypher,
AQL, DQL, ...). The runner never knows or cares which - it only calls
these methods, so every platform is exercised through an identical
logical workload. This is what makes the comparison fair: same calls,
same iteration counts, same warm-up, different engines underneath.
"""
from abc import ABC, abstractmethod
from typing import Iterable, List


class GraphAdapter(ABC):
    name: str  # short platform id, used as the results/<name>.json filename

    @abstractmethod
    def connect(self) -> None:
        """Open a connection / driver session."""

    @abstractmethod
    def close(self) -> None:
        """Close the connection cleanly."""

    @abstractmethod
    def wipe(self) -> None:
        """Delete all data so re-runs are repeatable from a clean slate."""

    @abstractmethod
    def create_indexes(self) -> List[str]:
        """Create whatever indexes this platform supports on the id/label
        properties used by the lookup workload. Return a human-readable
        list of what was created - this goes straight into the README's
        'which properties are indexed' disclosure."""

    @abstractmethod
    def load_nodes(self, nodes: Iterable[dict], batch_size: int = 1000) -> None:
        """Bulk-load nodes. Each dict has at least 'id' and 'label'."""

    @abstractmethod
    def load_edges(self, edges: Iterable[dict], batch_size: int = 1000) -> None:
        """Bulk-load edges. Each dict has 'src', 'dst', 'type'."""

    @abstractmethod
    def point_lookup(self, node_id: str) -> None:
        """Fetch a single node by its indexed id property."""

    @abstractmethod
    def filtered_lookup(self, label: str, limit: int = 50) -> None:
        """Fetch nodes filtered by an indexed label property."""

    @abstractmethod
    def traversal(self, start_id: str, hops: int) -> None:
        """Run a 1/2/3-hop traversal from start_id."""

    @abstractmethod
    def aggregation(self) -> None:
        """Count / group-by over a label or relationship type."""

    @abstractmethod
    def write_sample(self, node_id: str) -> None:
        """A small write, used by the mixed read/write workload."""

    @abstractmethod
    def footprint(self) -> dict:
        """Whatever the platform exposes about storage/memory use.
        Return {'note': 'not observable ...'} if it genuinely isn't -
        the assignment explicitly wants honesty here over guessing."""
