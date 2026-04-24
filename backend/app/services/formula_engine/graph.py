"""Dependency graph — topological sort + Tarjan SCC for cycle detection."""
from __future__ import annotations

from collections import defaultdict, deque
from typing import Iterable

__all__ = ["DependencyGraph", "CycleError"]


class CycleError(Exception):
    def __init__(self, cycle: list[str]):
        self.cycle = cycle
        super().__init__(f"Cycle detected: {' -> '.join(cycle + [cycle[0]])}")


class DependencyGraph:
    """Directed graph where edge a -> b means "a depends on b"."""

    def __init__(self) -> None:
        # dependents[b] = {a : a depends on b}
        self._dependents: dict[str, set[str]] = defaultdict(set)
        # depends_on[a] = {b : a depends on b}
        self._depends_on: dict[str, set[str]] = defaultdict(set)
        # ensure every known node has an entry
        self._nodes: set[str] = set()

    # ── structure ──────────────────────────────────────────────

    def add_node(self, path: str) -> None:
        self._nodes.add(path)
        self._dependents.setdefault(path, set())
        self._depends_on.setdefault(path, set())

    def set_dependencies(self, path: str, deps: Iterable[str]) -> None:
        """Replace ``path``'s dependency set with ``deps``."""
        self.add_node(path)
        for old in list(self._depends_on[path]):
            self._dependents[old].discard(path)
        self._depends_on[path] = set()
        for d in deps:
            self.add_node(d)
            self._depends_on[path].add(d)
            self._dependents[d].add(path)

    def remove_node(self, path: str) -> None:
        for p in list(self._dependents.get(path, ())):
            self._depends_on[p].discard(path)
        for d in list(self._depends_on.get(path, ())):
            self._dependents[d].discard(path)
        self._dependents.pop(path, None)
        self._depends_on.pop(path, None)
        self._nodes.discard(path)

    def nodes(self) -> set[str]:
        return set(self._nodes)

    def depends_on(self, path: str) -> set[str]:
        return set(self._depends_on.get(path, ()))

    def dependents(self, path: str) -> set[str]:
        return set(self._dependents.get(path, ()))

    # ── cycle detection (Tarjan SCC) ──────────────────────────

    def find_cycle(self) -> list[str] | None:
        """Return a cycle of nodes if any, else None. O(V+E)."""
        index = 0
        stack: list[str] = []
        on_stack: set[str] = set()
        indices: dict[str, int] = {}
        lowlink: dict[str, int] = {}
        found_cycle: list[str] | None = None

        def strongconnect(v: str) -> None:
            nonlocal index, found_cycle
            indices[v] = index
            lowlink[v] = index
            index += 1
            stack.append(v)
            on_stack.add(v)
            for w in self._depends_on.get(v, ()):
                if w not in indices:
                    strongconnect(w)
                    if found_cycle is not None:
                        return
                    lowlink[v] = min(lowlink[v], lowlink[w])
                elif w in on_stack:
                    lowlink[v] = min(lowlink[v], indices[w])
            if lowlink[v] == indices[v]:
                scc: list[str] = []
                while True:
                    w = stack.pop()
                    on_stack.discard(w)
                    scc.append(w)
                    if w == v:
                        break
                # An SCC with ≥ 2 nodes or a self-loop is a cycle
                if len(scc) > 1 or (len(scc) == 1 and scc[0] in self._depends_on.get(scc[0], ())):
                    found_cycle = scc

        # iterative version via explicit stack to avoid recursion depth limits
        # (but we keep a functional recursive helper for simplicity — for models
        # of ~2000 cells, Python's default 1000 recursion limit suffices)
        for node in list(self._nodes):
            if node not in indices:
                strongconnect(node)
                if found_cycle is not None:
                    return found_cycle
        return None

    # ── topological order ─────────────────────────────────────

    def topo_order(self, roots: Iterable[str] | None = None) -> list[str]:
        """Return a topological ordering of reachable nodes (dependencies first)."""
        if self.find_cycle():
            raise CycleError(self.find_cycle() or [])
        if roots is None:
            start_nodes = set(self._nodes)
        else:
            start_nodes = set()
            queue = deque(roots)
            while queue:
                n = queue.popleft()
                if n in start_nodes:
                    continue
                start_nodes.add(n)
                queue.extend(self._depends_on.get(n, ()))

        order: list[str] = []
        visited: set[str] = set()

        def visit(n: str) -> None:
            if n in visited:
                return
            visited.add(n)
            for dep in sorted(self._depends_on.get(n, ())):
                if dep in start_nodes:
                    visit(dep)
            order.append(n)

        for n in sorted(start_nodes):
            visit(n)
        return order

    def transitive_dependents(self, path: str) -> set[str]:
        """Return every node whose value transitively depends on ``path``."""
        out: set[str] = set()
        queue = deque(self._dependents.get(path, ()))
        while queue:
            n = queue.popleft()
            if n in out:
                continue
            out.add(n)
            queue.extend(self._dependents.get(n, ()))
        return out
