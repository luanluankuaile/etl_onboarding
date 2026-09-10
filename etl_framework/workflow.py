from dataclasses import dataclass, field
from collections.abc import Callable
from .context import RuntimeContext


@dataclass
class DAG:
    tasks: dict[str, Callable] = field(default_factory=dict)
    dependencies: dict[str, set[str]] = field(default_factory=dict)

    def add(self, name: str, task: Callable, depends_on: list[str] | None = None):
        self.tasks[name] = task; self.dependencies[name] = set(depends_on or [])

    def run(self, context: RuntimeContext):
        completed = set()
        while len(completed) < len(self.tasks):
            ready = [n for n in self.tasks if n not in completed and self.dependencies[n] <= completed]
            if not ready: raise ValueError("DAG contains a cycle or unknown dependency")
            for name in ready:
                self.tasks[name](context); completed.add(name)
