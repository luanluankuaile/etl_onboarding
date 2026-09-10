import csv
from pathlib import Path
from .control import ControlService
from .context import RuntimeContext, utc_now


def discover_csv(context: RuntimeContext, control: ControlService, pattern: str = "*.csv") -> list[Path]:
    files = []
    for path in sorted(context.landing_dir.glob(pattern)):
        if control.manifest(str(path), path.stat().st_size, path.stat().st_mtime, context.run_id, utc_now()):
            files.append(path)
    return files


def read_csv(path: Path) -> list[dict[str, str]]:
    with path.open(newline="", encoding="utf-8-sig") as handle:
        return list(csv.DictReader(handle))
