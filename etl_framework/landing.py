import csv
import hashlib
from pathlib import Path
from .control import ControlService
from .context import RuntimeContext


def discover_csv(context: RuntimeContext, control: ControlService, pattern: str = "*.csv") -> list[Path]:
    files = []
    for path in sorted(context.landing_dir.glob(pattern)):
        checksum = hashlib.sha256(path.read_bytes()).hexdigest()
        if control.manifest(str(path), path.stat().st_size, path.stat().st_mtime, checksum):
            files.append(path)
    return files


def read_csv(path: Path) -> list[dict[str, str]]:
    with path.open(newline="", encoding="utf-8-sig") as handle:
        return list(csv.DictReader(handle))
