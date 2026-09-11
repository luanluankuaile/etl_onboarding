import csv
import hashlib
from pathlib import Path
from .control import ControlService
from .context import RuntimeContext, utc_now


def discover_csv(context: RuntimeContext, control: ControlService, pattern: str = "*.csv") -> list[Path]:
    """Select eligible files only; manifest writes happen after successful processing."""
    files = []
    for path in sorted(context.landing_dir.glob(pattern)):
        checksum = hashlib.sha256(path.read_bytes()).hexdigest()
        stat = path.stat()
        if control.manifest_eligible(str(path), stat.st_size, stat.st_mtime, checksum):
            files.append(path)
    return files


def file_checksum(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def read_csv(path: Path) -> list[dict[str, str]]:
    with path.open(newline="", encoding="utf-8-sig") as handle:
        return list(csv.DictReader(handle))
