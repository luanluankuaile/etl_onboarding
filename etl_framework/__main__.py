import argparse
import uuid
from .context import RuntimeContext
from .metadata import load_metadata
from .runner import ETLRunner


def main():
    parser = argparse.ArgumentParser(description="Run local metadata-driven ETL")
    parser.add_argument("metadata")
    parser.add_argument("--landing", default="landing")
    parser.add_argument("--environment", default="local")
    parser.add_argument("--run-id", default=None)
    args = parser.parse_args()
    root = __import__('pathlib').Path(args.landing).parent
    context = RuntimeContext(args.run_id or str(uuid.uuid4()), args.environment,
                             __import__('pathlib').Path(args.landing), root / "raw.sqlite",
                             root / "persistent.sqlite", root / "consumption.sqlite", root / "etl_control.sqlite")
    ETLRunner(load_metadata(args.metadata), context).run()


if __name__ == "__main__":
    main()
