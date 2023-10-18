#!/usr/bin/env python3
import json
from pathlib import Path


def main() -> None:
    fixture_dir = Path("tests/fixtures")
    fixtures = {}
    for path in sorted(fixture_dir.glob("*.json")):
        fixtures[path.name] = json.loads(path.read_text(encoding="utf-8"))
    print(json.dumps({"fixture_count": len(fixtures), "fixtures": list(fixtures)}, indent=2))


if __name__ == "__main__":
    main()

