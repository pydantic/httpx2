from __future__ import annotations

import argparse
import shutil
import subprocess
from pathlib import Path


def main() -> None:
    parser = argparse.ArgumentParser(description="Prepare an isolated, experimental zttp version of httpcore2.")
    parser.add_argument("output", type=Path)
    args = parser.parse_args()
    here = Path(__file__).resolve().parent
    destination = args.output.resolve()
    destination.mkdir(parents=True, exist_ok=True)
    shutil.copytree(here.parents[1] / "src/httpcore2/httpcore2", destination / "httpcore2", dirs_exist_ok=False)
    subprocess.run(["patch", "-p1", "-i", str(here / "zttp.patch")], cwd=destination, check=True)


if __name__ == "__main__":
    main()
