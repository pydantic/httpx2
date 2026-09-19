from __future__ import annotations

import argparse
import subprocess
import sys
from pathlib import Path


def main() -> None:
    parser = argparse.ArgumentParser(description="Compile selected modules in an isolated HTTPX2 frontend copy.")
    parser.add_argument("directory", type=Path)
    args = parser.parse_args()
    directory = args.directory.resolve()
    if not (directory / "httpx2/_models.py").is_file():
        parser.error("Run prepare_frontend.py into this directory first")
    build_script = directory / "build_frontend.py"
    build_script.write_text(
        "from setuptools import setup\n"
        "from Cython.Build import cythonize\n"
        "setup(ext_modules=cythonize([\n"
        '    "httpx2/_models.py", "httpx2/_client.py", "httpx2/_urls.py",\n'
        '    "httpx2/_decoders.py", "httpx2/_utils.py",\n'
        '], compiler_directives={"language_level": 3, "annotation_typing": False}, nthreads=0))\n'
    )
    subprocess.run([sys.executable, str(build_script), "build_ext", "--inplace"], cwd=directory, check=True)


if __name__ == "__main__":
    main()
