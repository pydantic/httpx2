from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import run as harness

harness.CLIENT = Path(__file__).with_name("worker.py")
harness.PROFILE_LIBS = ("fused-url-buffered", "fused-url")

if __name__ == "__main__":
    harness.main()
