import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from src.build_b407_dashboard import build


if __name__ == "__main__":
    build()
