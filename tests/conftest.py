import sys
from pathlib import Path

# Ensure src/ is on the Python path for test imports.
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))
