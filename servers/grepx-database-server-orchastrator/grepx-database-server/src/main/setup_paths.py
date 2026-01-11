import sys
from pathlib import Path

project_root = Path(__file__).parent.parent.parent.parent
libs_path = project_root / "libs"

sys.path.insert(0, str(libs_path / "grepx-orm" / "src"))
sys.path.insert(0, str(libs_path / "grepx-connection-registry" / "src"))
sys.path.insert(0, str(Path(__file__).parent))
