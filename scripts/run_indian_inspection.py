"""DemandLens - Run Indian Job Market Dataset Quality Inspection."""

import sys
from pathlib import Path

# Add project root to sys.path
PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.demandlens.quality.inspect_indian_quality import main

if __name__ == "__main__":
    main()
