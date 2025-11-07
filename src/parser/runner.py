import sys
from pathlib import Path

# Add the src directory to Python path
src_path = Path(__file__).parent.parent
sys.path.insert(0, str(src_path))

from parser.cli import main

if __name__ == "__main__":
    main()