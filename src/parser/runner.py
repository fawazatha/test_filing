import sys
from pathlib import Path

# Add the project root (the directory containing 'src') to Python path
# This allows `from src.parser.cli import main` to work
project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(project_root))

try:
    from src.parser.cli import main
except ImportError:
    print("Error: Could not import 'src.parser.cli'.")
    print(f"Please ensure 'src' is in your PYTHONPATH or run from the project root.")
    print(f"Attempted to add: {project_root}")
    sys.exit(1)


if __name__ == "__main__":
    main()
