import argparse
import inspect
import json
import logging
from pathlib import Path
from typing import Any, Dict, Tuple

from . import parser_idx as parser_idx_mod
from . import parser_non_idx as parser_non_idx_mod

LOGGER = logging.getLogger("parser.cli")


def setup_logging(level: str = "INFO") -> None:
    numeric = getattr(logging, level.upper(), logging.INFO)
    logging.basicConfig(
        level=numeric,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )
    LOGGER.setLevel(numeric)


def build_argparser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description="Run IDX / Non-IDX PDF parsers.")
    p.add_argument("--parser", dest="parser_type", choices=["idx", "non-idx", "both"], default="idx")
    p.add_argument("--idx-folder", default="downloads/idx-format")
    p.add_argument("--idx-output", default="data/parsed_idx_output.json")
    p.add_argument("--non-idx-folder", default="downloads/non-idx-format")
    p.add_argument("--non-idx-output", default="data/parsed_non_idx_output.json")
    p.add_argument("--announcements", default="data/idx_announcements.json")
    p.add_argument("--log-level", default="INFO")
    return p


def _load_counts(out_path: str, alerts_path: str) -> Tuple[int, int]:
    """Return (parsed_count, alerts_count) by reading output & alerts files."""
    parsed = 0
    alerts = 0
    try:
        if Path(out_path).exists():
            data = json.loads(Path(out_path).read_text(encoding="utf-8") or "[]")
            if isinstance(data, list):
                parsed = len(data)
            elif isinstance(data, dict):
                parsed = len(data)
    except Exception:
        pass
    try:
        if Path(alerts_path).exists():
            adata = json.loads(Path(alerts_path).read_text(encoding="utf-8") or "[]")
            alerts = len(adata) if isinstance(adata, list) else len(adata)
    except Exception:
        pass
    return parsed, alerts


def run_idx_parser(args: argparse.Namespace):
    IDXClass = getattr(parser_idx_mod, "IDXParser", None)
    LOGGER.info("IDXParser symbol origin: %s", inspect.getsourcefile(IDXClass))  # type: ignore[arg-type]
    parser = IDXClass(
        pdf_folder=args.idx_folder,
        output_file=args.idx_output,
        announcement_json=args.announcements,
    )
    LOGGER.info("Using IDX parser class: %s", parser.__class__)
    parser.parse_folder()

    parsed, alerts = _load_counts(args.idx_output, "alerts/alerts_idx.json")
    LOGGER.info("IDX summary — parsed: %d, skipped/alerts: %d", parsed, alerts)


def run_non_idx_parser(args: argparse.Namespace):
    NonIDXClass = getattr(parser_non_idx_mod, "NonIDXParser", None)
    LOGGER.info("NonIDXParser symbol origin: %s", inspect.getsourcefile(NonIDXClass)) 
    parser = NonIDXClass(
        pdf_folder=args.non_idx_folder,
        output_file=args.non_idx_output,
        announcement_json=args.announcements,
    )
    LOGGER.info("Using Non-IDX parser class: %s", parser.__class__)
    parser.parse_folder()

    parsed, alerts = _load_counts(args.non_idx_output, "alerts/alerts_non_idx.json")
    LOGGER.info("Non-IDX summary — parsed: %d, skipped/alerts: %d", parsed, alerts)


def main() -> Dict[str, Any]:
    args = build_argparser().parse_args()
    setup_logging(args.log_level)

    LOGGER.info("Starting parsers…")
    results: Dict[str, Any] = {}
    try:
        if args.parser_type in ("idx", "both"):
            run_idx_parser(args)
        if args.parser_type in ("non-idx", "both"):
            run_non_idx_parser(args)

        LOGGER.info("All parsing completed.")
        return results
    except Exception as e:
        LOGGER.exception("Error in main execution: %s", e)
        raise


if __name__ == "__main__":
    main()
