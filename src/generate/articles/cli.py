# Command-line entrypoint untuk generator & uploader.
from __future__ import annotations
import argparse
import os

from dotenv import load_dotenv, find_dotenv
# penting: usecwd=True dan override=False (biar env shell tidak ditimpa .env)
load_dotenv(find_dotenv(usecwd=True), override=False)

from .runner import run_from_filings, run_from_text_items
from .utils.io_utils import write_jsonl, read_json, read_jsonl, get_logger
from .utils.uploader import upload_news_file_cli  # asumsi signature: (input_path, table, dry_run, timeout)

log = get_logger(__name__)

# =========================
# Helpers
# =========================
def _add_common_args(ap: argparse.ArgumentParser):
    ap.add_argument("--company-map", default="data/company/company_map.json")
    ap.add_argument("--latest-prices", default="data/company/latest_prices.json")
    ap.add_argument("--use-llm", action="store_true", help="Pakai LLM untuk ringkasan/klasifikasi.")
    ap.add_argument(
        "--model",
        default=os.getenv(
            "GROQ_MODEL",
            os.getenv("OPENAI_MODEL", os.getenv("GEMINI_MODEL", "llama-3.3-70b-versatile")),
        ),
        help="Nama model LLM (sesuaikan dengan providernya).",
    )
    ap.add_argument(
        "--provider",
        default=os.getenv("LLM_PROVIDER", ""),
        help="groq|openai|gemini (optional). Kosongkan untuk autodetect dari API key.",
    )
    ap.add_argument("--prefer-symbol", action="store_true", help="Jika ada tickers & symbol, utamakan field symbol.")

    # Opsi auto-upload setelah generate
    ap.add_argument("--upload", action="store_true", help="Langsung upload ke Supabase setelah generate.")
    ap.add_argument("--upload-table", default=os.getenv("SUPABASE_TABLE", "idx_news"))
    ap.add_argument("--upload-dry-run", action="store_true")
    ap.add_argument("--upload-timeout", type=int, default=int(os.getenv("SUPABASE_TIMEOUT", "30")))

def _maybe_upload_after_generate(args, output_path: str):
    if not getattr(args, "upload", False):
        return
    log.info("Auto-upload diaktifkan: mengunggah %s ke Supabase table=%s (dry_run=%s, timeout=%s)",
             output_path, args.upload_table, args.upload_dry_run, args.upload_timeout)
    # `upload_news_file_cli` diharapkan menangani normalisasi kolom & NULL (dimension/votes/score)
    upload_news_file_cli(
        input_path=output_path,
        table=args.upload_table,
        dry_run=args.upload_dry_run,
        timeout=args.upload_timeout,
    )

# =========================
# Commands
# =========================
def _cmd_generate_from_filings(args):
    filings = read_json(args.input)
    if not isinstance(filings, list):
        raise SystemExit("Input filings harus berupa JSON array.")
    articles = run_from_filings(
        filings,
        company_map_path=args.company_map,
        latest_prices_path=args.latest_prices,
        use_llm=args.use_llm,
        model_name=args.model,
        prefer_symbol=args.prefer_symbol,
        provider=(args.provider or None),
    )
    write_jsonl(args.output, articles)
    log.info("Wrote %d articles to %s", len(articles), args.output)
    _maybe_upload_after_generate(args, args.output)

def _cmd_generate_from_text(args):
    items = read_jsonl(args.input)
    articles = run_from_text_items(
        items,
        company_map_path=args.company_map,
        latest_prices_path=args.latest_prices,
        use_llm=args.use_llm,
        model_name=args.model,
        prefer_symbol=args.prefer_symbol,
        provider=(args.provider or None),
    )
    write_jsonl(args.output, articles)
    log.info("Wrote %d articles to %s", len(articles), args.output)
    _maybe_upload_after_generate(args, args.output)

def _cmd_upload_news(args):
    # Subcommand upload manual (tanpa generate)
    upload_news_file_cli(
        input_path=args.input,
        table=args.table,
        dry_run=args.dry_run,
        timeout=args.timeout,
    )

# =========================
# Main
# =========================
def main():
    ap = argparse.ArgumentParser(description="Modular Article Generator CLI")
    sub = ap.add_subparsers(dest="cmd", required=True)

    # Generate dari filings JSON (array dokumen)
    p1 = sub.add_parser("generate-from-filings", help="Generate dari filings JSON (array).")
    p1.add_argument("--input", required=True, help="Path ke JSON array filings.")
    p1.add_argument("--output", required=True, help="Path output JSONL artikel.")
    _add_common_args(p1)

    # Generate dari text JSONL (satu item per baris)
    p2 = sub.add_parser("generate-from-text", help="Generate dari text JSONL.")
    p2.add_argument("--input", required=True, help="Path ke JSONL berisi item teks.")
    p2.add_argument("--output", required=True, help="Path output JSONL artikel.")
    _add_common_args(p2)

    # Upload saja (tanpa generate)
    p3 = sub.add_parser("upload-news", help="Upload artikel ke Supabase table (default idx_news).")
    p3.add_argument("--input", required=True, help="Path JSONL artikel.")
    p3.add_argument("--table", default=os.getenv("SUPABASE_TABLE", "idx_news"))
    p3.add_argument("--dry-run", action="store_true")
    p3.add_argument("--timeout", type=int, default=int(os.getenv("SUPABASE_TIMEOUT", "30")))

    args = ap.parse_args()
    if args.cmd == "generate-from-filings":
        _cmd_generate_from_filings(args)
    elif args.cmd == "generate-from-text":
        _cmd_generate_from_text(args)
    elif args.cmd == "upload-news":
        _cmd_upload_news(args)

if __name__ == "__main__":
    main()
