# # Command-line entrypoint untuk generator & uploader.
# from __future__ import annotations
# import argparse
# import os

# from dotenv import load_dotenv, find_dotenv
# # penting: usecwd=True dan override=False (biar env shell tidak ditimpa .env)
# load_dotenv(find_dotenv(usecwd=True), override=False)

# from ..runner import run_from_filings, run_from_text_items
# from ..utils.io_utils import write_jsonl, read_json, read_jsonl, get_logger
# from ..utils.uploader import upload_news_file_cli

# log = get_logger(__name__)

# def _add_common_args(ap: argparse.ArgumentParser):
#     ap.add_argument("--company-map", default="data/company/company_map.json")
#     ap.add_argument("--latest-prices", default="data/company/latest_prices.json")
#     ap.add_argument("--use-llm", action="store_true", help="Pakai LLM untuk ringkasan/klasifikasi.")
#     ap.add_argument("--model", default=os.getenv("GROQ_MODEL", os.getenv("OPENAI_MODEL", "llama-3.3-70b-versatile")))
#     ap.add_argument("--provider", default=os.getenv("LLM_PROVIDER", ""), help="groq|openai (optional).")
#     ap.add_argument("--prefer-symbol", action="store_true")

# def _cmd_generate_from_filings(args):
#     filings = read_json(args.input)
#     if not isinstance(filings, list):
#         raise SystemExit("Input filings harus berupa JSON array.")
#     articles = run_from_filings(
#         filings,
#         company_map_path=args.company_map,
#         latest_prices_path=args.latest_prices,
#         use_llm=args.use_llm,
#         model_name=args.model,
#         prefer_symbol=args.prefer_symbol,
#         provider=(args.provider or None),
#     )
#     write_jsonl(args.output, articles)
#     log.info(f"Wrote {len(articles)} articles to {args.output}")

# def _cmd_generate_from_text(args):
#     items = read_jsonl(args.input)
#     articles = run_from_text_items(
#         items,
#         company_map_path=args.company_map,
#         latest_prices_path=args.latest_prices,
#         use_llm=args.use_llm,
#         model_name=args.model,
#         prefer_symbol=args.prefer_symbol,
#         provider=(args.provider or None),
#     )
#     write_jsonl(args.output, articles)
#     log.info(f"Wrote {len(articles)} articles to {args.output}")

# def _cmd_upload_news(args):
#     upload_news_file_cli(input_path=args.input, table=args.table, dry_run=args.dry_run, timeout=args.timeout)

# def main():
#     ap = argparse.ArgumentParser(description="Modular Article Generator CLI")
#     sub = ap.add_subparsers(dest="cmd", required=True)

#     p1 = sub.add_parser("generate-from-filings", help="Generate dari filings JSON (array).")
#     p1.add_argument("--input", required=True)
#     p1.add_argument("--output", required=True)
#     _add_common_args(p1)

#     p2 = sub.add_parser("generate-from-text", help="Generate dari text JSONL.")
#     p2.add_argument("--input", required=True)
#     p2.add_argument("--output", required=True)
#     _add_common_args(p2)

#     p3 = sub.add_parser("upload-news", help="Upload artikel ke Supabase table (default idx_news).")
#     p3.add_argument("--input", required=True)
#     p3.add_argument("--table", default="idx_news")
#     p3.add_argument("--dry-run", action="store_true")
#     p3.add_argument("--timeout", type=int, default=30)

#     args = ap.parse_args()
#     if args.cmd == "generate-from-filings":
#         _cmd_generate_from_filings(args)
#     elif args.cmd == "generate-from-text":
#         _cmd_generate_from_text(args)
#     elif args.cmd == "upload-news":
#         _cmd_upload_news(args)

# if __name__ == "__main__":
#     main()
