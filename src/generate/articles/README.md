# Generate Articles Module

Creates news-style articles from filings and uploads them if configured. Focused on transforming structured filings into narrative content for distribution channels.

## Directory Contents
- `cli.py` — entrypoint for article generation.
- `runner.py` — orchestrates article generation (wrapper around `generator.py` or helper funcs).
- `generator.py` — core logic to convert filings data into article text/structure.
- `__init__.py` — package marker.

## Critical Functions / Responsibilities
- `cli.py`
  - `main()`: parse args (input/output, upload toggle, verbosity), load env, invoke runner.
- `runner.py`
  - `run_from_filings(...)` (referenced in orchestrator): drive generation given filings input.
  - `run(...)`: generic entry to call `generator` with appropriate parameters.
- `generator.py`
  - Core article building: turn filings (symbol, holder, transaction details, tags) into headline + body, with optional summaries/snippets.
  - May call LLM/template helpers depending on configuration.
- `utils/uploader.py` (referenced in orchestrator)
  - Upload generated articles to downstream store/service when enabled.

## Data Flow
1) **Inputs**: filings data (typically output from `generate/filings` or Supabase MV); may read from provided JSON or DB depending on runner implementation.
2) **Generate**: build article headlines/body/snippets per filing using templates/LLM as configured in `generator.py`.
3) **Upload/Publish**: optional; depends on uploader hooks (e.g., `generate.articles.utils.uploader` referenced in pipeline/orchestrator).

## Usage (typical)
Invoked directly or via orchestrator:
```bash
python -m src.generate.articles.cli [--input filings.json] [--out articles.json]
```
Or from pipeline: `generate.articles.runner.run_from_filings(...)`.

Actual flags depend on your wiring in `cli.py`; common patterns:
- Input path to filings.
- Output path for generated articles.
- Optional upload toggle.

## Extending
- Update `generator.py` to adjust templates, summarization, or tone.
- Add new channels/uploaders in `utils/uploader.py` and wire in `runner.py`/`cli.py`.
- If using LLMs, ensure required env keys are set (e.g., OpenAI/Groq/Gemini).

## Environment/Config
- LLM keys as needed (e.g., `OPENAI_API_KEY`, `GROQ_API_KEY`, `GEMINI_API_KEY`).
- Any channel-specific config required by your uploader.
