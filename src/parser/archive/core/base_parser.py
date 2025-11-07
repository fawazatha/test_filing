from __future__ import annotations

import os
import json
from abc import ABC, abstractmethod
from typing import List, Dict, Any, Optional
from pathlib import Path

import logging
import pdfplumber

from src.common.log import get_logger
from src.parser.utils.alert_manager import AlertManager
from src.parser.utils.company.io import load_symbol_to_name_from_file
from src.parser.utils.company.resolver import build_reverse_map

logger = get_logger(__name__)

# PDFMiner loggers that are commonly noisy
_PDFMINER_LOGGERS = [
    "pdfminer", "pdfminer.psparser", "pdfminer.pdfparser", "pdfminer.pdfdocument",
    "pdfminer.pdfinterp", "pdfminer.pdfpage", "pdfminer.cmapdb", "pdfminer.layout",
    "pdfminer.image", "pdfminer.converter", "pdfminer.pdfdevice", "pdfminer.utils",
]

def _basic_root_config():
    """Initialize root logging only if not already configured."""
    root = logging.getLogger()
    if not root.handlers:
        logging.basicConfig(
            level=os.getenv("LOG_LEVEL", "INFO"),
            format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        )

def silence_pdfminer(level: int = logging.WARNING) -> None:
    """Lower PDFMiner log level and disable propagation to root."""
    for name in _PDFMINER_LOGGERS:
        lg = logging.getLogger(name)
        lg.setLevel(level)
        lg.propagate = False

class _PdfMinerChatterFilter(logging.Filter):
    """Filter out extremely verbose PDFMiner messages."""
    NOISE = ("seek:", "find_xref", "xref found", "nextline:", "nexttoken:", "read_xref_from")
    def filter(self, record: logging.LogRecord) -> bool:
        try:
            msg = record.getMessage()
        except Exception:
            return True
        return not any(x in msg for x in self.NOISE)

def init_logging(pdf_debug: Optional[bool] = None) -> None:
    """
    Initialize logging and control PDFMiner verbosity.
    """
    _basic_root_config()

    if pdf_debug is None:
        env = os.getenv("PDF_DEBUG", "0").strip().lower()
        pdf_debug = env in ("1", "true", "yes", "on")

    if not pdf_debug:
        silence_pdfminer(logging.WARNING)
        logging.getLogger("pdfminer").addFilter(_PdfMinerChatterFilter())

    # Reduce noise from other common libs
    logging.getLogger("urllib3").setLevel(logging.WARNING)
    logging.getLogger("PIL").setLevel(logging.WARNING)


# Base Parser
class BaseParser(ABC):
    """Base class for PDF parsers."""

    def __init__(
        self,
        pdf_folder: str,
        output_file: str,
        announcement_json: Optional[str] = None,
        alerts_file: Optional[str] = None,
        alerts_not_inserted_file: Optional[str] = None,
    ):
        init_logging(pdf_debug=None)

        self.pdf_folder = pdf_folder
        self.output_file = output_file
        self.announcement_json = announcement_json

        alerts_file = alerts_file or "alerts/alerts_idx.json"
        alerts_not_inserted_file = alerts_not_inserted_file or "alerts/alerts_not_inserted.json"

        os.makedirs(os.path.dirname(self.output_file), exist_ok=True)
        os.makedirs(os.path.dirname(alerts_file), exist_ok=True)
        os.makedirs(os.path.dirname(alerts_not_inserted_file), exist_ok=True)

        self.alert_manager = AlertManager(
            alert_file=alerts_file,
            preload_existing=False
        )
        self.alert_manager_not_inserted = AlertManager(
            alert_file=alerts_not_inserted_file,
            preload_existing=False
        )
        
        self.symbol_to_name: Dict[str, str] = load_symbol_to_name_from_file() or {}
        self.rev_company_map: Dict[str, List[str]] = build_reverse_map(self.symbol_to_name)
        self.company_names: List[str] = sorted({
            (name or "").strip() for name in self.symbol_to_name.values() if name
        })

        if self.company_names:
            logger.info(f"Loaded {len(self.company_names)} company names")
        else:
            logger.warning("No company names loaded. Check COMPANY_MAP_FILE env var.")

    def build_pdf_mapping(self) -> Dict[str, Any]:
        """Build mapping from PDF filenames to announcement metadata."""
        if not self.announcement_json or not os.path.exists(self.announcement_json):
            return {}

        try:
            with open(self.announcement_json, "r", encoding="utf-8") as f:
                announcements = json.load(f)
        except Exception as e:
            logger.error(f"Error loading announcement JSON: {e}")
            return {}

        file_to_announcement: Dict[str, Any] = {}

        for ann in announcements:
            main_link = ann.get("main_link", "")
            if main_link:
                main_file = os.path.basename(main_link.strip())
                file_to_announcement[main_file] = ann

            for attachment in ann.get("attachments", []):
                filename = attachment.get("filename", "").strip()
                url = attachment.get("url", "").strip()

                if filename:
                    file_to_announcement[filename] = ann
                if url:
                    file_from_url = os.path.basename(url)
                    file_to_announcement[file_from_url] = ann

        return file_to_announcement

    def extract_text_from_pdf(self, filepath: str) -> Optional[str]:
        """Extract plain text from a PDF file using pdfplumber."""
        try:
            with pdfplumber.open(filepath) as pdf:
                logger.debug(f"Opened {filepath} with {len(pdf.pages)} pages")
                text_parts: List[str] = []
                for page in pdf.pages:
                    try:
                        page_text = page.extract_text()
                    except Exception as e:
                        logger.warning(f"extract_text error on {os.path.basename(filepath)} page {page.page_number}: {e}")
                        page_text = None
                    if page_text:
                        text_parts.append(page_text)
                text = "\n".join(text_parts)
                return text.strip() if text and text.strip() else None
        except Exception as e:
            logger.error(f"Error extracting text from {filepath}: {e}")
            return None

    @abstractmethod
    def parse_single_pdf(self, filepath: str, filename: str, pdf_mapping: Dict[str, Any]) -> Optional[Any]:
        """Parse a single PDF file and return a structured dict or list."""
        pass

    @abstractmethod
    def validate_parsed_data(self, data: Any) -> bool:
        """Return True if parsed data is structurally valid and complete."""
        pass

    def save_debug_output(self, filename: str, text: str):
        """Persist extracted raw text for debugging purposes."""
        debug_dir = "debug_output"
        os.makedirs(debug_dir, exist_ok=True)
        debug_file = Path(debug_dir) / f"{Path(filename).name}.txt"

        try:
            debug_file.write_text(text, encoding="utf-8")
        except Exception as e:
            logger.error(f"Error saving debug output for {filename}: {e}")

    def parse_folder(self) -> List[Any]:
        """Parse all PDFs in the configured folder and persist results + alerts."""
        if not os.path.exists(self.pdf_folder):
            logger.error(f"Folder not found: {self.pdf_folder}")
            return []
        self.alert_manager.reset_file()
        self.alert_manager_not_inserted.reset_file()

        # Pre-reset output file
        try:
            with open(self.output_file, "w", encoding="utf-8") as f:
                json.dump([], f, ensure_ascii=False, indent=2)
        except Exception as e:
            logger.error(f"Error pre-reset output file {self.output_file}: {e}")

        parsed_results: List[Any] = []
        pdf_mapping = self.build_pdf_mapping()
        pdf_files = [f for f in os.listdir(self.pdf_folder) if f.lower().endswith(".pdf")]

        logger.info(f"Found {len(pdf_files)} PDF files to process")

        for filename in pdf_files:
            filepath = os.path.join(self.pdf_folder, filename)
            logger.info(f"Processing {filename}...")

            ann_ctx = pdf_mapping.get(filename, {})

            try:
                result = self.parse_single_pdf(filepath, filename, pdf_mapping)
                if result and self.validate_parsed_data(result):
                    parsed_results.append(result)
                    logger.info(f"Successfully parsed {filename}")
                else:
                    self.alert_manager_not_inserted.log_alert(
                        filename,
                        "Validation failed or no data extracted",
                        {"announcement": ann_ctx}
                    )
                    logger.warning(f"Validation failed for {filename}")

            except Exception as e:
                logger.error(f"Error processing {filename}: {e}", exc_info=True)
                self.alert_manager.log_alert(
                    filename,
                    f"Processing error: {str(e)}",
                    {"announcement": ann_ctx}
                )

        # Save results and alerts (overwrite)
        self.save_results(parsed_results)
        self.alert_manager.save_alerts()
        self.alert_manager_not_inserted.save_alerts()

        logger.info(f"Processing complete. {len(parsed_results)} files successfully parsed")
        return parsed_results

    def save_results(self, results: List[Any]):
        """Write parsing results to the configured output file (overwrite)."""
        try:
            with open(self.output_file, "w", encoding="utf-8") as f:
                json.dump(results, f, indent=2, ensure_ascii=False)
            logger.info(f"Saved {len(results)} results to {self.output_file}")
        except Exception as e:
            logger.error(f"Error saving results: {e}")

