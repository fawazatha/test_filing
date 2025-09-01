import os
import json
import logging
from abc import ABC, abstractmethod
from typing import List, Dict, Any, Optional
import pdfplumber

from ..utils.alert_manager import AlertManager
from ..utils.company_resolver import (
    load_symbol_to_name_from_file,
    build_reverse_map,
)

logger = logging.getLogger(__name__)


class BaseParser(ABC):
    """Base class for PDF parsers."""

    def __init__(self, pdf_folder: str, output_file: str, announcement_json: Optional[str] = None):
        self.pdf_folder = pdf_folder
        self.output_file = output_file
        self.announcement_json = announcement_json
        self.alert_manager = AlertManager(
            alert_file="alerts/alerts_idx.json",
            preload_existing=False
        )
        self.alert_manager_not_inserted = AlertManager(
            alert_file="alerts/alerts_not_inserted.json",
            preload_existing=False
        )

        os.makedirs(os.path.dirname(output_file), exist_ok=True)

        self.symbol_to_name: Dict[str, str] = load_symbol_to_name_from_file() or {}
        self.rev_company_map: Dict[str, List[str]] = build_reverse_map(self.symbol_to_name)
        self.company_names: List[str] = sorted({
            (name or "").strip() for name in self.symbol_to_name.values() if name
        })

        if self.company_names:
            logger.info(f"Loaded {len(self.company_names)} company names from company_map.json")
        else:
            logger.warning("No company names loaded. Check data/company/company_map.json or env COMPANY_MAP_FILE")

    def build_pdf_mapping(self) -> Dict[str, Any]:
        """Build mapping from PDF files to announcement metadata."""
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
        """Extract text from PDF file."""
        try:
            with pdfplumber.open(filepath) as pdf:
                logger.debug(f"Opened {filepath} with {len(pdf.pages)} pages")
                text = "\n".join([
                    page.extract_text() for page in pdf.pages
                    if page.extract_text()
                ])
                return text.strip() if text.strip() else None
        except Exception as e:
            logger.error(f"Error extracting text from {filepath}: {e}")
            return None

    @abstractmethod
    def parse_single_pdf(self, filepath: str, filename: str, pdf_mapping: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """Parse a single PDF file. Must be implemented by subclasses."""
        pass

    @abstractmethod
    def validate_parsed_data(self, data: Dict[str, Any]) -> bool:
        """Validate parsed data. Must be implemented by subclasses."""
        pass

    def save_debug_output(self, filename: str, text: str):
        """Save extracted text for debugging."""
        debug_dir = "debug_output"
        os.makedirs(debug_dir, exist_ok=True)
        debug_file = os.path.join(debug_dir, f"{filename}.txt")

        try:
            with open(debug_file, "w", encoding="utf-8") as f:
                f.write(text)
        except Exception as e:
            logger.error(f"Error saving debug output for {filename}: {e}")

    def parse_folder(self) -> List[Dict[str, Any]]:
        if not os.path.exists(self.pdf_folder):
            logger.error(f"Folder not found: {self.pdf_folder}")
            return []
        self.alert_manager.reset_file()
        self.alert_manager_not_inserted.reset_file()

        try:
            with open(self.output_file, "w", encoding="utf-8") as f:
                json.dump([], f, ensure_ascii=False, indent=2)
        except Exception as e:
            logger.error(f"Error pre-reset output file {self.output_file}: {e}")

        parsed_results: List[Dict[str, Any]] = []
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
                logger.error(f"Error processing {filename}: {e}")
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

    def save_results(self, results: List[Dict[str, Any]]):
        """Save parsing results to output file (overwrite)."""
        try:
            with open(self.output_file, "w", encoding="utf-8") as f:
                json.dump(results, f, indent=2, ensure_ascii=False)
            logger.info(f"Saved {len(results)} results to {self.output_file}")
        except Exception as e:
            logger.error(f"Error saving results: {e}")
