import os
from dataclasses import dataclass
from typing import Optional

@dataclass
class ParserConfig:
    """Configuration for PDF parsers."""
    
    # Directories
    idx_pdf_folder: str = "downloads/idx-format"
    non_idx_pdf_folder: str = "downloads/non-idx-format"
    output_dir: str = "data"
    debug_output_dir: str = "debug_output"
    alerts_dir: str = "alerts"
    
    # Files
    announcement_json: str = "data/idx_announcements.json"
    idx_output_file: str = "data/parsed_idx_output.json"
    non_idx_output_file: str = "data/parsed_non_idx_output.json"
    
    # Database
    supabase_url: Optional[str] = None
    supabase_key: Optional[str] = None
    
    # Processing
    fuzzy_match_threshold: int = 85
    enable_debug_output: bool = True
    
    def __post_init__(self):
        """Load environment variables if not provided."""
        if not self.supabase_url:
            self.supabase_url = os.getenv("SUPABASE_URL")
        if not self.supabase_key:
            self.supabase_key = os.getenv("SUPABASE_KEY")
