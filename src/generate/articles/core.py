"""
Core article generator module.

This module provides the main ArticleGenerator class that orchestrates
the article generation process from filing data.
"""

import logging
from typing import Dict, Any, Optional

from .utils.data_extractor import FilingDataExtractor
from .utils.data_processor import FilingDataProcessor
from .utils.content_generator import ContentGenerator
from .utils.validator import ArticleValidator
from .model.article import Article
from .model.filing_info import FilingInfo

logger = logging.getLogger(__name__)


class ArticleGenerator:
    """
    Main article generator that orchestrates the filing-to-article conversion process.
    
    This class follows the single responsibility principle by delegating
    specific tasks to specialized utility classes.
    """

    def __init__(self):
        """Initialize the article generator with required components."""
        self.data_extractor = FilingDataExtractor()
        self.data_processor = FilingDataProcessor()
        self.content_generator = ContentGenerator()
        self.validator = ArticleValidator()
        
        logger.info("ArticleGenerator initialized successfully")

    def generate_article(
        self,
        pdf_url: str,
        sector: str,
        sub_sector: str,
        holder_type: str,
        filing_text: str,
        uid: Optional[str] = None,
    ) -> Dict[str, Any]:
        """
        Generate a complete article from filing data.

        Args:
            pdf_url (str): URL of the source PDF
            sector (str): Company sector
            sub_sector (str): Company sub-sector
            holder_type (str): Type of holder (insider, etc.)
            filing_text (str): Raw filing text content
            uid (Optional[str]): Unique identifier

        Returns:
            Dict[str, Any]: Complete article data

        Raises:
            ValueError: If required parameters are missing
            Exception: For other processing errors
        """
        if not filing_text or not filing_text.strip():
            raise ValueError("Filing text cannot be empty")

        logger.info(f"Starting article generation for PDF: {pdf_url}")

        try:
            # Step 1: Extract structured data from raw filing text
            filing_info = self.data_extractor.extract_filing_info(filing_text)
            logger.debug(f"Extracted filing info for holder: {filing_info.holder_name}")

            # Step 2: Create initial article structure
            article = Article.create_initial_structure(
                pdf_url=pdf_url,
                sector=sector,
                sub_sector=sub_sector,
                holder_type=holder_type,
                uid=uid
            )

            # Step 3: Process and populate article with filing data
            article = self.data_processor.populate_article_data(article, filing_info)
            logger.debug("Article data populated successfully")

            # Step 4: Generate enhanced content (title, body, tags)
            article = self.content_generator.enhance_article_content(article)
            logger.debug("Article content enhanced")

            # Step 5: Update sector information from external sources
            article = self.data_processor.update_sector_information(article)
            
            # Step 6: Final validation and cleanup
            article = self.validator.validate_and_clean(article, filing_info)
            logger.debug("Article validation completed")

            # Step 7: Convert to dictionary for output
            result = article.to_dict()
            
            logger.info(f"Successfully generated article for {article.holder_name}")
            return result

        except Exception as e:
            logger.error(f"Error generating article: {e}", exc_info=True)
            # Return a minimal valid article structure on error
            return self._create_fallback_article(pdf_url, sector, sub_sector, holder_type, uid)

    def _create_fallback_article(
        self,
        pdf_url: str,
        sector: str,
        sub_sector: str,
        holder_type: str,
        uid: Optional[str]
    ) -> Dict[str, Any]:
        """
        Create a minimal fallback article when generation fails.

        Args:
            pdf_url (str): URL of the source PDF
            sector (str): Company sector
            sub_sector (str): Company sub-sector
            holder_type (str): Type of holder
            uid (Optional[str]): Unique identifier

        Returns:
            Dict[str, Any]: Minimal article structure
        """
        fallback_article = Article.create_initial_structure(
            pdf_url=pdf_url,
            sector=sector,
            sub_sector=sub_sector,
            holder_type=holder_type,
            uid=uid
        )
        
        fallback_article.title = "Filing Processing Error"
        fallback_article.body = "Unable to process filing data. Please check the source document."
        
        return fallback_article.to_dict()


# Backward compatibility functions
def generate_article_filings(
    pdf_url: str, 
    sub_sector: str, 
    holder_type: str, 
    data: str, 
    uid: Optional[str] = None
) -> Dict[str, Any]:
    """
    Backward-compatible wrapper function.
    
    Maintains compatibility with existing code while using the new modular structure.
    """
    generator = ArticleGenerator()
    return generator.generate_article(
        pdf_url=pdf_url,
        sector="",  # Legacy calls didn't include sector
        sub_sector=sub_sector,
        holder_type=holder_type,
        filing_text=data,
        uid=uid
    )