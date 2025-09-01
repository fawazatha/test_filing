"""
Article generation package.

This package provides modular, maintainable tools for generating articles
from financial filing data.
"""

from .core.generator import ArticleGenerator, generate_article_filings
from .model.article import Article
from .model.filing_info import FilingInfo
from .utils.data_extractor import FilingDataExtractor
from .utils.data_processor import FilingDataProcessor
from .utils.content_generator import ContentGenerator
from .utils.validator import ArticleValidator

__version__ = "1.0.0"
__all__ = [
    "ArticleGenerator",
    "Article", 
    "FilingInfo",
    "FilingDataExtractor",
    "FilingDataProcessor", 
    "ContentGenerator",
    "ArticleValidator",
    "generate_article_filings",  # Backward compatibility
]