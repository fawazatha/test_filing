"""
Content generation utilities for article enhancement.

This module handles the generation of enhanced content (titles, summaries, tags)
using LLM-based processing and classification services.
"""

import logging
from typing import List

from ..model.article import Article
from ...classifier import get_sentiment_chat, get_tags_chat
from ...summary_filings import FilingSummarizer

logger = logging.getLogger(__name__)


class ContentGenerator:
    """
    Generates enhanced content for articles using LLM services.
    
    This class handles the generation of improved titles, bodies, and tags
    using external LLM services and classification tools.
    """

    def __init__(self):
        """Initialize the content generator."""
        self.filing_summarizer = FilingSummarizer()

    def enhance_article_content(self, article: Article) -> Article:
        """
        Enhance article content with improved title, body, and tags.

        Args:
            article (Article): Article to enhance

        Returns:
            Article: Enhanced article
        """
        try:
            logger.debug("Starting article content enhancement")

            # Generate enhanced title and body using LLM
            self._generate_enhanced_content(article)

            # Generate tags and sentiment
            if article.body:
                self._generate_tags_and_sentiment(article)

            logger.debug("Article content enhancement completed")
            return article

        except Exception as e:
            logger.error(f"Error enhancing article content: {e}")
            # Return article with original content if enhancement fails
            return article

    def _generate_enhanced_content(self, article: Article) -> None:
        """Generate enhanced title and body using filing summarizer."""
        try:
            # Convert article to filing data format for summarizer
            filing_data = self._prepare_filing_data_for_summarizer(article)
            
            # Generate enhanced content
            enhanced_title, enhanced_body = self.filing_summarizer.summarize_filing(filing_data)
            
            # Update article if we got valid content
            if enhanced_title and enhanced_title.strip():
                article.title = enhanced_title.strip()
                
            if enhanced_body and enhanced_body.strip():
                article.body = enhanced_body.strip()

        except Exception as e:
            logger.warning(f"Failed to generate enhanced content: {e}")
            # Keep original content if enhancement fails

    def _generate_tags_and_sentiment(self, article: Article) -> None:
        """Generate tags and sentiment classification for the article."""
        try:
            # Get tags from content
            tags = get_tags_chat(article.body, preprocess=True) or []
            
            # Get sentiment
            sentiment = get_sentiment_chat(article.body) or []
            
            # Combine tags and sentiment
            all_tags = list(article.tags)  # Start with existing tags
            
            # Add new tags
            for tag in tags:
                if tag and tag not in all_tags:
                    all_tags.append(tag)
            
            # Add sentiment as tag
            if sentiment and isinstance(sentiment, list) and sentiment[0]:
                sentiment_tag = sentiment[0]
                if sentiment_tag not in all_tags:
                    all_tags.append(sentiment_tag)
            
            # Ensure insider-trading tag is always present
            if "insider-trading" not in all_tags:
                all_tags.append("insider-trading")
            
            # Update article tags
            article.tags = self._clean_and_deduplicate_tags(all_tags)

        except Exception as e:
            logger.warning(f"Failed to generate tags and sentiment: {e}")
            # Ensure minimum tags are present
            if "insider-trading" not in article.tags:
                article.tags.append("insider-trading")

    def _prepare_filing_data_for_summarizer(self, article: Article) -> dict:
        """
        Prepare article data in format expected by filing summarizer.

        Args:
            article (Article): Source article

        Returns:
            dict: Data formatted for summarizer
        """
        return {
            "amount_transaction": article.amount_transaction,
            "holder_type": article.holder_type,
            "holding_after": article.holding_after,
            "holding_before": article.holding_before,
            "sector": article.sector,
            "sub_sector": article.sub_sector,
            "timestamp": article.timestamp,
            "title": article.title,
            "transaction_type": article.transaction_type,
            "purpose": getattr(article, '_purpose', ''),
            "price_transaction": article.price_transaction,
            "tickers": article.tickers,
            "holder_name": article.holder_name,
        }

    def _clean_and_deduplicate_tags(self, tags: List[str]) -> List[str]:
        """
        Clean and deduplicate tags list while preserving order.

        Args:
            tags (List[str]): Raw tags list

        Returns:
            List[str]: Cleaned and deduplicated tags
        """
        seen = set()
        cleaned_tags = []
        
        for tag in tags:
            if tag and isinstance(tag, str):
                tag = tag.strip()
                if tag and tag not in seen:
                    seen.add(tag)
                    cleaned_tags.append(tag)
        
        return cleaned_tags