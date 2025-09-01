"""
Article validation and cleanup utilities.

This module handles the final validation and cleanup of generated articles,
ensuring they meet quality standards and have all required fields.
"""

import logging
from typing import Dict, Any

from ..model.article import Article
from ..model.filing_info import FilingInfo
from ....utils.database_helper import get_company_info

logger = logging.getLogger(__name__)


class ArticleValidator:
    """
    Validates and cleans up generated articles.
    
    This class performs final validation and cleanup of articles to ensure
    they meet quality standards and have all required information.
    """

    def __init__(self):
        """Initialize the article validator."""
        pass

    def validate_and_clean(self, article: Article, filing_info: FilingInfo) -> Article:
        """
        Perform final validation and cleanup of the article.

        Args:
            article (Article): Article to validate
            filing_info (FilingInfo): Original filing information for fallback

        Returns:
            Article: Validated and cleaned article
        """
        try:
            logger.debug("Starting article validation and cleanup")

            # Ensure basic article structure is valid
            self._ensure_basic_structure(article, filing_info)

            # Validate and fix ticker information
            self._validate_ticker_information(article, filing_info)

            # Validate and fix title
            self._validate_and_fix_title(article, filing_info)

            # Validate and fix body content
            self._validate_and_fix_body(article, filing_info)

            # Ensure required tags are present
            self._ensure_required_tags(article)

            # Clean up any remaining issues
            self._final_cleanup(article)

            logger.debug("Article validation completed successfully")
            return article

        except Exception as e:
            logger.error(f"Error during article validation: {e}")
            return self._apply_emergency_fixes(article, filing_info)

    def _ensure_basic_structure(self, article: Article, filing_info: FilingInfo) -> None:
        """Ensure article has basic required structure."""
        # Ensure holder name is set
        if not article.holder_name and filing_info.holder_name:
            article.holder_name = filing_info.holder_name

        # Ensure we have at least a minimal timestamp
        if not article.timestamp:
            article.timestamp = filing_info.date_time or ""

    def _validate_ticker_information(self, article: Article, filing_info: FilingInfo) -> None:
        """Validate and fix ticker information."""
        # Add ticker from filing info if missing
        if not article.tickers and filing_info.ticker:
            article.add_ticker(filing_info.ticker)

        # Ensure proper .JK suffix
        for i, ticker in enumerate(article.tickers):
            if ticker and not ticker.endswith('.JK'):
                article.tickers[i] = ticker + '.JK'

    def _validate_and_fix_title(self, article: Article, filing_info: FilingInfo) -> None:
        """Validate and fix article title."""
        title_needs_fix = (
            not article.title or
            article.title.strip() == "" or
            " for  in " in article.title or
            " in  " in article.title
        )

        if title_needs_fix:
            article.title = self._generate_fallback_title(article, filing_info)

    def _validate_and_fix_body(self, article: Article, filing_info: FilingInfo) -> None:
        """Validate and fix article body content."""
        body_needs_fix = (
            not article.body or
            article.body.strip() == "" or
            "No information available" in article.body or
            len(article.body.strip()) < 20
        )

        if body_needs_fix:
            article.body = self._generate_fallback_body(article, filing_info)

    def _ensure_required_tags(self, article: Article) -> None:
        """Ensure required tags are present."""
        if not article.tags:
            article.tags = ["insider-trading"]
        elif "insider-trading" not in article.tags:
            article.tags.append("insider-trading")

        # Remove duplicates while preserving order
        seen = set()
        unique_tags = []
        for tag in article.tags:
            if tag and tag not in seen:
                seen.add(tag)
                unique_tags.append(tag)
        article.tags = unique_tags

    def _final_cleanup(self, article: Article) -> None:
        """Perform final cleanup operations."""
        # Clean up title
        article.title = article.title.replace("  ", " ").strip()
        
        # Clean up body
        article.body = article.body.replace("  ", " ").strip()
        
        # Ensure body ends with period if it's a complete sentence
        if article.body and not article.body.endswith(('.', '!', '?')):
            article.body += "."

    def _generate_fallback_title(self, article: Article, filing_info: FilingInfo) -> str:
        """Generate fallback title when original is invalid."""
        holder = (
            article.holder_name or 
            filing_info.holder_name or 
            "Insider"
        )
        
        # Get company name from various sources
        company_name = self._get_best_company_name(article, filing_info)
        
        # Get ticker for suffix
        ticker = article.tickers[0] if article.tickers else ""
        ticker_suffix = f" ({ticker})" if ticker and company_name and ticker not in company_name else ""
        
        return f"Insider trading information for {holder} in {company_name}{ticker_suffix}".strip()

    def _generate_fallback_body(self, article: Article, filing_info: FilingInfo) -> str:
        """Generate fallback body when original is invalid."""
        holder = (
            article.holder_name or 
            filing_info.holder_name or 
            "insider"
        )
        
        company_name = self._get_best_company_name(article, filing_info)
        
        # Determine transaction verb
        transaction_type = article.transaction_type or self._infer_transaction_type(article)
        verb = {
            "buy": "bought",
            "sell": "sold"
        }.get(transaction_type, "transacted")
        
        # Build body components
        body_parts = [f"{holder} {verb}"]
        
        # Add amount if available
        if article.amount_transaction > 0:
            body_parts.append(f"{article.amount_transaction:,} shares")
        else:
            body_parts.append("shares")
        
        # Add company
        if company_name:
            body_parts.append(f"of {company_name}")
        
        # Add price if available
        if article.price > 0:
            body_parts.append(f"at IDR {article.price:,.0f} per share")
        
        # Add timestamp if available
        if article.timestamp:
            body_parts.append(f"on {article.timestamp}")
        
        # Add holding change if available
        if article.holding_before > 0 or article.holding_after > 0:
            body_parts.append(
                f", changing holding from {article.holding_before:,} "
                f"to {article.holding_after:,} shares"
            )
        
        return " ".join(body_parts) + "."

    def _get_best_company_name(self, article: Article, filing_info: FilingInfo) -> str:
        """Get the best available company name from various sources."""
        # Try filing info first
        if filing_info.company_name:
            return filing_info.company_name.strip()
        
        # Try database lookup if we have ticker
        if article.tickers:
            ticker_symbol = article.tickers[0].replace(".JK", "")
            company_info = get_company_info(ticker_symbol)
            if company_info and company_info.get("company_name"):
                return company_info["company_name"]
        
        # Fallback to ticker
        return article.tickers[0] if article.tickers else "Unknown Company"

    def _infer_transaction_type(self, article: Article) -> str:
        """Infer transaction type from available data."""
        # Try percentage comparison
        if article.share_percentage_after > article.share_percentage_before:
            return "buy"
        elif article.share_percentage_after < article.share_percentage_before:
            return "sell"
        
        # Try holding comparison
        if article.holding_after > article.holding_before:
            return "buy"
        elif article.holding_after < article.holding_before:
            return "sell"
        
        return "transacted"

    def _apply_emergency_fixes(self, article: Article, filing_info: FilingInfo) -> Article:
        """Apply emergency fixes when validation fails."""
        logger.warning("Applying emergency fixes to article")
        
        # Ensure absolute minimum content
        if not article.title:
            article.title = "Filing Processing Information"
        
        if not article.body:
            article.body = "Filing transaction processed successfully."
        
        if not article.tags:
            article.tags = ["insider-trading"]
        
        return article