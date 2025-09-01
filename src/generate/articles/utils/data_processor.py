"""
Data processing utilities for article generation.

This module handles the processing and transformation of extracted filing data
into article format, including financial calculations and data normalization.
"""

import re
import logging
from typing import Dict, Any

from ..model.article import Article
from ..model.filing_info import FilingInfo
from ...utils.database_helper import get_company_info
from ...utils.price_calculator import PriceCalculator

logger = logging.getLogger(__name__)


class FilingDataProcessor:
    """
    Processes and transforms filing data into article format.
    
    This class handles the conversion of raw filing information into
    structured article data, including financial calculations and
    data normalization.
    """

    def __init__(self):
        """Initialize the data processor."""
        self.price_calculator = PriceCalculator()

    def populate_article_data(self, article: Article, filing_info: FilingInfo) -> Article:
        """
        Populate article with data from filing information.

        Args:
            article (Article): Article to populate
            filing_info (FilingInfo): Source filing information

        Returns:
            Article: Populated article

        Raises:
            Exception: If critical data processing fails
        """
        try:
            logger.debug("Starting article data population")

            # Set basic information
            article.holder_name = filing_info.holder_name
            article._purpose = filing_info.purpose

            # Process and set ticker information
            self._process_ticker_data(article, filing_info)

            # Process timestamp
            article.set_timestamp_from_string(filing_info.date_time)

            # Process financial data
            self._process_financial_data(article, filing_info)

            # Process transaction data
            self._process_transaction_data(article, filing_info)

            # Generate initial title and body
            self._generate_initial_content(article, filing_info)

            logger.debug("Article data population completed")
            return article

        except Exception as e:
            logger.error(f"Error populating article data: {e}")
            raise

    def update_sector_information(self, article: Article) -> Article:
        """
        Update sector information from external data sources.

        Args:
            article (Article): Article to update

        Returns:
            Article: Updated article
        """
        try:
            # Skip if we already have complete sector information
            if article.sector and article.sub_sector:
                return article

            # Get ticker without .JK suffix for lookup
            if not article.tickers:
                return article

            ticker_symbol = article.tickers[0].replace(".JK", "")
            company_info = get_company_info(ticker_symbol)

            if company_info:
                if not article.sector:
                    article.sector = company_info.get("sector", "")
                if not article.sub_sector:
                    article.sub_sector = company_info.get("sub_sector", "")

                # Update company name in title if needed
                self._update_company_name_in_title(article, company_info)

            logger.debug(f"Updated sector info for {ticker_symbol}")
            return article

        except Exception as e:
            logger.error(f"Error updating sector information: {e}")
            return article

    def _process_ticker_data(self, article: Article, filing_info: FilingInfo) -> None:
        """Process and normalize ticker information."""
        if filing_info.ticker:
            article.add_ticker(filing_info.ticker)

    def _process_financial_data(self, article: Article, filing_info: FilingInfo) -> None:
        """Process financial data (holdings and percentages)."""
        # Process shareholdings
        article.holding_before = self._extract_integer(filing_info.shareholding_before)
        article.holding_after = self._extract_integer(filing_info.shareholding_after)

        # Process percentages
        article.share_percentage_before = self._normalize_percentage(
            filing_info.share_percentage_before
        )
        article.share_percentage_after = self._normalize_percentage(
            filing_info.share_percentage_after
        )
        article.share_percentage_transaction = self._normalize_percentage(
            filing_info.share_percentage_transaction
        )

        # Calculate transaction details
        article.transaction_type = article.determine_transaction_type()
        article.amount_transaction = article.calculate_amount_transaction()

    def _process_transaction_data(self, article: Article, filing_info: FilingInfo) -> None:
        """Process transaction price data."""
        if filing_info.price_transaction:
            article.price_transaction = {
                "prices": filing_info.price_transaction.prices,
                "amount_transacted": filing_info.price_transaction.amount_transacted
            }

            # Calculate weighted average price and total value
            try:
                price, transaction_value = self.price_calculator.calculate_weighted_price_and_value(
                    filing_info.price_transaction.prices,
                    filing_info.price_transaction.amount_transacted
                )
                article.price = price
                article.transaction_value = transaction_value
            except Exception as e:
                logger.warning(f"Error calculating price data: {e}")

    def _generate_initial_content(self, article: Article, filing_info: FilingInfo) -> None:
        """Generate initial title and body content."""
        # Generate initial title
        holder = filing_info.holder_name or "insider"
        company = filing_info.company_name or ""
        article.title = f"Insider trading information for {holder} in {company}".strip()

        # Generate initial body
        body_parts = []
        
        if filing_info.document_number:
            body_parts.append(f"Document: {filing_info.document_number}")
        
        if filing_info.date_time:
            body_parts.append(f"Date: {filing_info.date_time}")
        
        if filing_info.category:
            body_parts.append(f"Category: {filing_info.category}")
        
        if holder and company:
            body_parts.append(f"Transaction by {holder} in {company}")
        
        if filing_info.shareholding_before and filing_info.shareholding_after:
            body_parts.append(
                f"Holdings changed from {filing_info.shareholding_before} "
                f"to {filing_info.shareholding_after}"
            )

        article.body = " - ".join(body_parts)

    def _extract_integer(self, value: str) -> int:
        """Extract integer from string, removing formatting."""
        if not value:
            return 0
        
        # Remove dots and commas used as thousands separators
        cleaned = re.sub(r"[^\d]", "", value)
        return int(cleaned) if cleaned.isdigit() else 0

    def _normalize_percentage(self, value: str) -> float:
        """Normalize percentage string to float."""
        if not value:
            return 0.0
        
        # Remove % sign and normalize decimal separator
        cleaned = value.replace("%", "").replace(",", ".")
        try:
            return float(cleaned)
        except ValueError:
            return 0.0

    def _update_company_name_in_title(self, article: Article, company_info: Dict[str, Any]) -> None:
        """Update company name in title if it's missing or incomplete."""
        company_name = company_info.get("company_name", "")
        if not company_name:
            return

        # Check if title has incomplete company name pattern
        if " in " in article.title and " in  " in article.title:
            article.title = article.title.replace(" in ", f" in {company_name}")
        elif " in " in article.title and not company_name.lower() in article.title.lower():
            # Replace generic company reference with actual name
            parts = article.title.split(" in ")
            if len(parts) == 2:
                article.title = f"{parts[0]} in {company_name}"