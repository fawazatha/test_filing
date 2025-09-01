"""
Script to summarize a filing into an article.

Provides comprehensive summarization of financial filings with:
- Structured data extraction and processing
- LLM-based title and body generation
- Token counting and optimization
- Error handling and validation
"""

import json
import logging
import os
from typing import Dict, Any, Tuple, Optional

import dotenv
import tiktoken
from langchain.chat_models import init_chat_model
from langchain_core.output_parsers import JsonOutputParser
from langchain_core.prompts import ChatPromptTemplate
from pydantic import BaseModel, Field

# Load environment variables
dotenv.load_dotenv()

# Configure logging
logger = logging.getLogger(__name__)


class FilingsOutput(BaseModel):
    """Model for filing summarization output."""

    title: str = Field(description="Concise title for the filing summary")
    body: str = Field(description="Detailed body text for the filing summary")


class FilingSummarizer:
    """Enhanced filing summarizer with robust error handling and optimization."""

    def __init__(
        self, model_name: str = "llama3-70b-8192", model_provider: str = "groq"
    ):
        """
        Initialize filing summarizer.

        Args:
            model_name (str): Name of the LLM model to use
            model_provider (str): Provider of the LLM model
        """
        try:
            self.llm = init_chat_model(model_name, model_provider=model_provider)
            logger.info(f"Initialized LLM: {model_name} from {model_provider}")
        except Exception as e:
            logger.error(f"Failed to initialize LLM: {e}")
            raise

        self.parser = JsonOutputParser(pydantic_object=FilingsOutput)
        self._prompt_template = self._create_prompt_template()

    def _create_prompt_template(self) -> ChatPromptTemplate:
        """Create the prompt template for filing summarization."""
        return ChatPromptTemplate.from_messages(
            [
                (
                    "system",
                    """You are a financial analyst. Provide direct, concise summaries without any additional commentary or prefixes. 
            Output must be in JSON format with 'title' and 'body' fields.
            Focus on accuracy, clarity, and relevance to Indonesian stock market investors.""",
                ),
                (
                    "user",
                    """Analyze this filing transaction and provide:
            1. A title following this structure: (Shareholder name) (Transaction type) Transaction of (Company) (buyer/seller if applicable)
            2. A one-paragraph summary (max 150 tokens) focusing on: entities involved, transaction type, ownership changes, purpose, and significance

            Filing: {text}
            {format_instructions}""",
                ),
            ]
        )

    def count_tokens(self, text: str) -> int:
        """
        Count tokens in text for optimization purposes.

        Args:
            text (str): Text to count tokens for

        Returns:
            int: Number of tokens
        """
        try:
            enc = tiktoken.encoding_for_model("gpt-3.5-turbo")
            tokens = enc.encode(text)
            return len(tokens)
        except Exception as e:
            logger.warning(f"Error counting tokens: {e}")
            # Fallback: rough estimation (1 token ≈ 4 characters)
            return len(text) // 4

    def summarize_llama(self, filings_text: str) -> Dict[str, str]:
        """
        Summarize filing text using LLM.

        Args:
            filings_text (str): Raw filing text to summarize

        Returns:
            Dict[str, str]: Dictionary containing 'title' and 'body'
        """
        if not filings_text or not filings_text.strip():
            logger.warning("Empty filing text provided")
            return {"title": "", "body": ""}

        try:
            logger.info(
                f"Summarizing filing ({self.count_tokens(filings_text)} tokens)"
            )

            chain = self._prompt_template | self.llm | self.parser

            response = chain.invoke(
                {
                    "text": filings_text,
                    "format_instructions": self.parser.get_format_instructions(),
                }
            )

            # Validate response
            if not isinstance(response, dict):
                logger.error(f"Invalid response type: {type(response)}")
                return {"title": "", "body": ""}

            title = response.get("title", "").strip()
            body = response.get("body", "").strip()

            if not title or not body:
                logger.warning("Incomplete response from LLM")
                return {
                    "title": title or "Filing Summary",
                    "body": body or "Filing transaction processed.",
                }

            logger.info("Successfully generated filing summary")
            return {"title": title, "body": body}

        except Exception as e:
            logger.error(f"Error summarizing filing: {e}")
            return {
                "title": "Filing Summary",
                "body": "Error processing filing transaction.",
            }

    def summarize_filing(self, data: Dict[str, Any]) -> Tuple[str, str]:
        """
        Generate summary from structured filing data.

        Args:
            data (Dict[str, Any]): Structured filing data

        Returns:
            Tuple[str, str]: (title, body) summary
        """
        if not data or not isinstance(data, dict):
            logger.error("Invalid filing data provided")
            return "", ""

        try:
            # Extract and structure relevant data
            news_text = self._extract_filing_data(data)
            news_text_json = json.dumps(news_text, indent=2)

            logger.debug(
                f"Structured filing data: {self.count_tokens(news_text_json)} tokens"
            )

            # Generate summary
            response = self.summarize_llama(news_text_json)

            return response["title"], response["body"]

        except Exception as e:
            logger.error(f"Error in summarize_filing: {e}")
            return "", ""

    def _extract_filing_data(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Extract relevant data from filing for summarization.

        Args:
            data (Dict[str, Any]): Raw filing data

        Returns:
            Dict[str, Any]: Structured data for summarization
        """
        return {
            "amount_transaction": data.get("amount_transaction", 0),
            "holder_type": data.get("holder_type", ""),
            "holding_after": data.get("holding_after", 0),
            "holding_before": data.get("holding_before", 0),
            "sector": data.get("sector", ""),
            "sub_sector": data.get("sub_sector", ""),
            "timestamp": data.get("timestamp", ""),
            "title": data.get("title", ""),
            "transaction_type": data.get("transaction_type", ""),
            "purpose": data.get("purpose", ""),
            "transactions": data.get("price_transaction", {}),
            "ticker": data.get("tickers", [""])[0] if data.get("tickers") else "",
            "company_name": self._extract_company_name(data.get("title", "")),
            "holder_name": data.get("holder_name", ""),
        }

    def _extract_company_name(self, title: str) -> str:
        """
        Extract company name from title.

        Args:
            title (str): Title containing company information

        Returns:
            str: Extracted company name
        """
        if not title:
            return ""

        # Look for patterns like "PT [Company Name]" or company names in title
        if "PT " in title:
            parts = title.split("PT ")[1].split(" ")
            # Take first few words as company name
            return "PT " + " ".join(parts[:3])

        return title


# Global instance for backward compatibility
_summarizer = FilingSummarizer()


# Backward compatible functions
def count_tokens(text: str) -> int:
    """
    Count tokens in text.

    This function maintains backward compatibility with existing code.
    """
    return _summarizer.count_tokens(text)


def summarize_llama(filings_text: str) -> Dict[str, str]:
    """
    Summarize filing text using LLM.

    This function maintains backward compatibility with existing code.
    """
    return _summarizer.summarize_llama(filings_text)


def summarize_filing(data: Dict[str, Any]) -> Tuple[str, str]:
    """
    Generate summary from structured filing data.

    This function maintains backward compatibility with existing code.
    """
    return _summarizer.summarize_filing(data)