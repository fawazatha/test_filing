"""
Script to classify the tags, subsector, tickers, and sentiment of the news article
"""

import dotenv
import json
import os
import string
import asyncio
from datetime import datetime
from typing import List, Dict, Optional, Union, Tuple

import nltk
from nltk.tokenize import word_tokenize
from nltk.corpus import stopwords
from nltk.stem import WordNetLemmatizer
from supabase import create_client, Client
from langchain.prompts import ChatPromptTemplate
from langchain_core.output_parsers import JsonOutputParser
from pydantic import BaseModel, Field

from model.llm_collection import LLMCollection

dotenv.load_dotenv()


class NewsClassifier:
    """
    A class to handle news article classification including tags, subsectors, tickers, and sentiment.
    """

    def __init__(self):
        """Initialize the NewsClassifier with required dependencies."""
        # NLTK setup
        nltk.data.path.append("./nltk_data")

        # Supabase setup
        self.supabase: Client = create_client(
            os.getenv("SUPABASE_URL", ""), os.getenv("SUPABASE_KEY", "")
        )

        # LLM setup
        self.llm_collection = LLMCollection()

        # Cache for loaded data
        self._subsectors_cache: Optional[Dict[str, str]] = None
        self._tags_cache: Optional[List[str]] = None
        self._company_cache: Optional[Dict[str, Dict[str, str]]] = None
        self._prompts_cache: Optional[Dict] = None

    def _load_prompts(self) -> Dict:
        """
        Load prompts from JSON config file.

        Returns:
            Dict: Dictionary containing all prompts
        """
        if self._prompts_cache is not None:
            return self._prompts_cache

        with open("./config/prompts.json", "r") as f:
            prompts = json.load(f)

        self._prompts_cache = prompts
        return prompts

    def _get_prompt(self, category: str, **kwargs) -> str:
        """
        Get formatted prompt for a specific category.

        Args:
            category (str): Category of the prompt (tags, tickers, subsectors, sentiment, dimension)
            **kwargs: Format parameters for the prompt template

        Returns:
            str: Formatted prompt
        """
        prompts = self._load_prompts()
        if category not in prompts:
            raise ValueError(f"Unknown prompt category: {category}")

        prompt_data = prompts[category]
        template = prompt_data["template"]
        return template.format(**kwargs)

    def _load_subsector_data(self) -> Dict[str, str]:
        """
        Load subsector data from Supabase or cache.

        Returns:
            Dict[str, str]: Dictionary mapping subsector slugs to descriptions
        """
        if self._subsectors_cache is not None:
            return self._subsectors_cache

        if datetime.today().day in [1, 15]:
            response = (
                self.supabase.table("idx_subsector_metadata")
                .select("slug, description")
                .execute()
            )

            subsectors = {row["slug"]: row["description"] for row in response.data}

            with open("./data/subsectors_data.json", "w") as f:
                json.dump(subsectors, f)
        else:
            with open("./data/subsectors_data.json", "r") as f:
                subsectors = json.load(f)

        self._subsectors_cache = subsectors
        return subsectors

    def _load_tag_data(self) -> List[str]:
        """
        Load tag data from JSON file.

        Returns:
            List[str]: List of available tags
        """
        if self._tags_cache is not None:
            return self._tags_cache

        with open("./data/unique_tags.json", "r") as f:
            tags = json.load(f)

        self._tags_cache = tags
        return tags

    def _load_company_data(self) -> Dict[str, Dict[str, str]]:
        """
        Load company data from Supabase or cache.

        Returns:
            Dict[str, Dict[str, str]]: Dictionary mapping company symbols to their details
        """
        if self._company_cache is not None:
            return self._company_cache

        if datetime.today().day in [1, 15]:
            response = (
                self.supabase.table("idx_company_profile")
                .select("symbol, company_name, sub_sector_id")
                .execute()
            )

            subsector_response = (
                self.supabase.table("idx_subsector_metadata")
                .select("sub_sector_id, sub_sector")
                .execute()
            )

            subsector_data = {
                row["sub_sector_id"]: row["sub_sector"]
                for row in subsector_response.data
            }

            company = {}
            for row in response.data:
                company[row["symbol"]] = {
                    "symbol": row["symbol"],
                    "name": row["company_name"],
                    "sub_sector": subsector_data[row["sub_sector_id"]],
                }

            for attr in company:
                company[attr]["sub_sector"] = (
                    company[attr]["sub_sector"]
                    .replace("&", "")
                    .replace(",", "")
                    .replace("  ", " ")
                    .replace(" ", "-")
                    .lower()
                )

            with open("./data/companies.json", "w") as f:
                json.dump(company, f, indent=2)
        else:
            with open("./data/companies.json", "r") as f:
                company = json.load(f)

        self._company_cache = company
        return company

    def _preprocess_text(self, text: str) -> str:
        """
        Preprocess text by tokenizing, removing stopwords, and lemmatizing.

        Args:
            text (str): Input text to preprocess

        Returns:
            str: Preprocessed text
        """
        tokens = word_tokenize(text.lower())
        table = str.maketrans("", "", string.punctuation)
        tokens = [word.translate(table) for word in tokens]
        tokens = [word for word in tokens if word.isalpha()]
        stop_words = set(stopwords.words("english"))
        tokens = [word for word in tokens if word not in stop_words]
        lemmatizer = WordNetLemmatizer()
        tokens = [lemmatizer.lemmatize(word) for word in tokens]
        return " ".join(tokens)

    def _classify_llama(
        self, body: str, category: str, title: str = ""
    ) -> Union[List[str], str]:
        """
        Synchronous wrapper for _classify_llama_async.

        Args:
            body (str): Text to classify
            category (str): Category to classify into
            title (str): Article title (required for dimension category)

        Returns:
            Union[List[str], str]: Classification results
        """
        import asyncio

        try:
            # Check if there's already a running event loop
            loop = asyncio.get_running_loop()
            # If we're in an async context, create a new thread to run the async code
            import concurrent.futures
            import threading

            def run_async():
                new_loop = asyncio.new_event_loop()
                asyncio.set_event_loop(new_loop)
                try:
                    return new_loop.run_until_complete(
                        self._classify_llama_async(body, category, title)
                    )
                finally:
                    new_loop.close()

            with concurrent.futures.ThreadPoolExecutor() as executor:
                future = executor.submit(run_async)
                return future.result()

        except RuntimeError:
            # No event loop running, we can create one
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            try:
                return loop.run_until_complete(
                    self._classify_llama_async(body, category, title)
                )
            finally:
                loop.close()

    async def _classify_llama_async(
        self, body: str, category: str, title: str = ""
    ) -> Union[List[str], str]:
        """
        Asynchronously classify text using LLM based on the specified category.

        Args:
            body (str): Text to classify
            category (str): Category to classify into (tags, tickers, subsectors, sentiment, dimension)
            title (str): Article title (required for dimension category)

        Returns:
            Union[List[str], str]: Classification results
        """
        # Load required data
        tags = self._load_tag_data()
        company = self._load_company_data()
        subsectors = self._load_subsector_data()

        # Prepare prompt parameters based on category
        if category == "dimension":
            prompt_params = {
                "title": title,
                "article": body,
            }
        else:
            prompt_params = {
                "tags": ", ".join(tags),
                "tickers": ", ".join(company.keys()),
                "subsectors": ", ".join(subsectors.keys()),
                "body": body,
            }

        # Get formatted prompt
        prompt = self._get_prompt(category, **prompt_params)

        # Process with LLM
        for llm in self.llm_collection.get_llms():
            try:
                outputs = await llm.ainvoke(prompt)
                outputs = outputs.content

                # Remove think tags and their content
                if "<think>" in outputs:
                    # Find the last occurrence of </think>
                    last_think_end = outputs.rfind("</think>")
                    if last_think_end != -1:
                        # Remove everything from <think> to </think>
                        outputs = outputs[last_think_end + 9 :].strip()
                    else:
                        # If no closing tag, remove from <think> to end
                        outputs = outputs[outputs.find("<think>") + 7 :].strip()

                # Clean up any remaining whitespace and newlines
                outputs = outputs.strip()

                # Handle dimension category differently (returns dict, not list)
                if category == "dimension":
                    result = {
                        "valuation": None,
                        "future": None,
                        "technical": None,
                        "financials": None,
                        "dividend": None,
                        "management": None,
                        "ownership": None,
                        "sustainability": None,
                    }

                    for line in outputs.splitlines():
                        if ":" in line:
                            parts = line.split(":")
                            if len(parts) >= 2:
                                key = parts[0].strip()
                                try:
                                    value = int(parts[1].strip())
                                    if key in result:
                                        result[key] = value
                                except ValueError:
                                    pass
                    return result
                else:
                    # Split by comma and clean up each item
                    outputs = str(outputs).split(",")
                    outputs = [out.strip() for out in outputs if out.strip()]

                    if category == "tags":
                        seen = set()
                        outputs = [
                            e
                            for e in outputs
                            if e in tags and not (e in seen or seen.add(e))
                        ]

                    return outputs
            except Exception as e:
                print(f"[ERROR] LLM failed with error: {e}")

        # Return appropriate default based on category
        if category == "dimension":
            return {
                "valuation": None,
                "future": None,
                "technical": None,
                "financials": None,
                "dividend": None,
                "management": None,
                "ownership": None,
                "sustainability": None,
            }
        else:
            return []

    async def classify_article_async(
        self, title: str, body: str
    ) -> Tuple[List[str], List[str], str, str, Dict[str, Optional[int]]]:
        """
        Asynchronously classify an article's tags, tickers, subsector, sentiment, and dimensions.

        Args:
            title (str): Article title
            body (str): Article content

        Returns:
            Tuple[List[str], List[str], str, str, Dict[str, Optional[int]]]:
                (tags, tickers, subsector, sentiment, dimensions)
        """
        # Run all classifications concurrently
        tasks = [
            self._classify_llama_async(body, "tags", title),
            self._classify_llama_async(body, "tickers", title),
            self._classify_llama_async(body, "subsectors", title),
            self._classify_llama_async(body, "sentiment", title),
            self._classify_llama_async(body, "dimension", title),
        ]

        results = await asyncio.gather(*tasks)
        tags, tickers, subsector, sentiment, dimension = results

        return tags, tickers, subsector, sentiment, dimension

    def get_tickers(self, text: str) -> List[str]:
        """
        Extract tickers from text.

        Args:
            text (str): Input text

        Returns:
            List[str]: List of identified tickers
        """
        company_names = self._identify_company_names(text)
        company = self._load_company_data()
        return self._match_ticker_codes(company_names, company)

    def get_tags(self, text: str, preprocess: bool = True) -> List[str]:
        """
        Extract tags from text.

        Args:
            text (str): Input text
            preprocess (bool): Whether to preprocess the text

        Returns:
            List[str]: List of identified tags
        """
        if preprocess:
            text = self._preprocess_text(text)
        return self._classify_llama(text, "tags")

    def get_subsector(self, text: str) -> str:
        """
        Extract subsector from text.

        Args:
            text (str): Input text

        Returns:
            str: Identified subsector
        """
        text = self._preprocess_text(text)
        return self._classify_llama(text, "subsectors")

    def get_sentiment(self, text: str) -> str:
        """
        Extract sentiment from text.

        Args:
            text (str): Input text

        Returns:
            str: Identified sentiment
        """
        text = self._preprocess_text(text)
        return self._classify_llama(text, "sentiment")

    def _identify_company_names(self, body: str) -> List[str]:
        """
        Identify company names in text.

        Args:
            body (str): Input text

        Returns:
            List[str]: List of identified company names
        """

        class CompanyNamesOutput(BaseModel):
            company_names: List[str] = Field(
                description="List of company names extracted from the article"
            )

        parser = JsonOutputParser(pydantic_object=CompanyNamesOutput)
        template = """
        ### **Company Name Extraction**
        Identify all company names that are explicitly mentioned in the article.

        ### **Extraction Rules:**
        - Extract full company names without abbreviations.
        - If a company name includes **"PT."**, omit **"PT."** and return only the full company name.
        - If a company name includes **"Tbk"**, omit **"Tbk"** and return only the full company name.
        - Example: **PT. Antara Business Service Tbk (ABS)** → `"Antara Business Service"`
        - If no company names are found, return an empty list.

        ### **Response Format:**
        {format_instructions}

        ---
        #### **Article Content:**
        {body}
        """

        prompt = ChatPromptTemplate.from_template(template=template)
        messages = prompt.format_messages(
            body=body, format_instructions=parser.get_format_instructions()
        )

        for llm in self.llm_collection.get_llms():
            try:
                output = llm.invoke(messages[0].content)
                parsed_output = parser.parse(output.content)
                return parsed_output["company_names"]
            except Exception as e:
                print(f"[ERROR] LLM failed: {e}")
                continue

        return []

    def _match_ticker_codes(
        self, company_names: List[str], company_data: Dict[str, Dict[str, str]]
    ) -> List[str]:
        """
        Match company names to ticker codes.

        Args:
            company_names (List[str]): List of company names
            company_data (Dict[str, Dict[str, str]]): Company data dictionary

        Returns:
            List[str]: List of matched ticker codes
        """
        matched_tickers = []
        for name in company_names:
            if name:
                for ticker, info in company_data.items():
                    if (
                        name.lower() in info["name"].lower()
                        or name.lower() == ticker.split()[0].lower()
                    ):
                        if ticker not in matched_tickers:
                            matched_tickers.append(ticker)
        return matched_tickers

    def predict_dimension(self, title: str, article: str) -> Dict[str, Optional[int]]:
        """
        Predict dimensions of the article.

        Args:
            title (str): Article title
            article (str): Article content

        Returns:
            Dict[str, Optional[int]]: Dictionary of dimension predictions
        """
        prompt = self._get_prompt("dimension", title=title, article=article)

        result = {
            "valuation": None,
            "future": None,
            "technical": None,
            "financials": None,
            "dividend": None,
            "management": None,
            "ownership": None,
            "sustainability": None,
        }

        for llm in self.llm_collection.get_llms():
            try:
                outputs = llm.invoke(prompt).content
                for line in outputs.splitlines():
                    item = line.split(":")
                    key = item[0].strip()
                    try:
                        value = int(item[1])
                        if key in result:
                            result[key] = value
                    except:
                        pass
                return result
            except Exception as e:
                print(f"[ERROR] LLM failed with error: {e}")
        return result


# Create a singleton instance
classifier = NewsClassifier()


# Public interface functions
def get_tickers(text: str) -> List[str]:
    """Get tickers from text."""
    return classifier.get_tickers(text)


def get_tags_chat(text: str, preprocess: bool = True) -> List[str]:
    """Get tags from text."""
    return classifier.get_tags(text, preprocess)


def get_subsector_chat(text: str) -> str:
    """Get subsector from text."""
    return classifier.get_subsector(text)


def get_sentiment_chat(text: str) -> str:
    """Get sentiment from text."""
    return classifier.get_sentiment(text)


def predict_dimension(title: str, article: str) -> Dict[str, Optional[int]]:
    """Predict dimensions of the article."""
    return classifier.predict_dimension(title, article)


# Backward compatibility functions
def load_company_data() -> Dict[str, Dict[str, str]]:
    """Load company data from Supabase or cache."""
    return classifier._load_company_data()


def load_subsector_data() -> Dict[str, str]:
    """Load subsector data from Supabase or cache."""
    return classifier._load_subsector_data()


def load_tag_data() -> List[str]:
    """Load tag data from JSON file."""
    return classifier._load_tag_data()