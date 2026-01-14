"""
Script to generate an article from PDF reader for filings.
Handles extraction and processing of insider trading documents.
"""

import json
import re
from datetime import datetime
from typing import Dict, Optional, Any
import logging

# from model.price_transaction import PriceTransaction
# from scripts.classifier import get_sentiment_chat, get_tags_chat
# from scripts.summary_filings import summarize_filing, summarize_filing_manual
# from handlers.support import clean_company_name
from deep_translator import GoogleTranslator


# Configure logging
logger = logging.getLogger(__name__)

import json
import pandas as pd 


class PriceTransaction:
  def __init__(self, amount_transacted: list[float], prices: list[float], transaction_type: list[str]):
    """
    Initializes the Transaction object.

    @param amount: Amount of the transaction.
    @param price: Price of the transaction.
    """
    self.amount_transacted = amount_transacted
    self.prices = prices
    self.transaction_type = transaction_type

  def calculate_two_transaction_type(self) -> dict:
    """
    Calculates the net transaction details from a dictionary of price transactions.

    @return: Dictionary containing the weighted average price, total transaction value, and overall transaction type.
    """
    df = pd.DataFrame({
        'amount_transacted': self.amount_transacted,
        'prices': self.prices,
        'type': self.transaction_type
    }) 
   
    df['value'] = df['prices'] * df['amount_transacted']

    # Calculate total shares and value for all 'buy' transactions
    total_buy_shares = df[df['type'] == 'buy']['amount_transacted'].sum()
    total_buy_value = df[df['type'] == 'buy']['value'].sum()

    # Calculate total shares and value for all 'sell' transactions
    total_sell_shares = df[df['type'] == 'sell']['amount_transacted'].sum()
    total_sell_value = df[df['type'] == 'sell']['value'].sum()

    # Calculate total shares and value for 'others' transactions
    total_others_shares = df[~df['type'].isin(['buy', 'sell'])]['amount_transacted'].sum()
    total_others_value = df[~df['type'].isin(['buy', 'sell'])]['value'].sum()

    # Check if there are any buy or sell transactions
    has_buy_sell = (total_buy_shares > 0) or (total_sell_shares > 0)

    if has_buy_sell:
        # Calculate the net difference between buys and sells
        net_value = total_buy_value - total_sell_value
        net_shares = total_buy_shares - total_sell_shares

        if net_value > 0:
            transaction_type = 'buy'
        elif net_value < 0:
            transaction_type = 'sell'
        else: 
            transaction_type = 'others'
        
        # Calculate the weighted average price
        if net_shares != 0:
            weighted_average_price = abs(net_value / net_shares)
        else:
            weighted_average_price = 0.0

        result = {
            "price": round(weighted_average_price, 3),
            "transaction_value": abs(int(net_value)),
            "transaction_type": transaction_type
        }

    else:
        # Only "others" transactions exist
        if total_others_shares > 0:
            weighted_average_price = total_others_value / total_others_shares
        else:
            weighted_average_price = 0.0
        
        result = {
            "price": round(weighted_average_price, 3),
            "transaction_value": abs(int(total_others_value)),
            "transaction_type": "others"
        }

    return result.get('price'), result.get('transaction_value'), result.get('transaction_type')
    
  def get_price_transaction_value(self):
    """
    Calculates the price and transaction value of the transaction.
    
    @return: Tuple containing the price and transaction value.
    """
    sum_price_transaction = 0
    sum_transaction = 0  

    for index in range(len(self.prices)):
      sum_price_transaction += self.prices[index] * self.amount_transacted[index]
      sum_transaction += self.amount_transacted[index]

    self.price = sum_price_transaction / sum_transaction if sum_transaction != 0 else 0
    self.transaction_value = sum_price_transaction
    self.price = round(self.price, 3)
    return self.price, self.transaction_value
  
  def to_json(self):
    """
    Converts the Transaction object to a JSON string.

    @return: JSON string representation of the Transaction object.
    """
    return json.dumps(self.__dict__, indent=2)

  @classmethod
  def from_json(cls, json_str):
    """
    Creates a Transaction object from a JSON string.

    @param json_str: JSON string representation of a Transaction object.
    @return: Transaction object.
    """
    data = json.loads(json_str)
    return cls(**data)


MONTH_MAPPING = {
    "januari": "january",
    "februari": "february",
    "maret": "march",
    "april": "april",
    "mei": "may",
    "juni": "june",
    "juli": "july",
    "agustus": "august",
    "september": "september",
    "oktober": "october",
    "november": "november",
    "desember": "december",

    "jan": "january",
    "feb": "february",
    "mar": "march",
    "apr": "april",
    "mei": "may",
    "jun": "june",
    "jul": "july",
    "agu": "august",
    "sep": "september",
    "okt": "october",
    "nov": "november",
    "des": "december"
}

def summarize_filing_manual(
    holder_name: str,
    company_name: str,
    tx_type: str,
    amount: Optional[int],
    holding_before: Optional[int],
    holding_after: Optional[int],
    purpose_en: str,
) -> tuple[str, str]:
        """
        Human-friendly title/body with minimal grammar rules.
        """
        action_title = tx_type.replace("-", " ")
        if tx_type == "buy":
            action_verb = "bought"
            title = f"{holder_name} buys shares of {company_name}"
        elif tx_type == "sell":
            action_verb = "sold"
            title = f"{holder_name} sells shares of {company_name}"
        elif tx_type == "share-transfer":
            action_verb = "transferred"
            title = f"{holder_name} transfers shares of {company_name}"
        elif tx_type == "award":
            action_verb = "was awarded"
            title = f"{holder_name} was awarded shares of {company_name}"
        elif tx_type == "inheritance":
            action_verb = "inherited"
            title = f"{holder_name} inherits shares of {company_name}"
        elif tx_type == "others": 
            action_verb = "executed a transaction for"
            title = f"Change in {holder_name}'s position in {company_name}"
        else:
            action_verb = "executed a transaction for"
            title = f"{holder_name} {action_title} transaction of {company_name}"

        amount_str = f"{amount:,} shares" if amount is not None else "shares"
        body = f"{holder_name} {action_verb} {amount_str} of {company_name}."

        if holding_before is not None and holding_after is not None:
            hb_str, ha_str = f"{holding_before:,}", f"{holding_after:,}"
            if holding_after > holding_before:
                body += f" This increases their holdings from {hb_str} to {ha_str} shares."
            elif holding_after < holding_before:
                body += f" This decreases their holdings from {hb_str} to {ha_str} shares."
            else:
                body += f" Their holdings remain at {ha_str} shares."

        if purpose_en:
            body += f" The stated purpose of the transaction was {purpose_en.lower()}."
        return title, body

def clean_company_name(company_name: str) -> str:
    needs_cleaning = False

    upper_count = sum(1 for char in company_name if char.isupper())
    lower_count = sum(1 for char in company_name if char.islower())

    # Check if the string is mostly uppercase
    if upper_count > lower_count:
        needs_cleaning = True
    
    # Check if all words are capitalized
    words = company_name.split()
    if not needs_cleaning and not all(word[0].isupper() for word in words if word):
        needs_cleaning = True
    
    # Check if last letter of the last word capitalized
    if not needs_cleaning and words:
        last_word = words[-1]
        if last_word and last_word[-1].isalpha() and last_word[-1].isupper():
            needs_cleaning = True

    if needs_cleaning:
        cleaned_name = company_name.title()
        cleaned_name = re.sub(r'\bPt\.?\b', 'PT', cleaned_name)
        return cleaned_name.strip()
    else:
        return company_name
    
class FilingArticleGenerator:
    """Class to handle generation of articles from filing PDFs."""

    def __init__(self):
        """Initialize the generator with sector data."""
        try:
            with open("./data/sectors_data.json", "r") as f:
                self.sectors_data = json.load(f)
        except FileNotFoundError:
            logger.error("sectors_data.json not found")
            self.sectors_data = {}
        except json.JSONDecodeError:
            logger.error("Invalid JSON in sectors_data.json")
            self.sectors_data = {}

        self.kw_inherit = ["waris", "inheritance", "hibah", "grant", "bequest"]
        self.kw_mesop = ["mesop", "msop", "esop", "program opsi saham", "employee stock option"]
        self.kw_freefloat = ["free float", "free-float", "freefloat", "pemenuhan porsi publik"]
        self.kw_transfer = [
            "transfer", "pemindahan", "konversi", "conversion",
            "neutral", "tanpa perubahan", "alih", "pengalihan"
        ]
        self.kw_capital_restructuring = [
            "restructuring", "capital reduction", "capital restructuring" 
        ]

    @staticmethod
    def extract_datetime(text: str) -> str:
        """
        Extract datetime from text using regex pattern.

        Args:
            text (str): Text containing datetime information

        Returns:
            str: Extracted datetime string or empty string if not found
        """
        pattern = r"\d{2}-\d{2}-\d{4} \d{2}:\d{2}(?::\d{2})?"
        match = re.search(pattern, text)
        return match.group(0) if match else ""

    @staticmethod
    def extract_number(input_string: str) -> Optional[int]:
        """
        Extract numeric part from input string.

        Args:
            input_string (str): String containing number and other characters

        Returns:
            Optional[int]: Extracted number or None if not found
        """
        try:
            cleaned = input_string.replace(".", "")
            match = re.search(r"\d+", cleaned)
            return int(match.group()) if match else None
        except (ValueError, AttributeError):
            logger.warning(f"Could not extract number from: {input_string}")
            return None
        
    def translator(self, text: str) -> str:
        try:
            translated = GoogleTranslator(source='auto', target='en').translate(text) 
            return translated
        except Exception as error:
            logger.error(f'Error translator: {error}')
            return None

    def extract_flag_tags(self, text: str, list_flag_tags: list[str]) -> bool:
        text_lower = (text or "").lower() 
        return any(flag_tag in text_lower for flag_tag in list_flag_tags)

    def parse_datetime_idn(self, date_ind: str) -> str:
        parts = date_ind.split()
        if len(parts) != 3:
            return None 
        
        day, month, year = parts
        month_eng = MONTH_MAPPING.get(month.lower())

        if not month_eng:
            return None
        
        english_date = f"{day} {month_eng} {year}"
        english_obj =  datetime.strptime(english_date, "%d %B %Y")
        return english_obj.strftime("%Y-%m-%d")

    def extract_info(self, text: str) -> Dict[str, Any]:
        """
        Extract filing information from PDF text.

        Args:
            text (str): Raw PDF text content

        Returns:
            Dict[str, Any]: Extracted filing information
        """
        lines = text.split("\n")

        # print(f"\nlines extracted: {lines}")

        # Initialize article info with default values
        article_info = {
            "document_number": "",
            "company_name": "",
            "holder_name": "",
            "ticker": "",
            "category": "",
            "control_status": "",
            "transactions": [],
            "shareholding_before": "",
            "shareholding_after": "",
            "share_percentage_before": "",
            "share_percentage_after": "",
            "share_percentage_transaction": "",
            "purpose": "",
            "date_time": "",
            "price": 0,
            "price_transaction": {"prices": [], "amount_transacted": [], "types": []},
        }

        # Extraction patterns for different fields
        extraction_patterns = {
            "document_number": "Nomor Surat",
            "company_name": "Nama Perusahaan",
            "ticker": "Kode Emiten",
        }

        price_transactions = []
        amounts_transacted = []
        transaction_types = []
        transaction_date = []
        transaction_type_map = {
            "pembelian": "buy",
            "penjualan": "sell"
        }
        
        try:
            transactions_extracted = False
            for i, line in enumerate(lines):
                # Extract basic information
                for field, pattern in extraction_patterns.items():
                    if pattern in line and i > 0:
                        article_info[field] = lines[i - 1].strip()

                # flag for tags
                if "go to indonesian page" in (line or "").lower():
                    text_to_find = "\n".join(lines[i + 1:])
                    flag_checks = {
                        "inheritance": self.kw_inherit,
                        "MESOP": self.kw_mesop,
                        "free_float_requirement": self.kw_freefloat,
                        "share-transfer": self.kw_transfer
                    }

                    article_info['flag_tags'] = None

                    for tag, keyword in flag_checks.items():
                        if self.extract_flag_tags(text_to_find, keyword):
                            article_info["flag_tags"] = tag
                            break

                # Extract holder name (special case)
                if "Nama Pemegang Saham" in line:
                    holder_name = " ".join(line.split()[3:])
                    holder_name_cleaned = clean_company_name(holder_name)
                    article_info['holder_name'] = holder_name_cleaned
                    
                # Extract category
                if "Kategori" in line:
                    article_info["category"] = " ".join(line.split()[1:])

                # Extract control status
                if "Status Pengedali" in line or "Status Pengendali" in line:
                    article_info["control_status"] = " ".join(line.split()[2:])

                # Extract shareholding information
                shareholding_fields = {
                    "Jumlah Saham Sebelum Transaksi": "shareholding_before",
                    "Jumlah Saham Setelah Transaksi": "shareholding_after",
                    "Persentase Saham Sebelum Transaksi": "share_percentage_before",
                    "Persentase Saham Sesudah Transaksi": "share_percentage_after",
                    "Persentase Saham yang ditransaksi": "share_percentage_transaction",
                }

                for pattern, field in shareholding_fields.items():
                    if pattern in line:
                        article_info[field] = line.split()[-1]

                # Extract purpose
                if "Tujuan Transaksi" in line:
                    purpose = " ".join(line.split()[2:])
                    purpose = self.translator(purpose)
                    article_info['purpose'] = purpose

                # Extract datetime
                if "Tanggal dan Waktu" in line or "Date and Time" in line:
                    date_time_str = " ".join(line.split()[3:])
                    date_time_str = self.extract_datetime(date_time_str)

                    if date_time_str:
                        try:
                            parsed_date = datetime.strptime(
                                date_time_str, "%d-%m-%Y %H:%M"
                            )
                            article_info["date_time"] = parsed_date.strftime(
                                "%d-%m-%Y %H:%M"
                            )
                        except ValueError:
                            logger.warning(f"Could not parse date: {date_time_str}")
                            article_info["date_time"] = date_time_str

                # Extract transaction prices
                if "Jenis Transaksi Harga Transaksi" in line and i + 2 < len(lines):
                    if not transactions_extracted and "Jenis Transaksi Harga Transaksi" in line and i + 2 < len(lines):
                        transactions_extracted = True
                        try:
                            # Get the main transaction price
                            price_line = lines[i + 2].split(" ")
                            # print(f"price line: {price_line}\n")

                            if len(price_line) > 1:
                                article_info["price"] = (
                                    self.extract_number(price_line[1]) or 0
                                )
                            
                            for j in range(i + 2, len(lines)):
                                line_parts = lines[j].split(" ")
                                # print(f'\n check line parts: {line_parts}')
                                if len(line_parts) == 0:
                                    break

                                transaction_type = line_parts[0]
                                valid_types = ["Pembelian", "Penjualan", "Pengalihan", 'Hibah', 'Waris']

                                if transaction_type in valid_types:
                                    if transaction_type in ["Pembelian", "Penjualan"]:
                                        transaction_types.append(transaction_type_map.get(transaction_type.lower(), ""))
                                    else:
                                        transaction_types.append('others')

                                    if len(line_parts) > 1:
                                        price = self.extract_number(line_parts[1])
                                        if price is not None:
                                            price_transactions.append(price)

                                    if len(line_parts) > 5:
                                        # Extract amount transacted
                                        amount = self.extract_number(line_parts[5])
                                        if amount is not None:
                                            amounts_transacted.append(amount)
                                        
                                        # Extract transaction date if available
                                        date_time_str = " ".join(line_parts[2:5])
                                        if date_time_str: 
                                            try:
                                                parsed_date = self.parse_datetime_idn(date_time_str)
                                                if parsed_date:
                                                    transaction_date.append(parsed_date)
                                                else:
                                                    transaction_date.append(None)
                                            except ValueError:
                                                logger.warning(f"Could not parse transaction date: {date_time_str}")
                                                transaction_date.append(None)
                                else:
                                    if "Jenis" in line_parts[0]:
                                        continue

                                    if ("Tujuan" in line_parts[0] or 
                                    "Jumlah" in line_parts[0] or 
                                    "Status" in line_parts[0]):
                                        break 

                            article_info["price_transaction"]["prices"] = price_transactions
                            article_info["price_transaction"]["amount_transacted"] = (
                                amounts_transacted
                            )
                            article_info["price_transaction"]["types"] = transaction_types
                            article_info['price_transaction']['dates'] = transaction_date

                        except (IndexError, ValueError) as e:
                            logger.warning(f"Error extracting transaction prices: {e}")

        except Exception as e:
            logger.error(f"Error extracting filing information: {e}")

        # print(f'\n extract_info: {json.dumps(article_info, indent=2)}\n')
        return article_info

    def generate_article_filings(
        self,
        pdf_url: str,
        sub_sector: str,
        holder_type: str,
        data: str,
        uid: Optional[str] = None,
    ) -> Dict[str, Any]:
        """
        Generate article from PDF filing data.

        Args:
            pdf_url (str): URL of the PDF source
            sub_sector (str): Sub-sector classification
            holder_type (str): Type of holder (insider/institution)
            data (str): Raw PDF text content
            uid (Optional[str]): Unique identifier for the filing

        Returns:
            Dict[str, Any]: Generated article data
        """
        logger.info(f"Generating article from PDF: {pdf_url}")

        # Initialize article structure
        article = self._initialize_article_structure(
            pdf_url, sub_sector, holder_type, uid
        )

        try:
            # Extract information from PDF text
            article_info = self.extract_info(data)

            # Populate article with extracted information
            self._populate_article_data(article, article_info)

            # Generate title and body using LLM
            article = self._generate_title_and_body(article)

            # Update sector information if missing
            article = self._update_sector_information(article)

            # Clean up article (remove purpose if exists)
            # if "purpose" in article:
            #     del article["purpose"]

            logger.info(
                f"Successfully generated article for {article_info.get('company_name', 'Unknown')}"
            )
            # print('\n generated article: ', json.dumps(article, indent=2), '\n')
            return article

        except Exception as e:
            logger.error(f"Error generating article: {e}")
            # Return minimal article structure on error
            return article

    def _initialize_article_structure(
        self, pdf_url: str, sub_sector: str, holder_type: str, uid: Optional[str]
    ) -> Dict[str, Any]:
        """Initialize basic article structure."""
        return {
            "title": "",
            "body": "",
            "source": pdf_url,
            "timestamp": "",
            "sub_sector": sub_sector,
            "sector": self.sectors_data.get(sub_sector, ""),
            "tags": [],
            "tickers": [],
            "transaction_type": "",
            "holder_type": holder_type,
            "holding_before": 0,
            "holding_after": 0,
            "share_percentage_before": 0.0,
            "share_percentage_after": 0.0,
            "share_percentage_transaction": 0.0,
            "amount_transaction": 0,
            "holder_name": "",
            "purpose": '',
            "price": 0,
            "transaction_value": 0,
            "price_transaction": {"prices": [], "amount_transacted": []},
            "UID": uid,
            "flag_tags": ""
        }

    def _populate_article_data(
        self, article: Dict[str, Any], article_info: Dict[str, Any]
    ) -> None:
        """Populate article with extracted information."""
        try:
            article['flag_tags'] = article_info.get('flag_tags')

            # Company Name 
            article['company_name'] = article_info.get('company_name')

            # Basic information
            article["title"] = (
                f"Informasi insider trading {article_info['holder_name']} dalam {article_info['company_name']}"
            )
            article["body"] = (
                f"{article_info['document_number']} - {article_info['date_time']} - "
                f"Kategori {article_info['category']} - Transaksi {article_info['holder_name']} "
                f"dalam saham {article_info['company_name']} berubah dari "
                f"{article_info['shareholding_before']} menjadi {article_info['shareholding_after']}"
            )

            # Ticker information
            ticker = article_info["ticker"].upper()
            if not ticker.endswith(".JK"):
                ticker += ".JK"
            article["tickers"] = [ticker]

            # Timestamp handling
            date_time = article_info["date_time"]
            if date_time:
                try:
                    if not date_time.endswith(":00"):
                        date_time += ":00"
                    parsed_timestamp = datetime.strptime(date_time, "%d-%m-%Y %H:%M:%S")
                    article["timestamp"] = parsed_timestamp.strftime(
                        "%Y-%m-%d %H:%M:%S"
                    )
                except ValueError as e:
                    logger.warning(f"Error parsing timestamp {date_time}: {e}")
                    article["timestamp"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

            # Financial data
            self._populate_financial_data(article, article_info)

            # Transaction value calculation
            if (
                article_info["price_transaction"]["amount_transacted"]
                and article_info["price_transaction"]["prices"]
            ):
                price_transaction_types = article_info['price_transaction'].get('types')
                if price_transaction_types:
                    price_transaction = PriceTransaction(
                        amount_transacted=article_info["price_transaction"][
                            "amount_transacted"
                        ],
                        prices=article_info["price_transaction"]["prices"],
                        transaction_type=article_info['price_transaction']['types']
                    )
                    
                    check_unique_type = len(set(article_info['price_transaction']['types']))
                    if check_unique_type > 1: 
                        article["price"], article["transaction_value"], article['transaction_type'] = (
                            price_transaction.get_price_transaction_value_two_values()
                        )
                    else: 
                        article["price"], article["transaction_value"] = (
                        price_transaction.get_price_transaction_value()
                    )
                else:
                    price_transaction = PriceTransaction(
                        amount_transacted=article_info["price_transaction"]["amount_transacted"],
                        prices=article_info["price_transaction"]["prices"],
                        transaction_type=[]  
                    )
                
                    article["price"], article["transaction_value"] = (
                        price_transaction.get_price_transaction_value()
                    )

        except Exception as e:
            logger.error(f"Error populating article data: {e}")

    def _populate_financial_data(
        self, article: Dict[str, Any], article_info: Dict[str, Any]
    ) -> None:
        """Populate financial data with proper error handling."""
        try:
            # Holdings
            holding_before_str = article_info["shareholding_before"].replace(".", "")
            holding_after_str = article_info["shareholding_after"].replace(".", "")

            article["holding_before"] = (
                int(holding_before_str) if holding_before_str.isdigit() else 0
            )
            article["holding_after"] = (
                int(holding_after_str) if holding_after_str.isdigit() else 0
            )

            # Percentages
            for field in [
                "share_percentage_before",
                "share_percentage_after",
                "share_percentage_transaction",
            ]:
                value_str = article_info[field].replace(",", ".").replace("%", "")
                try:
                    article[field] = float(value_str) if value_str else 0.0
                except ValueError:
                    article[field] = 0.0

            # Transaction details
            price_transaction_types = article_info.get('price_transaction', {}).get('types')
            if 'others' in price_transaction_types:
                article['transaction_type'] = 'others'
            elif price_transaction_types and len(set(price_transaction_types)) == 1:
                article["transaction_type"] = (
                    "buy"
                    if article["holding_before"] < article["holding_after"]
                    else "sell"
                )

            article["amount_transaction"] = abs(
                article["holding_before"] - article["holding_after"]
            )
            article["holder_name"] = article_info["holder_name"]
            article["purpose"] = article_info["purpose"]
            article["price_transaction"] = article_info["price_transaction"]

        except Exception as e:
            logger.error(f"Error populating financial data: {e}")

    def _generate_title_and_body(self, article: Dict[str, Any]) -> Dict[str, Any]:
        """Generate enhanced title and body using LLM."""
        try:
            # new_title, new_body = summarize_filing_manual(article)
            
            holder_name = article.get('holder_name')
            holder_name = holder_name.rstrip('.')
            company_name = article.get('company_name')
            transaction_type = article.get('transaction_type').lower()
            amount_transaction = article.get('amount_transaction')
            holding_before = article.get('holding_before') 
            holding_after = article.get('holding_after') 
            purpose = article.get('purpose')
            purpose = purpose.rstrip('.')

            new_title, new_body = summarize_filing_manual(
                holder_name, company_name, transaction_type, 
                amount_transaction, holding_before, holding_after, 
                purpose
            )

            if new_body:
                article["body"] = new_body

            if new_title:
                article["title"] = new_title

        except Exception as e:
            logger.error(f"Error generating title and body: {e}")

        return article

    def _update_sector_information(self, article: Dict[str, Any]) -> Dict[str, Any]:
        """Update sector information if missing."""
        # print(f"\n article: {article}")
        try:
            if article.get("tickers"):
                # Try to get sub_sector from companies data
                try:
                    with open("./data/companies.json", "r") as f:
                        companies = json.load(f)
                   
                    ticker = article.get('tickers')[0]
                    if ticker in companies:
                        article["sub_sector"] = companies[ticker]["sub_sector"]

                except (FileNotFoundError, KeyError, IndexError) as e:
                    logger.warning(
                        f"Could not update sub_sector from companies data: {e}"
                    )

            # Update sector if missing
            if (
                not article.get("sector")
                and article.get("sub_sector") in self.sectors_data
            ):
                article["sector"] = self.sectors_data[article["sub_sector"]]

        except Exception as e:
            logger.error(f"Error updating sector information: {e}")

        return article


# Global instance for backward compatibility
_generator = FilingArticleGenerator()


# Export functions for backward compatibility
def generate_article_filings(
    pdf_url: str,
    sub_sector: str,
    holder_type: str,
    data: str,
    uid: Optional[str] = None,
) -> Dict[str, Any]:
    """
    Generate article from PDF filing data.

    This function maintains backward compatibility with the existing API.
    """
    return _generator.generate_article_filings(
        pdf_url, sub_sector, holder_type, data, uid
    )


def extract_info(text: str) -> Dict[str, Any]:
    """Extract filing information from PDF text (backward compatibility)."""
    return _generator.extract_info(text)


def extract_datetime(text: str) -> str:
    """Extract datetime from text (backward compatibility)."""
    return FilingArticleGenerator.extract_datetime(text)


def extract_number(input_string: str) -> Optional[int]:
    """Extract number from string (backward compatibility)."""
    return FilingArticleGenerator.extract_number(input_string)
