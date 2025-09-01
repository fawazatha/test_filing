import os
import logging
from typing import Dict, Any, Optional, List, Tuple

logger = logging.getLogger(__name__)

class PriceCalculator:
    """
    Calculator for price and transaction value computations.
    
    Handles weighted average calculations and transaction value computations
    from price and amount data.
    """

    def __init__(self):
        """Initialize price calculator."""
        pass

    def calculate_weighted_price_and_value(
        self, 
        prices: List[int], 
        amounts: List[int]
    ) -> Tuple[int, int]:
        """
        Calculate weighted average price and total transaction value.

        Args:
            prices (List[int]): List of transaction prices
            amounts (List[int]): List of transaction amounts

        Returns:
            Tuple[int, int]: (weighted_average_price, total_transaction_value)

        Raises:
            ValueError: If input lists are invalid or empty
        """
        if not prices or not amounts:
            return 0, 0

        if len(prices) != len(amounts):
            raise ValueError("Prices and amounts lists must have the same length")

        try:
            total_value = 0
            total_shares = 0

            for price, amount in zip(prices, amounts):
                if price < 0 or amount < 0:
                    logger.warning(f"Negative values detected: price={price}, amount={amount}")
                    continue
                
                total_value += price * amount
                total_shares += amount

            if total_shares == 0:
                return 0, 0

            weighted_price = round(total_value / total_shares, 0)
            
            logger.debug(f"Calculated weighted price: {weighted_price}, total value: {total_value}")
            return int(weighted_price), total_value

        except Exception as e:
            logger.error(f"Error calculating price and value: {e}")
            return 0, 0

    def calculate_simple_average(self, prices: List[int]) -> float:
        """
        Calculate simple average of prices.

        Args:
            prices (List[int]): List of prices

        Returns:
            float: Average price
        """
        if not prices:
            return 0.0

        valid_prices = [p for p in prices if p > 0]
        if not valid_prices:
            return 0.0

        return sum(valid_prices) / len(valid_prices)


# Global instances for backward compatibility
_price_calculator = PriceCalculator()


def get_company_info(symbol: str) -> Optional[Dict[str, Any]]:
    """
    Get company information by symbol.
    
    Backward-compatible function that uses global database helper instance.
    """
    return _database_helper.get_company_info(symbol)


def calculate_weighted_price_and_value(
    prices: List[int], 
    amounts: List[int]
) -> Tuple[int, int]:
    """
    Calculate weighted average price and total transaction value.
    
    Backward-compatible function that uses global price calculator instance.
    """
    return _price_calculator.calculate_weighted_price_and_value(prices, amounts)