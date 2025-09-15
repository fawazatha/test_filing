import json

class PriceTransaction:
  prices: list[float] = []
  amount_transacted: list[float] = []
  def __init__(self, amount_transacted: list[float], prices: list[float]):
    """
    Initializes the Transaction object.

    @param amount: Amount of the transaction.
    @param price: Price of the transaction.
    """
    self.amount_transacted = amount_transacted
    self.prices = prices

  def get_price_transaction_value(self):
    """
    Calculates the price and transaction value of the transaction.
    
    @return: Tuple containing the price and transaction value.
    """
    sum_price_transaction = 0
    sum_transaction = 0
    for i in range(len(self.prices)):
      sum_price_transaction += self.prices[i] * self.amount_transacted[i]
      sum_transaction += self.amount_transacted[i]
    self.price = round(sum_price_transaction / sum_transaction if sum_transaction != 0 else 0, 3)
    self.transaction_value = sum_price_transaction
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

# Example usage
# transaction = PriceTransaction(amount_transacted=[100,200], prices=[9.99, 8.99])
# json_str = transaction.to_json()
# print(json_str)

# new_transaction = PriceTransaction.from_json(json_str)
# print(new_transaction.__dict__)