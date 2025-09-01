# Adapter: try to use your Pydantic models; fall back to plain dicts.
from typing import Any

USING_PYDANTIC_MODELS = True
try:
    from src.models.parsed_filing import (
        Filing as FilingModel,
        Transaction as TransactionModel,
        LinkingMeta as LinkingMetaModel,
    )
except Exception:
    USING_PYDANTIC_MODELS = False
    FilingModel = TransactionModel = LinkingMetaModel = None  # type: ignore

def make_filing(**kwargs) -> Any:
    return FilingModel(**kwargs) if USING_PYDANTIC_MODELS else kwargs

def make_transaction(**kwargs) -> Any:
    return TransactionModel(**kwargs) if USING_PYDANTIC_MODELS else kwargs

def make_linking_meta(**kwargs) -> Any:
    return LinkingMetaModel(**kwargs) if USING_PYDANTIC_MODELS else kwargs
