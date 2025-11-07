"""
This package groups all company-related utilities.

The __init__.py file exposes the primary "facade" class (CompanyService)
and other common helpers, making imports cleaner for other modules.
"""
from .service import CompanyService
from .format import pretty_company_name, canonical_name_for_symbol, normalize_company_name
from .resolver import suggest_symbols, resolve_symbol_and_name

__all__ = [
    "CompanyService",
    "pretty_company_name",
    "canonical_name_for_symbol",
    "normalize_company_name",
    "suggest_symbols",
    "resolve_symbol_and_name",
    "build_reverse_map",
]
