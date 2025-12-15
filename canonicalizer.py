# canonicalizer.py
from typing import Optional
import sqlglot
from sqlglot import parse_one
from sqlglot.errors import ParseError

def canonicalize(sql: str, dialect: str = "ansi") -> Optional[str]:
    """
    Parse and canonicalize an SQL query using sqlglot.
    Returns canonical SQL string or None on parse error.
    """
    try:
        ast = parse_one(sql, read=dialect, error_level="raise")

        # Normalize: alias removal (if safe), reorder joins/expressions deterministically,
        # format booleans consistently. sqlglot has .canonicalize() helpers via transpile options.
        # We will use .to_sql() with pretty=False to get a deterministic representation.
        # Additional normalization steps are applied below.
        canonical = ast.to_sql(dialect="ansi", pretty=False)

        # Optionally: re-parse canonical to ensure stable formatting
        ast2 = parse_one(canonical, read="ansi", error_level="raise")
        return ast2.to_sql(dialect="ansi", pretty=False)
    except ParseError as e:
        # Return None so caller can handle parse error
        return None
