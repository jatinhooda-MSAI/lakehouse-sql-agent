import sqlglot
from sqlglot import exp


class GuardError(Exception):
    pass


def check(sql: str, dialect: str, schemas: set[str], catalog: str = "") -> None:
    statements = [s for s in sqlglot.parse(sql, read=dialect) if s is not None]
    if len(statements) != 1:
        raise GuardError(f"expected exactly one statement, got {len(statements)}")
    tree = statements[0]
    if not isinstance(tree, exp.Query) or tree.find(exp.DML, exp.DDL):
        raise GuardError(f"only a single SELECT or WITH query is allowed, got {tree.key.upper()}")
    ctes = {cte.alias for cte in tree.find_all(exp.CTE)}
    for table in tree.find_all(exp.Table):
        if not isinstance(table.this, exp.Identifier):
            raise GuardError(f"table functions are not allowed: {table.sql(dialect)}")
        if not table.db and table.name in ctes:
            continue
        if table.db not in schemas or table.catalog not in ("", catalog):
            allowed = ", ".join(sorted(s or "unqualified" for s in schemas))
            raise GuardError(f"table {table.sql(dialect)} is outside the allowed schemas ({allowed})")
