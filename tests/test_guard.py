import pytest

from sqlagent.guard import GuardError, check

WAREHOUSE = ("duckdb", {"gold", "silver"}, "warehouse")
SQLITE = ("sqlite", {"", "main"})


@pytest.mark.parametrize(
    "sql",
    [
        "SELECT SUM(incidents) FROM gold.fct_crimes_daily WHERE crime_date >= DATE '2026-03-01'",
        "select * from warehouse.gold.dim_community_area;",
        "SELECT d.community_area_name, COUNT(*) FROM silver.stg_crimes c "
        "JOIN gold.dim_community_area d USING (community_area) GROUP BY 1",
        "WITH m AS (SELECT month, SUM(incidents) AS n FROM gold.mart_area_month GROUP BY 1) "
        "SELECT month, n - LAG(n) OVER (ORDER BY month) FROM m",
        "SELECT 1 AS x UNION ALL SELECT 2",
        "SELECT * FROM (SELECT primary_type FROM gold.fct_crimes_daily) t",
        "SELECT * FROM gold.fct_311_daily WHERE sr_type IN (SELECT sr_type FROM silver.stg_311_requests)",
    ],
)
def test_allowed(sql):
    check(sql, *WAREHOUSE)


@pytest.mark.parametrize(
    "sql",
    [
        "DROP TABLE gold.dim_community_area",
        "DELETE FROM gold.fct_crimes_daily",
        "INSERT INTO gold.dim_community_area VALUES (99, 'X')",
        "UPDATE gold.dim_community_area SET community_area_name = 'X'",
        "CREATE TABLE gold.copy AS SELECT * FROM gold.dim_community_area",
        "WITH d AS (DELETE FROM gold.dim_community_area RETURNING *) SELECT * FROM d",
        "SELECT 1; DROP TABLE gold.dim_community_area",
        "PRAGMA database_list",
        "ATTACH 'other.duckdb' AS other",
        "COPY gold.fct_crimes_daily TO 'out.csv'",
        "INSTALL httpfs",
        "LOAD httpfs",
        "SET threads = 1",
        "SELECT * FROM read_csv('/etc/passwd')",
        "SELECT * FROM read_parquet('s3://bucket/x.parquet')",
        "SELECT * FROM gold.fct_crimes_daily, read_text('/etc/hosts')",
        "SELECT * FROM glob('*')",
        "SELECT * FROM 'data.csv'",
        "SELECT * FROM duckdb_settings()",
        "SELECT * FROM information_schema.tables",
        "SELECT * FROM main.secrets",
        "SELECT * FROM fct_crimes_daily",
        "SELECT * FROM other.gold.dim_community_area",
    ],
)
def test_blocked(sql):
    with pytest.raises(GuardError):
        check(sql, *WAREHOUSE)


def test_sqlite():
    check('SELECT name FROM singer WHERE country = "France"', *SQLITE)
    check("SELECT COUNT(*) FROM main.singer", *SQLITE)
    for sql in ["ATTACH DATABASE 'x.db' AS x", "PRAGMA table_info(singer)", "SELECT * FROM aux.singer"]:
        with pytest.raises(GuardError):
            check(sql, *SQLITE)
