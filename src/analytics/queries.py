import duckdb
import pandas as pd


def get_latest_performance(
    connection: duckdb.DuckDBPyConnection,
) -> pd.DataFrame:
    """Get currency performance for the latest week."""

    query = """
        SELECT
            week_start,
            base,
            quote,
            start_rate,
            end_rate,
            min_rate,
            max_rate,
            avg_rate,
            weekly_return_pct
        FROM weekly_performance
        WHERE week_start = (
            SELECT MAX(week_start)
            FROM weekly_performance
        )
        ORDER BY weekly_return_pct DESC
    """

    return connection.execute(query).df()


def get_top_performers(
    connection: duckdb.DuckDBPyConnection,
    limit: int = 5,
) -> pd.DataFrame:
    """Get best-performing currency pairs."""

    query = """
        SELECT
            week_start,
            base,
            quote,
            weekly_return_pct
        FROM weekly_performance
        WHERE week_start = (
            SELECT MAX(week_start)
            FROM weekly_performance
        )
        ORDER BY weekly_return_pct DESC
        LIMIT ?
    """

    return connection.execute(
        query,
        [limit],
    ).df()


def get_worst_performers(
    connection: duckdb.DuckDBPyConnection,
    limit: int = 5,
) -> pd.DataFrame:
    """Get worst-performing currency pairs."""

    query = """
        SELECT
            week_start,
            base,
            quote,
            weekly_return_pct
        FROM weekly_performance
        WHERE week_start = (
            SELECT MAX(week_start)
            FROM weekly_performance
        )
        ORDER BY weekly_return_pct ASC
        LIMIT ?
    """

    return connection.execute(
        query,
        [limit],
    ).df()


def get_pair_history(
    connection: duckdb.DuckDBPyConnection,
    base: str,
    quote: str,
) -> pd.DataFrame:
    """Get weekly performance history for a pair."""

    query = """
        SELECT
            week_start,
            base,
            quote,
            start_rate,
            end_rate,
            min_rate,
            max_rate,
            avg_rate,
            weekly_return_pct
        FROM weekly_performance
        WHERE base = ?
          AND quote = ?
        ORDER BY week_start
    """

    return connection.execute(
        query,
        [base, quote],
    ).df()


def get_weekly_summary(
    connection: duckdb.DuckDBPyConnection,
) -> pd.DataFrame:
    """Get summary statistics for each week."""

    query = """
        SELECT
            week_start,
            COUNT(*) AS pair_count,
            AVG(weekly_return_pct)
                AS avg_weekly_return_pct,
            MAX(weekly_return_pct)
                AS best_weekly_return_pct,
            MIN(weekly_return_pct)
                AS worst_weekly_return_pct
        FROM weekly_performance
        GROUP BY week_start
        ORDER BY week_start DESC
    """

    return connection.execute(query).df()

