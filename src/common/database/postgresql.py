from typing import TYPE_CHECKING

from sqlalchemy import create_engine


if TYPE_CHECKING:
    import pandas as pd  # noqa: TC004


def readPostgresQuery(
    query: str, credentials_string: str, **kwargs
) -> pd.DataFrame:
    """Read a query from PostgreSQL database.

    Wrapper over ``pandas.read_sql_query`` function to read a SQL query
    from a PostgreSQL database

    Parameters
    ----------
    query : str
        SQL query
    credentials_string : str
        Credentials string for database connection
    kwargs
        Arguments passed to the ``pandas.read_sql_query`` function

    Returns
    -------
    response : pd.DataFrame
        Pandas DataFrame object with the Netezza SQL query response
    """
    # Stablish connection
    db_connection = create_engine(credentials_string)

    # Make the query
    response = pd.read_sql_query(query, con=db_connection, **kwargs)

    # Close connection
    db_connection.dispose()

    return response


if __name__ == '__main__':
    err_msg = 'This file is only meant to be imported.'
    raise Exception(err_msg)
