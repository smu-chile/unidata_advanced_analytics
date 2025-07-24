import pandas as pd
from sqlalchemy import create_engine


def readPostgresQuery(
    query: str, credentials_dict: dict, **kwargs
) -> pd.DataFrame:
    """Read a query from PostgreSQL database.

    Wrapper over ``pandas.read_sql_query`` function to read a SQL query
    from a PostgreSQL database

    Parameters
    ----------
    query : str
        SQL query
    credentials_dict : dict
        Credentials string for database connection
    kwargs
        Arguments passed to the ``pandas.read_sql_query`` function

    Returns
    -------
    response : pd.DataFrame
        Pandas DataFrame object with the Netezza SQL query response
    """
    # Stablish connection
    db_connection = create_engine(
        'postgresql://'
        + credentials_dict['username']
        + ':'
        + credentials_dict['password']
        + '@'
        + credentials_dict['host']
        + ':'
        + credentials_dict['port']
        + '/postgres'
    )

    # Make the query
    response = pd.read_sql_query(query, con=db_connection, **kwargs)

    # Close connection
    db_connection.dispose()

    return response


if __name__ == '__main__':
    err_msg = 'This file is only meant to be imported.'
    raise Exception(err_msg)
