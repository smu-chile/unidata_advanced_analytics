# pip
import pandas as pd
import awswrangler as wr
from boto3 import Session


def readAthenaQuery(query: str, user: str, **kwargs) -> pd.DataFrame:
    """Read a query from AWS Athena -sort of- SQL database.

    Wrapper over ``awswrangler.athena.read_sql_query`` function to read a
    SQL query from AWS Athena database

    Parameters
    ----------
    query : str
        SQL query
    user : str
        Name of the user making the query
    kwargs
        Arguments passed to the ``wr.athena.read_sql_query`` function

    Returns
    -------
    response : pd.DataFrame
        Pandas DataFrame object with the Netezza SQL query response
    """
    return wr.athena.read_sql_query(
        query,
        s3_output=f's3://smu-datalake-test-athena-query-results/{user}/',
        **kwargs
    )


def createTableAsSelect(
        user: str, query: str, database: str, table_name: str,
        athena_workgroup: str, use_threads: bool | int = True, *,
        boto3_session: Session = None, storage_format: str = 'parquet',
        write_compression: str = 'snappy', wait: bool = True,  **kwargs
    ) -> tuple[bool, dict]:
    """Create a table from a ``SELECT`` statement in Athena.

    .. warning:: As the names of the files in S3 are searched using RegEx,
       if your table name is "TABLE_A" it will delete the contents of all
       tables called "TABLE_A*" where * is the wildcard in RegEx.

    Parameters
    ----------
    user : str
        Name of the user making this operation
    query : str
        Query containing the ``SELECT`` statement for the CTAS
    database : str
        Database in which the table will be stored
    table_name : str
        Name of the table to be created with the CTAS
    athena_workgroup : str
        Athena workgroup
    use_threads : bool | int, default=True
        Use or not threads if the operation allows for it. Specific number
        of threads to be osed or all when True
    boto3_session : Session, optional
        Custom Boto3 session for the queries
    storage_format : str, default='parquet'
        The storage format for the CTAS query results, such as ORC,
        PARQUET, AVRO, JSON, or TEXTFILE. PARQUET by default
    write_compression : str, default='snappy'
        The compression type to use for any storage format that allows
        compression to be specified
    wait : bool, default=True
        Whether to wait for the query to finish and return a dictionary
        with the Query metadata
    **kwargs
        Arguments passed on to the ``wr.athena.create_ctas_table`` function

    Returns
    -------
    s3_table_output : str
        S3 URI with the table content otuput
    delete_table_response : bool
        Reponse from the delete table operation
    ctas_response : dict
        Response from the CTAS operation
    """
    s3_user_output = f's3://smu-datalake-test-athena-query-results/{user}/'
    s3_table_output = f's3://smu-datalake-test-athena-query-results/{user}/{table_name.lower()}'
    delete_table_response = wr.catalog.delete_table_if_exists(
        database=database,
        table=table_name,
        boto3_session=boto3_session
    )
    wr.s3.delete_objects(
        path=s3_table_output,
        use_threads=use_threads,
        boto3_session=boto3_session
    )
    ctas_response = wr.athena.create_ctas_table(
        sql=query,
        database=database,
        ctas_table=table_name,
        s3_output=s3_user_output,
        workgroup=athena_workgroup,
        storage_format=storage_format,
        write_compression=write_compression,
        wait=wait,
        boto3_session=boto3_session,
        **kwargs
    )
    return s3_table_output, delete_table_response, ctas_response


if __name__ == '__main__':
    err_msg = 'This file is only meant to be imported.'
    raise Exception(err_msg)
