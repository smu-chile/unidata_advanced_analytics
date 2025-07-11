"""Extension of the google.cloud.bigquery modules."""
from __future__ import annotations

# Default
import json
from typing import TYPE_CHECKING, Literal

# pip
import pandas_gbq
from google.cloud import bigquery
from google.cloud.exceptions import BadRequest

# Own
from ..databases.queries import QueryDict  # noqa: TID252


# Type checking imports
if TYPE_CHECKING:
    from datetime import datetime

    import pandas as pd


_TIME_PARTITIONING_TYPES = {
    'DAY': bigquery.TimePartitioningType.DAY,
    'HOUR': bigquery.TimePartitioningType.HOUR,
    'MONTH': bigquery.TimePartitioningType.MONTH,
    'YEAR': bigquery.TimePartitioningType.YEAR,
}


def readBigQuery(
        query: str, user: str, gbq_client: bigquery.Client,
        **kwargs
    ) -> pd.DataFrame | None:
    """Read a query from a GCP BigQuery table.

    Wrapper over `pandas_gbq.read_gbq` function to send a SQL query to a
    GCP BigQuery database

    Parameters
    ----------
    query : str
        SQL query
    user : str
        Name of the user making the query
    gbq_client : bigquery.Client
        Client used for making the queries
    **kwargs
        Arguments passed to the `pandas_gbq.read_gbq` function

    Returns
    -------
    response_df : pd.DataFrame | None
        Pandas DataFrame object with the query response
    """
    return pandas_gbq.read_gbq(**{
        'query_or_table': query,
        'configuration': {
            'labels': {
                'user': user
            }
        },
        'progress_bar_type': None,
        'bigquery_client': gbq_client,
        **kwargs
    })


def createTableFromJSON(
        ddl_json_config_path: str,
        project: str,
        gbq_client: bigquery.Client,
        if_exists: Literal['raise', 'ignore', 'rebuild'] = 'raise',
    ) -> dict:
    """Create a BigQuery table using a JSON config file.

    Parameters
    ----------
    ddl_json_config_path : str,
        Path to the JSON file with the DDL configuration for the table.
    project : str
        Google BigQuery project in which the table will be created.
    gbq_client : bigquery.Client
        Client used for making the queries
    if_exists : {'raise', 'ignore', 'rebuild'}, optional
        Behavior to take if the table allready exists.
        -  `raise` raises an `Already Exists` error
        -  `ignore` ignores `Already Exists` errors
        -  `rebuild` deletes the table with all its data and build it again

       .. warning:: All the data in the table will be lost if `rebuild` is
          used.

    Returns
    -------
    ddl_dict : dict
        Formatted dictionary with the DDL used for the table

    Raises
    ------
    google.cloud.exceptions.BadRequest
        If time and range partitioning are used at the same time.
    """
    # Args verification
    if if_exists not in ['raise', 'ignore', 'rebuild']:
        err_msg = "if_exists must be one of 'raise', 'ignore' or 'rebuild'"
        raise ValueError(err_msg)
    # Read config JSON
    with open(ddl_json_config_path) as f:
        ddl_config = json.load(f)

    # Build table location
    table_ref = '.'.join([
            project,
            ddl_config['schema'],
            ddl_config['table']
    ])

    # Verify either time or range partition is used
    if ('time_partitioning' in ddl_config
        and 'range_partitioning' in ddl_config):
        err_msg = (
            'Tables can only be defined using time or range partitioning, '
            'not both'
        )
        raise BadRequest(err_msg)


    # Configure table location and columns
    table = bigquery.Table(
        table_ref=table_ref,
        schema=[
            bigquery.SchemaField(**colum_config)
            for colum_config
            in ddl_config['columns']
        ]
    )

    # Configure time partition if given
    if 'time_partitioning' in ddl_config:
        # Configure time parititioning type
        ddl_config['time_partitioning']['type_'] = _TIME_PARTITIONING_TYPES[
            ddl_config['time_partitioning']['type_']
        ]

        table.time_partitioning = bigquery.TimePartitioning(
            **ddl_config['time_partitioning']
        )

    # Configure range partition if given
    elif 'range_partitioning' in ddl_config:
        # Configure range
        ddl_config['range_partitioning']['range_'] = bigquery.PartitionRange(
            **ddl_config['range_partitioning']['range_']
        )

        table.range_partitioning = bigquery.RangePartitioning(
            **ddl_config['range_partitioning']
        )

    # Configure clustering fields
    table.clustering_fields = ddl_config.get('clustering_fields', None)

    # Delete the previous table and buildit all over again
    if if_exists == 'rebuild':
        gbq_client.delete_table(table_ref, not_found_ok=True)
        gbq_client.create_table(table)
    elif if_exists == 'raise':
        gbq_client.create_table(table, exists_ok=False)
    else:
        gbq_client.create_table(table, exists_ok=True)

    # Return the DDL config dict
    return ddl_config


def createTableAsSelect(
        query: str,
        table_ref: str,
        gbq_client: bigquery.Client,
        create_disposition: Literal['CREATE_IF_NEEDED', 'CREATE_NEVER'] = 'CREATE_IF_NEEDED',
        write_disposition: Literal['WRITE_TRUNCATE', 'WRITE_APPEND', 'WRITE_EMPTY'] = 'WRITE_TRUNCATE',  # noqa: E501
        use_legacy_sql: bool = True,
        time_partitioning: bigquery.TimePartitioning | None = None,
        range_partitioning: bigquery.RangePartitioning | None = None,
        clustering_fields: list[str] | None = None
    ) ->  pd.DataFrame:
    """Create a BigQuery table populating it with the outputs of a select.

    Execute a CTAS using a user specified query. Waits until completion of
    the transaction.

    Parameters
    ----------
    query : str
        Query containing the ``SELECT`` statement for the CTAS
    table_ref : str
        Table where results are written. The value must included a project
        ID, dataset ID, and table ID, each separated by ``.``.
        For example: `your-project.your_dataset.your_table`
    gbq_client : bigquery.Client
        Client used for making the queries
    create_disposition : {'CREATE_IF_NEEDED', 'CREATE_NEVER'}
        Specifies whether the job is allowed to create new tables.
        -  `CREATE_IF_NEEDED`: If the table does not exist, BigQuery
           creates the table.
        -  `CREATE_NEVER`: The table must already exist. If it does not, a
           'notFound' error is returned in the job result.
    write_disposition: {'WRITE_TRUNCATE', 'WRITE_APPEND', 'WRITE_EMPTY'}
        Specifies the action that occurs if the destination table already
        exists:
        -  `WRITE_TRUNCATE`: If the table already exists, BigQuery
           overwrites the data, removes the constraints, and uses the
           schema from the query result.
        -  `WRITE_APPEND`: If the table already exists, BigQuery appends
           the data to the table
        -  `WRITE_EMPTY`: If the table already exists and contains data, a
           'duplicate' error is returned in the job result.
    use_legacy_sql : bool, default=True
        Specifies whether to use BigQuery's legacy SQL dialect for this
        query. If set to false, the query will use BigQuery's GoogleSQL
    time_partitioning : bigquery.TimePartitioning, optional
        Time-based partitioning specification for the destination table.
        Only one of timePartitioning and rangePartitioning should
        be specified.
    range_partitioning : bigquery.RangePartitioning, optional
        Range partitioning specification for the destination table. Only
        one of timePartitioning and rangePartitioning should be specified.
    clustering_fields : list[str], optional
        Fields defining clustering for the table

    Returns
    -------
    result : pd.DataFrame
        Empty DataFrame with the column names of the created table
    """
    ctas_response = gbq_client.query_and_wait(
        query=query,
        job_config=bigquery.QueryJobConfig(
            destination=table_ref,
            create_disposition=create_disposition,
            write_disposition=write_disposition,
            allow_large_results=True,
            use_legacy_sql=use_legacy_sql,
            time_partitioning=time_partitioning,
            range_partitioning=range_partitioning,
            clustering_fields=clustering_fields,
        ),
        max_results=0
    )
    return ctas_response.to_dataframe()


def uploadFrame(
        df: pd.DataFrame,
        table_ddl_json_path: str,
        project: str,
        gbq_client: bigquery.Client,
        if_exists: Literal['fail', 'replace', 'append'] = 'fail',
        progress_bar: bool = False,
        **kwargs
    ) -> None:
    """Uploads a Pandas DataFrame to a table in Google BigQuery.

    Parameters
    ----------
    df : pd.DataFrame
        Pandas DataFrame with the data to upload into Google BigQuery
    table_ddl_json_path : str
        Path to a json with the DDL of the table
    project : str
        Google BigQuery project in which the table will be loaded.
    gbq_client : bigquery.Client
        Client used for making the queries
    if_exists : {'fail', 'replace', 'append'}
        Behavior to take if the table allready exists.
        -  `fail` throws an error
        -  `replace` re-builds the table inserting the data in the frame
        -  `append` inserts the data in the existing table
    progress_bar : pd.DataFrame
        Whether to use `tqdm` to log the upload progress
    **kwargs : pd.DataFrame
        Arguments passed on to `pandas_gbq.to_bgq`
    """
    with open(table_ddl_json_path) as table_ddl_file:
        table_ddl = json.load(table_ddl_file)

    table_schema = [{
            'name': x['name'],
            'type': x['field_type']
        }
        for x in table_ddl['columns']
    ]

    # Build table reference
    table_ref = f"{project}.{table_ddl['schema']}.{table_ddl['table']}"

    # Handle replace automatically
    if if_exists == 'replace':
        # Delete the object with all its data and create it again
        createTableFromJSON(
            ddl_json_config_path=table_ddl_json_path,
            project=project,
            gbq_client=gbq_client,
            if_exists='rebuild',
        )

        # Change if_exists
        if_exists = 'append'

    # Rename DataFrame columns
    df.columns = [column['name'] for column in table_schema]

    # Upload
    return pandas_gbq.to_gbq(**{
        'dataframe': df,
        'destination_table': table_ref,
        'if_exists': if_exists,
        'progress_bar': progress_bar,
        'table_schema': table_schema,
        'bigquery_client': gbq_client,
        **kwargs
    })


def deleteFromTable(
        table_ref: str, column_name: str, column_value: str, column_type: str,
        gbq_client: bigquery.Client
    ) -> None:
    """Delete data from a table filtering by a specific column value.

    Parameters
    ----------
    table_ref : str
        Table from which the data will be deleted. The value must included
        a project ID, dataset ID, and table ID, each separated by ``.``.
        For example: `your-project.your_dataset.your_table`
    column_name : str
        Name of the column that will be used to filter the data to be
        deleted
    column_value : str
        Value of col_name in the rows to be deleted
    column_type : str
        BigQuery column type (same as DDL)
    gbq_client : bigquery.Client
        Client used for making the queries
    """
    sql_query = QueryDict({
        'delete_query':
        """
        DELETE FROM `${table_ref}`
        WHERE ${column_name} = CAST('${column_value}' AS ${column_type})
        """
    })

    gbq_client.query_and_wait(
        query=sql_query['delete_query'].substitute(
            table_ref=table_ref,
            column_name=column_name,
            column_value=column_value,
            column_type=column_type,
        ),
    )


def setTableExpiration(
        table_ref: str, expiration: datetime,
        gbq_client: bigquery.Client
    ) -> None:
    """Sets an expiration date and time for a BigQuery table.

    Parameters
    ----------
    table_ref : str
        Table from which the data will be deleted. The value must included
        a project ID, dataset ID, and table ID, each separated by ``.``.
        For example: `your-project.your_dataset.your_table`
    expiration : datetime.datetime
        Date and time of the table expiration
    gbq_client : bigquery.Client
        Client used for making the queries
    """
    gbq_table = gbq_client.get_table(table_ref)
    gbq_table.expires = expiration
    gbq_table = gbq_client.update_table(gbq_table, ['expires'])


if __name__ == '__main__':
    err_msg = 'This file is only meant to be imported.'
    raise Exception(err_msg)
