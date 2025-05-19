"""Extension of the google.cloud.bigquery modules."""
from __future__ import annotations

# Default
import json
from typing import TYPE_CHECKING, Literal

# pip
import pandas_gbq
from google.cloud import bigquery
from google.cloud.exceptions import BadRequest


# Type checking imports
if TYPE_CHECKING:
    import pandas as pd


_TIME_PARTITIONING_TYPES = {
    'DAY': bigquery.TimePartitioningType.DAY,
    'HOUR': bigquery.TimePartitioningType.HOUR,
    'MONTH': bigquery.TimePartitioningType.MONTH,
    'YEAR': bigquery.TimePartitioningType.YEAR,
}


def readBigQuery(query: str, user: str, **kwargs) -> pd.DataFrame | None:
    """Read a query from a GCP BigQuery table.

    Wrapper over `pandas_gbq.read_gbq` function to send a SQL query to a
    GCP BigQuery database

    Parameters
    ----------
    query : str
        SQL query
    user : str
        Name of the user making the query
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
        **kwargs
    })


def createTableFromJSON(
        ddl_json_config_path: str,
        if_exists: Literal['raise', 'rebuild'] = 'raise'
    ) -> dict:
    """Create a BigQuery table using a JSON config file.

    Parameters
    ----------
    ddl_json_config_path : str,
        Path to the JSON file with the DDL configuration for the table.
    if_exists : {'raise', 'rebuild'}, optional
        Behavior to take if the table allready exists.
        -  `raise` raises an `Already Exists` error
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
    # Read config JSON
    with open(ddl_json_config_path) as f:
        ddl_config = json.load(f)

    # Build table location
    table_ref = '.'.join([
            ddl_config['project'],
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

    # Setup client
    gbq_client = bigquery.Client()
    # Delete the previous table and buildit all over again
    if if_exists == 'rebuild':
        gbq_client.delete_table(table_ref, not_found_ok=True)
        gbq_client.create_table(table)
    else:
        gbq_client.create_table(table, exists_ok=False)

    # Return the DDL config dict
    return ddl_config


def createTableAsSelect(
        query: str,
        table_ref: str,
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
    ctas_response = bigquery.Client().query_and_wait(
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
        df: pd.DataFrame, table_ref: str,
        if_exists: Literal['fail', 'replace', 'append'] = 'fail',
        progress_bar: bool = False,
        **kwargs
    ) -> None:
    """Add better argument docs to some parameters in `pandas_gbq.to_bgq`.

    Parameters
    ----------
    df : pd.DataFrame
        Pandas DataFrame with the data to upload into Google BigQuery
    table_ref : str
        Path to the table in the form `project.schema.table` (Case
        sensitive)
    if_exists : {'fail', 'replace', 'append'}
        Behavior to take if the table allready exists.
        -  `fail` throws an error
        -  `replace` re-builds the table inserting the data in the frame
        -  `append` inserts the data in the existing table

        .. warning:: `replace` mode its not recommended as its use will
           modify the DDL of the table.

    progress_bar : pd.DataFrame
        Whether to use `tqdm` to log the upload progress
    **kwargs : pd.DataFrame
        Arguments passed on to `pandas_gbq.to_bgq`
    """
    return pandas_gbq.to_gbq(
        dataframe=df,
        destination_table=table_ref,
        if_exists=if_exists,
        progress_bar=progress_bar,
        **kwargs
    )


if __name__ == '__main__':
    err_msg = 'This file is only meant to be imported.'
    raise Exception(err_msg)
