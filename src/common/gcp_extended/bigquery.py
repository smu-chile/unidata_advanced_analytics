"""Extension of the google.cloud.bigquery modules."""
from __future__ import annotations

# Default
import os
import json
import posixpath
from uuid import uuid4
from typing import Literal

# pip
import pandas as pd
import pyarrow as pa
from pendulum import DateTime
from google.cloud import storage, bigquery, bigquery_storage
from google.cloud.exceptions import NotFound, BadRequest

# Own
from ..databases.queries import QueryDict  # noqa: TID252


_TIME_PARTITIONING_TYPES = {
    'DAY': bigquery.TimePartitioningType.DAY,
    'HOUR': bigquery.TimePartitioningType.HOUR,
    'MONTH': bigquery.TimePartitioningType.MONTH,
    'YEAR': bigquery.TimePartitioningType.YEAR,
}

_GBQ_TO_PYARROW_DTYPES = {
    ('BOOL', 'BOOLEAN'): pa.bool_(),
    ('BYTES'): pa.binary(),
    ('DATE'): pa.date32(),
    ('DATETIME'): pa.timestamp('us'),
    # TODO(ecastrot): Add Interval
    ('INT64', 'INTEGER', 'INT', 'SMALLINT', 'BIGINT', 'TINYINT', 'BYTEINT'): pa.int64(),
    ('NUMERIC', 'DECIMAL'): pa.decimal128(38, 9),
    ('BIGNUMERIC', 'BIGDECIMAL'): pa.decimal256(76, 38),
    ('FLOAT', 'FLOAT64'): pa.float64(),
    # TODO(ecastrot): Add Range
    ('STRING'): pa.string(),
    # TODO(ecastrot): Add Struct
    ('TIME'): pa.time64('us'),
    ('TIMESTAMP'): pa.timestamp('us', tz='UTC'),
}


def readBigQuery(
        query: str, user: str, gbq_client: bigquery.Client,
        **kwargs
    ) -> pd.DataFrame | None:
    """Read a query from a GCP BigQuery table.

    Sends a SQL query to a GCP BigQuery database and bring the resultant
    table as a DataFrame

    Parameters
    ----------
    query : str
        SQL query
    user : str
        Name of the user making the query
    gbq_client : bigquery.Client
        Client used for making the queries
    **kwargs
        Arguments passed to the `bigquery.QueryJobConfig` constructor.
        **This argument will override the user argument**, so you'll need
        to add a user label manually.

    Returns
    -------
    response_df : pd.DataFrame | None
        Pandas DataFrame object with the query response
    """
    return gbq_client.query_and_wait(
        query=query,
        job_config=bigquery.QueryJobConfig(**{
            'labels': {'user': user},
            **kwargs
        }),
    ).to_dataframe(
        bqstorage_client=bigquery_storage.BigQueryReadClient(),
        progress_bar_type=None,
    )


def createTableFromJSON(
        table_ddl_json_path: str,
        project: str,
        gbq_client: bigquery.Client,
        if_exists: Literal['raise', 'ignore', 'rebuild'] = 'raise',
        json_encoding: str = 'utf8',
    ) -> dict:
    """Create a BigQuery table using a JSON config file.

    Parameters
    ----------
    table_ddl_json_path : str,
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
    json_encoding : str, default='utf8'
        Encoding of the JSON file with the DDL

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
    with open(table_ddl_json_path, encoding=json_encoding) as f:
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


def verifyTableExistence(
        table_ref: str,
        gbq_client: bigquery.Client,
        if_not_exists: Literal['raise', 'ignore'] = 'raise',
    ) -> bool:
    """Verify table existence.

    Returns True if the table exists. If it doesn't the behavior depends on
    the value of `if_not_exists`.

    Parameters
    ----------
    table_ref : str
        Table from which the data will be deleted. The value must included
        a project ID, dataset ID, and table ID, each separated by ``.``.
        For example: `your-project.your_dataset.your_table`
    gbq_client : bigquery.Client
        Client used for making the queries
    if_not_exists : ['raise', 'ignore'], default='raise'
        Behavior if table does not exist:
        - `ignore`: Return False if the table does not exist
        - `raise`: Raises NotFound exception

    Returns
    -------
    table_exists : bool
        True when table exists and False when it doesn't and the behavior
        is ignore
    """
    # Search the table
    try:
        gbq_client.get_table(table_ref)  # Make an API request.
        return True
    except NotFound:
        if if_not_exists == 'ignore': return False
        raise


def uploadFrame(
        df: pd.DataFrame,
        table_ddl_json_path: str,
        project: str,
        gbq_client: bigquery.Client,
        if_exists: Literal['fail', 'replace', 'append'] = 'fail',
        json_encoding: str = 'utf8',
    ) -> None:
    """Upload a Pandas DataFrame to a table in Google BigQuery.

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
    json_encoding : str, default='utf8'
        Encoding of the JSON file with the DDL
    **kwargs : pd.DataFrame
        Arguments passed on to `pandas_gbq.to_bgq`
    """
    with open(table_ddl_json_path, encoding=json_encoding) as table_ddl_file:
        table_ddl = json.load(table_ddl_file)

    table_schema = [{
            'name': x['name'],
            'type': x['field_type']
        }
        for x in table_ddl['columns']
    ]

    # Build table reference
    table_ref = f"{project}.{table_ddl['schema']}.{table_ddl['table']}"

    # If table does not exists then create it first
    if not verifyTableExistence(
        table_ref=table_ref, gbq_client=gbq_client, if_not_exists='ignore',
    ):
        createTableFromJSON(
            table_ddl_json_path=table_ddl_json_path,
            project=project,
            gbq_client=gbq_client,
            if_exists='rebuild',
            json_encoding=json_encoding,
        )

    # Handle replace automatically
    if if_exists == 'replace':
        # Delete the object with all its data and create it again
        createTableFromJSON(
            table_ddl_json_path=table_ddl_json_path,
            project=project,
            gbq_client=gbq_client,
            if_exists='rebuild',
            json_encoding=json_encoding,
        )

        # Change if_exists
        if_exists = 'append'

    # Rename DataFrame columns
    df.columns = [column['name'] for column in table_schema]

    uuid_id = uuid4()
    # Save dataframe to local storage
    tmp_local_filename = f'tmp_df-{uuid_id}.parquet'
    # Change types from the frame to pyarrow
    df.astype(
        pyArrowDTypesFromJSON(table_ddl_json_path)
    # Save to local as parquet
    ).to_parquet(
        tmp_local_filename,
        engine='pyarrow',
        schema=_pyArrowSchemaFromJSON(table_ddl_json_path)
    )

    # Upload file to GCS
    tmp_gcs_filename = posixpath.join('uploads', tmp_local_filename)
    gcs_tmp_file = storage.Client().bucket(
        f"cl-bigdata-analytics-{project.split('-')[-1]}-us-sandbox-temporary"
    ).blob(
        # Path of the file in GCS
        tmp_gcs_filename
    )
    gcs_tmp_file.upload_from_filename(
        # Path to localfile
        tmp_local_filename
    )

    # Load the data to the table from the GCS file
    gbq_client.load_table_from_uri(
        source_uris=f'gs://cl-bigdata-analytics-{project.split('-')[-1]}-us-sandbox-temporary/{tmp_gcs_filename}',
        destination=(
            project
            + '.' + table_ddl['schema']
            + '.' + table_ddl['table']
        ),
        job_config=bigquery.LoadJobConfig(
            source_format=bigquery.SourceFormat.PARQUET,
            write_disposition=bigquery.WriteDisposition.WRITE_APPEND
        )
    ).result()

    # Remove tempral files from local and GCS
    os.remove(tmp_local_filename)
    gcs_tmp_file.delete()


def deleteFromTable(
        table_ref: str, where_clause: str, gbq_client: bigquery.Client,
        if_not_exists: Literal['raise', 'ignore'] = 'ignore',
        project: str =  '', json_encoding: str = 'utf8'
    ) -> None:
    """Delete data from a table filtering by a specific column value.

    Parameters
    ----------
    table_ref : str
        Table from which the data will be deleted. It must be either:
          - Path to the JSON DDL of the table, in which case project must
            be also provided.
          - Table ref ID that includes project ID, dataset ID, and table
            ID, each separated by ``.``. For example:
            `your-project.your_dataset.your_table`
    where_clause : str
        Comparisons inside the WHERE clause for the columns to be deleted.
        For example: `column_a = date('1998-08-30')`
    gbq_client : bigquery.Client
        Client used for making the queries
    if_not_exists: ['raise', 'ignore'], default='raise'
        Behavior to take if the table does not exist.
    project : str
        Project ID in which the table is located. Unused if the table ref
        ID is given.
    json_encoding : str, default='utf8'
        Encoding of the JSON file with the DDL. Only used if table_ref is a
        path.

    See Also
    --------
    :func:`~verifyTableExistence`
    """
    # If a table ref is a path to the json ddl, extract from there
    if os.path.isfile(table_ref):
        with open(table_ref, encoding=json_encoding) as f:
            ddl_config = json.load(f)
        table_ref = (
            project + '.'
            + ddl_config['schema'] + '.'
            + ddl_config['table']
        )

    # Build default delete query job
    sql_query = QueryDict({
        'delete_query':
        """
        DELETE FROM `${table_ref}`
        WHERE ${where_clause}
        """
    })

    # Send deletion job
    if verifyTableExistence(
        table_ref=table_ref,
        gbq_client=gbq_client,
        if_not_exists=if_not_exists,
    ):
        gbq_client.query_and_wait(
            query=sql_query['delete_query'].substitute(
                table_ref=table_ref,
                where_clause=where_clause,
            ),
        )


def setTableExpiration(
        table_ref: str,
        expiration: DateTime | int,
        gbq_client: bigquery.Client,
        partition_colum_name: str = '',
    ) -> None:
    """Set an expiration BigQuery table or partition.

    When ``partition_column_name`` is not given then ``expiration`` must be
    a datetime in which the whole table will expire. If given, then
    ``expiration`` must be the number of miliseconds in which the
    partitions will expire.

    Parameters
    ----------
    table_ref : str
        Table from which the data will be deleted. The value must included
        a project ID, dataset ID, and table ID, each separated by ``.``.
        For example: `your-project.your_dataset.your_table`
    expiration : pendulum.DateTime
        Datetime in which the whole table will expire or number of
        miliseconds in which the partitions must be deleted
    gbq_client : bigquery.Client
        Client used for making the queries
    partition_colum_name : str, optional
        Name of the column in which the partition is located
    """
    gbq_table = gbq_client.get_table(table_ref)
    # Partition column name is not given, so expiration applies to the
    # whole table
    if (
        isinstance(expiration, DateTime)
        and (not partition_colum_name)
    ):
        gbq_table.expires = expiration
        gbq_table = gbq_client.update_table(gbq_table, ['expires'])
    # Partition column name is given, so expiration applies only to the
    # partition
    elif (
        isinstance(expiration, int)
        and partition_colum_name
    ):
        gbq_table.time_partitioning.expiration_ms = expiration
        gbq_table = gbq_client.update_table(gbq_table, ['time_partitioning'])
    else:
        err_msg = (
            'You pass a wrong combination of expiration type and '
            'partition_colum_name value.'
        )
        raise ValueError(err_msg)


def _pyArrowSchemaFromJSON(table_ddl_json_path: str) -> pa.Schema:
    """Builds a pyarrow schema from the BigQuery DDL JSON

    The schema created by this function is usefull when transforming a
    frame to parquet. When a parquet file is uploaded to BigQuery it will
    fail if the file does not contain the metadata of the nullability of
    the columns.

    Parameters
    ----------
    table_ddl_json_path : str,
        Path to the JSON file with the DDL configuration for the table.

    Returns
    ----------
    parquet_schema : pa.Schema
        PyArrow schema with the information of the column names, dtypes and
        if the column is nullable or not
    """
    # Get the DDL of the columns
    with open(table_ddl_json_path) as table_ddl_file:
        table_columns: dict[str, str] = json.load(table_ddl_file)['columns']
    # Build the schema
    return pa.schema([
        (
            # Name of the column
            col['name'],
            # Column dtype
            next(
                v
                for k, v
                in _GBQ_TO_PYARROW_DTYPES.items()
                if col['field_type'].upper() in k
            ),
            # Column is nullable?
            col.get('mode', 'NULLABLE').upper() != 'REQUIRED'
        )
        for col in table_columns
    ])


def pyArrowDTypesFromJSON(table_ddl_json_path: str) -> dict[str, str]:
    """Builds a dictionary with columns and its pyarrow dtype from the DDL

    Usses the JSON that contains the DDL of the table to build a dictionary
    with the column names and they correspondency in pyarrow dtypes

    Parameters
    ----------
    table_ddl_json_path : str,
        Path to the JSON file with the DDL configuration for the table.

    Returns
    -------
    compatibility_dict : dict[str, str]
        Dictionary with the new dtypes in the format
        ``{'column_name': 'dtype[pyarrow]'}``
    """
    # Get the DDL
    with open(table_ddl_json_path) as table_ddl_file:
        table_ddl = json.load(table_ddl_file)
    # Build a dictionary with column_name: pyarrow dtype
    return {
        col['name']: next(
            pd.ArrowDtype(v)
            for k, v
            in _GBQ_TO_PYARROW_DTYPES.items()
            if col['field_type'].upper() in k
        )
        for col in table_ddl['columns']
    }


if __name__ == '__main__':
    err_msg = 'This file is only meant to be imported.'
    raise Exception(err_msg)
