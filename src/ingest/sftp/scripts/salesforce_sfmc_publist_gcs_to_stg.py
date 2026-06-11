"""Load SFMC Publication List files from GCS to STG BigQuery table.
Flow: GCS -> STG.
"""  # noqa: D205
# ---------------------------------------------------------------------
# Imports
# ---------------------------------------------------------------------
import json
import logging
import argparse

from google.cloud import bigquery


# ---------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s')

parser = argparse.ArgumentParser()
parser.add_argument('--project_id', required=True)
parser.add_argument('--execution_date', required=True)
parser.add_argument('--schema_file', required=True)

# ---------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------
def main() -> None:  # noqa: D103

    args = vars(parser.parse_args())
    project_id = args['project_id']
    execution_date = args['execution_date']
    schema_file = args['schema_file']

    bucket_name = ('cl-bigdata-analytics-prod-us-sandbox-datasets')
    bucket_path = 'CRM'

    formatos = ['UNIMARC', 'ALVI', 'UNIPAY', 'M10S10']
    # -------------------------------------------------------------
    # Load STG schema
    # -------------------------------------------------------------
    json_path = f'gbq_objects/{schema_file}'
    logging.info(f'Loading schema: {json_path}')

    with open(
        json_path,
        encoding='utf-8'
    ) as json_file:

        table_config = json.load(
            json_file
        )

    dataset_id = table_config['schema']
    table_name = table_config['table']
    table_id = (
        f'{project_id}.'
        f'{dataset_id}.'
        f'{table_name}'
    )

    bq_schema = []

    for column in table_config['columns']:

        bq_schema.append(  # noqa: PERF401
            bigquery.SchemaField(
                column['name'],
                column['field_type'],
                mode=column['mode']
            )
        )

    # -------------------------------------------------------------
    # BigQuery client
    # -------------------------------------------------------------
    bq_client = bigquery.Client(project=project_id)

    # -------------------------------------------------------------
    # Create table if not exists
    # -------------------------------------------------------------
    try:

        bq_client.get_table(table_id)

        logging.info('STG table already exists')

    except Exception:  # noqa: BLE001

        logging.info('Creating STG table')

        table = bigquery.Table(table_id, schema=bq_schema)

        bq_client.create_table(table)

    # -------------------------------------------------------------
    # Truncate STG
    # -------------------------------------------------------------
    logging.info(f'Truncating {table_id}')

    truncate_sql = f"""TRUNCATE TABLE `{table_id}`"""

    bq_client.query(truncate_sql).result()

    # -------------------------------------------------------------
    # Load files
    # -------------------------------------------------------------
    for formato in formatos:

        source_uri = (
            f'gs://{bucket_name}/'
            f'{bucket_path}/'
            f'CRM_DATA_SFMC_PUBLIST_'
            f'{formato}_'
            f'{execution_date}.csv'
        )

        logging.info(f'Loading file: {source_uri}')

        if formato == 'M10S10':  # noqa: SIM108
            skip_rows = 0
        else:
            skip_rows = 1

        job_config = (
            bigquery.LoadJobConfig(
                schema=bq_schema,
                source_format=(
                    bigquery.SourceFormat.CSV
                ),
                skip_leading_rows=(
                    skip_rows
                ),
                write_disposition=(
                    bigquery.WriteDisposition
                    .WRITE_APPEND
                ),
                field_delimiter=',',
                allow_quoted_newlines=True
            )
        )

        load_job = (
            bq_client.load_table_from_uri(
                source_uri,
                table_id,
                job_config=job_config
            )
        )

        load_job.result()
        logging.info(f'{formato} loaded successfully')

    logging.info('STG LOAD COMPLETED')
# ---------------------------------------------------------------------
if __name__ == '__main__':
    main()
