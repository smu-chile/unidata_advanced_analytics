"""Download SFMC files from SFTP and load ONLY first 100 rows to BigQuery.

Flow:
SFTP -> memory (100 rows) -> BigQuery
"""

# ---------------------------------------------------------------------
# Imports
# ---------------------------------------------------------------------
import io
import csv
import json
import logging
import datetime
from pathlib import Path

import paramiko
from google.cloud import bigquery

from common.gcp_extended import secretsmanager


# ---------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

# ---------------------------------------------------------------------
# Function convierte string a timestamp
# ---------------------------------------------------------------------
def parse_sfmc_datetime(value):
    """Convert SFMC timestamp to BigQuery DATETIME."""

    if not value:
        return None

    value = value.strip()

    try:

        parsed_date = (datetime.datetime.strptime(value,'%b %d %Y %I:%M%p'))  # noqa: DTZ007

        return parsed_date.strftime('%Y-%m-%d %H:%M:%S')

    except Exception as err:  # noqa: BLE001

        logging.warning(
            f'Error parsing timestamp [{value}] : {err}'
        )

        return None
# ---------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------
def main() -> None:
    """Main process."""

    formatos = [
        'unimarc',
        'alvi',
        'unipay',
        'm10s10'
    ]

    remote_path = '/Import/PublicationListAutomation'

    execution_date = datetime.datetime.now(datetime.UTC).strftime('%Y%m%d')

    csv_name = (
        f'PUBLICATION_LIST_AUTOMATION_'
        f'{execution_date}.csv'
    )

    remote_file = (f'{remote_path}/{csv_name}')

    project_id = ('cl-bigdata-analytics-preprod')

    # -----------------------------------------------------------------
    # Load schema JSON
    # -----------------------------------------------------------------
    json_path = (
        Path(__file__).resolve().parents[1]
        / 'gbq_objects'
        / 'CRM_DATA_SFMC_PUBLIST.json'
    )

    logging.info(f'Loading schema from {json_path}')

    with open(
        json_path,
        encoding='utf-8'
    ) as json_file:

        table_config = json.load(
            json_file
        )

    dataset_id = table_config['schema']

    base_table_name = (
        table_config['table']
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

    # -----------------------------------------------------------------
    # Secret Manager
    # -----------------------------------------------------------------
    logging.info('Getting SFTP credentials from Secret Manager')

    sftp_secret = (
        secretsmanager.getSecret(
            'salesforce_sftp_credentials',
            project=project_id
        )
    )

    # -----------------------------------------------------------------
    # BigQuery Client
    # -----------------------------------------------------------------
    logging.info('Creating BigQuery client')
    bq_client = bigquery.Client(project=project_id)

    # -----------------------------------------------------------------
    # Process formatos
    # -----------------------------------------------------------------
    for formato in formatos:

        logging.info('=' * 60)

        logging.info(f'Processing formato: {formato}')
        sftp_host = sftp_secret['host']
        sftp_port = int(sftp_secret['port'])
        sftp_user = (sftp_secret[f'user_{formato}'])
        sftp_password = (sftp_secret[f'pass_{formato}'])

        # -------------------------------------------------------------
        # Connect SFTP
        # -------------------------------------------------------------
        logging.info('Connecting to SFTP')
        transport = paramiko.Transport((sftp_host, sftp_port))

        transport.connect(
            username=sftp_user,
            password=sftp_password
        )

        sftp = (
            paramiko.SFTPClient
            .from_transport(
                transport
            )
        )

        logging.info('SFTP connection established')

        # -------------------------------------------------------------
        # Read first 100 rows
        # -------------------------------------------------------------
        with sftp.open(
            remote_file,
            'rb'
        ) as remote_stream:

            text_stream = (
                io.TextIOWrapper(
                    remote_stream,
                    encoding='utf-16'
                )
            )

            sample_lines = []

            for index, line in enumerate(
                text_stream
            ):

                sample_lines.append(
                    line
                )

                if index >= 10000:
                    break
        # -------------------------------------------------------------
        # Parse CSV
        # -------------------------------------------------------------
        sample_content = ''.join(sample_lines)

        if formato == 'm10s10':
            column_names = [
                'SubscriberID',
                'SubscriberKey',
                'AddedBy',
                'AddMethod',
                'CreatedDate',
                'DateUnsubscribed',
                'EmailAddress',
                'ListID',
                'ListName',
                'ListType',
                'Status',
                'SubscriberType'
            ]

            csv_reader = csv.reader(
                io.StringIO(sample_content)
            )

            rows = []

            for raw_row in csv_reader:

                row = {}

                for idx, column_name in enumerate(column_names):

                    row[column_name] = (
                        raw_row[idx]
                        if idx < len(raw_row)
                        else None
                    )
                rows.append(row)
        else:
            csv_reader = csv.DictReader(
                io.StringIO(sample_content)
            )

            rows = list(csv_reader)
        # -------------------------------------------------------------
        # Add FECHA_CARGA
        # -------------------------------------------------------------
        fecha_carga = datetime.date.today().isoformat()  # noqa: DTZ011

        # -------------------------------------------------------------
        # Transform rows
        # -------------------------------------------------------------
        rows_transformed = []

        for row in rows:

            row_data = {

                'SUBSCRIBERID': (
                    int(row.get('SubscriberID'))
                    if row.get('SubscriberID')
                    else None
                ),

                'SUBSCRIBERKEY': (
                    row.get('SubscriberKey')
                ),

                'ADDEDBY': (
                    int(row.get('AddedBy'))
                    if row.get('AddedBy')
                    else None
                ),

                'ADDMETHOD': (
                    row.get('AddMethod')
                ),

                'CREATEDDATE': (
                    parse_sfmc_datetime(
                        row.get('CreatedDate')
                    )
                ),

                'DATEUNSUBSCRIBED': (
                    parse_sfmc_datetime(
                        row.get('DateUnsubscribed')
                    )
                ),

                'EMAILADDRESS': (
                    row.get('EmailAddress')
                ),

                'LISTID': (
                    int(row.get('ListID'))
                    if row.get('ListID')
                    else None
                ),

                'LISTNAME': (
                    row.get('ListName')
                ),

                'LISTTYPE': (
                    row.get('ListType')
                ),

                'STATUS': (
                    row.get('Status')
                ),

                'SUBSCRIBERTYPE': (
                    row.get('SubscriberType')
                ),

                'FECHA_CARGA': (
                    fecha_carga
                )
            }

            rows_transformed.append(
                row_data
            )

        # -------------------------------------------------------------
        # Remove invalid rows
        # -------------------------------------------------------------
        rows_transformed = [
            row
            for row in rows_transformed
            if row['SUBSCRIBERID'] is not None
        ]

        logging.info(
            f'Rows after validation: '
            f'{len(rows_transformed)}'
        )
        # -------------------------------------------------------------
        # Table name
        # -------------------------------------------------------------
        table_name = (
            f'{base_table_name}_'
            f'{formato.upper()}_TEST'
        )

        table_id = (
            f'{project_id}.'
            f'{dataset_id}.'
            f'{table_name}'
        )
        logging.info(f'Target table: {table_id}')

        # -------------------------------------------------------------
        # Create table if not exists
        # -------------------------------------------------------------
        try:

            bq_client.get_table(table_id)

            logging.info('Table already exists')

        except Exception:  # noqa: BLE001

            logging.info('Creating table')

            table = (
                bigquery.Table(
                    table_id,
                    schema=bq_schema
                )
            )

            bq_client.create_table(table)

        # -------------------------------------------------------------
        # Load to BigQuery
        # -------------------------------------------------------------
        job_config = (
            bigquery.LoadJobConfig(
                schema=bq_schema,
                write_disposition=(
                    bigquery.WriteDisposition
                    .WRITE_TRUNCATE
                )
            )
        )

        load_job = (
            bq_client.load_table_from_json(
                rows_transformed,
                table_id,
                job_config=job_config
            )
        )

        load_job.result()
        # -------------------------------------------------------------
        # Close SFTP
        # -------------------------------------------------------------
        sftp.close()
        transport.close()
        logging.info('SFTP connection closed')

    logging.info('=' * 60)

    logging.info('PROCESS COMPLETED SUCCESSFULLY')
# ---------------------------------------------------------------------
if __name__ == '__main__':
    main()
