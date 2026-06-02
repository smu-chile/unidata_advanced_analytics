"""Download SFMC files from SFTP and upload ONLY first 100 lines to GCS.

Flow:
SFTP -> memory (100 lines) -> GCS
"""

# ---------------------------------------------------------------------
# Imports
# ---------------------------------------------------------------------
import io  # noqa: F401
import logging
import datetime

import paramiko
from google.cloud import storage

from common.gcp_extended import secretsmanager  # noqa: F401


# ---------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

# ---------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------
def main() -> None:
    """Main process."""

    # -----------------------------------------------------------------
    # Config
    # -----------------------------------------------------------------
    formatos = [
        'unimarc',
        'alvi',
        'unipay',
        'm10s10'
    ]

    remote_path = '/Import/PublicationListAutomation'

    execution_date = datetime.datetime.now(datetime.UTC).strftime('%Y%m%d')

    csv_name = f'PUBLICATION_LIST_AUTOMATION_{execution_date}.csv'

    remote_file = f'{remote_path}/{csv_name}'

    bucket_name = 'cl-bigdata-analytics-preprod-us-sandbox-datasets'
    bucket_path = 'CRM'

    # -----------------------------------------------------------------
    # Secrets
    # -----------------------------------------------------------------
    logging.info('Getting SFTP credentials from Secret Manager')

    sftp_secret = secretsmanager.getSecret(
        'salesforce_sftp_credentials',
        project='cl-bigdata-analytics-preprod'
    )

    # -----------------------------------------------------------------
    # GCS client
    # -----------------------------------------------------------------
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)

    # -----------------------------------------------------------------
    # Process formatos
    # -----------------------------------------------------------------
    for formato in formatos:

        logging.info('=' * 60)
        logging.info(f'Processing formato: {formato}')

        sftp_host = sftp_secret['host']
        sftp_port = int(sftp_secret['port'])
        sftp_user = sftp_secret[f'user_{formato}']
        sftp_password = sftp_secret[f'pass_{formato}']

        # -------------------------------------------------------------
        # Connect SFTP
        # -------------------------------------------------------------
        logging.info('Connecting to SFTP')

        transport = paramiko.Transport((sftp_host, sftp_port))
        transport.connect(username=sftp_user, password=sftp_password)

        sftp = paramiko.SFTPClient.from_transport(transport)

        logging.info('SFTP connection established')

        # -------------------------------------------------------------
        # Read ONLY first 100 lines (NO DATAFRAME)
        # -------------------------------------------------------------
        logging.info(
            'Reading first 100 lines from remote file'
        )

        with sftp.open(remote_file, 'rb') as remote_stream:

            text_stream = io.TextIOWrapper(
                remote_stream,
                encoding='utf-16'
            )

            sample_lines = []

            for i, line in enumerate(text_stream):

                sample_lines.append(line)

                if i >= 99:
                    break

        logging.info(
            f'Lines read: {len(sample_lines)}'
        )

        # -------------------------------------------------------------
        # Upload sample to GCS
        # -------------------------------------------------------------
        sample_content = ''.join(
            sample_lines
        )

        destination_blob = (
            f'{bucket_path}/'
            f'CRM_DATA_SFMC_PUBLIST_{formato}_sample.csv'
        )

        logging.info(
            f'Uploading sample file to '
            f'gs://{bucket_name}/{destination_blob}'
        )

        blob = bucket.blob(
            destination_blob
        )

        blob.upload_from_string(
            sample_content,
            content_type='text/csv'
        )

        logging.info(
            'Sample upload completed successfully'
        )

        # -------------------------------------------------------------
        # Close connections
        # -------------------------------------------------------------
        sftp.close()
        transport.close()

        logging.info('SFTP connection closed')

    logging.info('=' * 60)
    logging.info('TEST PROCESS COMPLETED SUCCESSFULLY')


# ---------------------------------------------------------------------
if __name__ == '__main__':
    main()
