"""Download complete SFMC files from SFTP and upload to GCS.
Flow:
SFTP -> archivo temporal local -> GCS
"""

# ---------------------------------------------------------------------
# Imports
# ---------------------------------------------------------------------
import time
import logging
import datetime
import tempfile
from pathlib import Path

import paramiko
from google.cloud import storage

from common.gcp_extended import secretsmanager


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
    formatos = ['alvi']

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
        # Download full file from SFTP
        # -------------------------------------------------------------
        start_time = time.time()

        logging.info(f'Downloading full file: {remote_file}')

        file_info = sftp.stat(remote_file)

        remote_size_mb = (file_info.st_size / 1024 / 1024)

        logging.info(
            f'Remote file size: '
            f'{remote_size_mb:.2f} MB')

        local_tmp_file = (
            Path(tempfile.gettempdir())
            / f'{formato}_{csv_name}')

        logging.info(f'Local temp file: {local_tmp_file}')

        start_download = datetime.datetime.now()  # noqa: DTZ005

        sftp.get(
            remote_file,
            str(local_tmp_file)
        )

        download_seconds = (
            datetime.datetime.now() - start_download).total_seconds()  # noqa: DTZ005

        logging.info(
            f'Download time: '
            f'{download_seconds:.2f} seconds')

        logging.info(
            f'Average speed: '
            f'{remote_size_mb / download_seconds:.2f} MB/s'
        )

        elapsed_time = (time.time() - start_time)

        file_size_mb = (
            Path(local_tmp_file).stat().st_size
            / 1024
            / 1024
        )
        logging.info(
            f'File downloaded successfully. '
            f'Size: {file_size_mb:.2f} MB')
        logging.info(
            f'Download time: '
            f'{elapsed_time:.2f} seconds')
        logging.info(
            f'Average speed: '
            f'{file_size_mb / elapsed_time:.2f} MB/s')

        # -------------------------------------------------------------
        # Upload complete file to GCS
        # -------------------------------------------------------------
        destination_blob = (
            f'{bucket_path}/'
            f'CRM_DATA_SFMC_PUBLIST_'
            f'{formato.upper()}_'
            f'{execution_date}.csv'
        )

        logging.info(
            f'Uploading file to '
            f'gs://{bucket_name}/{destination_blob}'
        )

        blob = bucket.blob(destination_blob)
        blob.upload_from_filename(local_tmp_file)

        logging.info('File uploaded successfully to GCS')

        # -------------------------------------------------------------
        # Remove temp file
        # -------------------------------------------------------------
        Path(local_tmp_file).unlink(missing_ok=True)
        logging.info('Temporary file removed')

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
