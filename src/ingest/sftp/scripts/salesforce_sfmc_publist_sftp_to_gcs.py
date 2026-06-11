"""Download complete SFMC files from SFTP and upload to GCS.
Flow:
SFTP -> archivo temporal local -> GCS
"""

# ---------------------------------------------------------------------
# Imports
# ---------------------------------------------------------------------
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
    formatos = ['unimarc', 'alvi', 'unipay', 'm10s10']
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

    # -------------------------------------------------------------
    # Clean bucket folder
    # -------------------------------------------------------------
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blobs = bucket.list_blobs(prefix=f'{bucket_path}/')
    logging.info(f'Cleaning gs://{bucket_name}/{bucket_path}/')

    for blob in blobs:
        if blob.name.startswith(
            f'{bucket_path}/CRM_DATA_SFMC_PUBLIST_'):
            blob.delete()

        logging.info('Bucket folder cleaned')

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
        logging.info(f'Downloading: {remote_file}')
        file_info = sftp.stat(remote_file)
        remote_size_mb = (file_info.st_size / 1024 / 1024)

        logging.info(f'Remote file size: {remote_size_mb:.2f} MB')

        local_tmp_file = (
            Path(tempfile.gettempdir())
            / f'{formato}_{csv_name}')

        logging.info(f'Local temp file: {local_tmp_file}')
        sftp.get(remote_file, str(local_tmp_file))

        # -------------------------------------------------------------
        # Convert UTF-16 -> UTF-8
        # -------------------------------------------------------------
        logging.info('Converting file from UTF-16 to UTF-8')

        utf8_file = (
            Path(tempfile.gettempdir())
            / f'{formato}_{csv_name}_utf8.csv'
        )

        with open(local_tmp_file, 'r', encoding='utf-16') as src:  # noqa: UP015
            content = src.read()

        with open(utf8_file, 'w', encoding='utf-8', newline='') as dst:
            dst.write(content)

        logging.info(f'UTF-8 file created: {utf8_file}')

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
        blob.upload_from_filename(str(utf8_file))
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
