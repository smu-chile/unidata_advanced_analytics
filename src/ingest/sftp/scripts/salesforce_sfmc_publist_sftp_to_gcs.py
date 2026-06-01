"""Download SFMC files from SFTP and upload directly to GCS.

Flow:
SFTP -> GCS
"""

# ---------------------------------------------------------------------
# Imports
# ---------------------------------------------------------------------
import logging
import argparse
from datetime import datetime, timezone

import paramiko
from google.cloud import storage

# GCP / Common
import common.gcp_extended.secretsmanager as secretmanager


# ---------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

# ---------------------------------------------------------------------
# Arguments
# ---------------------------------------------------------------------
parser = argparse.ArgumentParser()

parser.add_argument(
    '--project_id',
    type=str,
    required=True,
    help='GCP project id'
)

parser.add_argument(
    '--execution_date',
    type=str,
    help='Execution date YYYYMMDD'
)


# ---------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------
def main() -> None:
    """Main process."""  # noqa: D401
    # -----------------------------------------------------------------
    # Parse args
    # -----------------------------------------------------------------
    args = vars(parser.parse_args())

    gcp_project_id = args['project_id']

    execution_date = args['execution_date']

    if execution_date is None:

        execution_date = datetime.now(
            timezone.UTC
        ).strftime('%Y%m%d')

    # -----------------------------------------------------------------
    # Config
    # -----------------------------------------------------------------
    formatos = [
        'unimarc',
        'alvi',
        'unipay',
        'm10s10'
    ]

    remote_path = (
        '/Import/PublicationListAutomation'
    )

    csv_name = (
        f'PUBLICATION_LIST_AUTOMATION_'
        f'{execution_date}.csv'
    )

    remote_file = (
        f'{remote_path}/{csv_name}'
    )

    bucket_name = 'cl-bigdata-analytics-preprod-us-sandbox-datasets'

    bucket_path = 'CRM/'

    # -----------------------------------------------------------------
    # Get SFTP credentials
    # -----------------------------------------------------------------
    logging.info(
        'Getting SFTP credentials from Secret Manager'
    )

    sftp_secret = secretmanager.getSecret(
        'salesforce_sftp_credentials',
        project=gcp_project_id
    )

    # -----------------------------------------------------------------
    # GCS Client
    # -----------------------------------------------------------------
    logging.info(
        'Creating GCS client'
    )

    storage_client = storage.Client()

    bucket = storage_client.bucket(
        bucket_name
    )

    # -----------------------------------------------------------------
    # Process formatos
    # -----------------------------------------------------------------
    for formato in formatos:

        logging.info(
            '===================================================='
        )

        logging.info(
            f'Processing formato: {formato}'
        )

        sftp_host = sftp_secret['host']

        sftp_port = int(
            sftp_secret['port']
        )

        sftp_user = (
            sftp_secret[f'user_{formato}']
        )

        sftp_password = (
            sftp_secret[f'pass_{formato}']
        )

        # -------------------------------------------------------------
        # Connect SFTP
        # -------------------------------------------------------------
        logging.info(
            'Connecting to SFTP'
        )

        transport = paramiko.Transport(
            (sftp_host, sftp_port)
        )

        transport.window_size = 2147483647

        transport.packetizer.REKEY_BYTES = pow(2, 40)

        transport.packetizer.REKEY_PACKETS = pow(2, 40)

        transport.connect(
            username=sftp_user,
            password=sftp_password
        )

        sftp = (
            paramiko.SFTPClient
            .from_transport(transport)
        )

        logging.info(
            'SFTP connection established'
        )

        # -------------------------------------------------------------
        # File stats
        # -------------------------------------------------------------
        stats = sftp.stat(
            remote_file
        )

        file_size = stats.st_size

        logging.info(
            f'Remote file size: '
            f'{file_size / 1024 / 1024 / 1024:.2f} GB'
        )

        # -------------------------------------------------------------
        # Destination blob
        # -------------------------------------------------------------
        destination_blob = (
            f'{bucket_path}/'
            f'CRM_DATA_SFMC_PUBLIST_{formato}.csv'
        )

        logging.info(
            f'Uploading to: '
            f'gs://{bucket_name}/{destination_blob}'
        )

        blob = bucket.blob(
            destination_blob
        )

        # -------------------------------------------------------------
        # Stream SFTP -> GCS
        # -------------------------------------------------------------
        with sftp.open(
            remote_file,
            'rb'
        ) as remote_stream:

            blob.upload_from_file(
                remote_stream,
                rewind=True,
                timeout=7200
            )

        logging.info(
            'Upload completed successfully'
        )

        # -------------------------------------------------------------
        # Close connections
        # -------------------------------------------------------------
        sftp.close()

        transport.close()

        logging.info(
            'SFTP connection closed'
        )

    # -----------------------------------------------------------------
    # End
    # -----------------------------------------------------------------
    logging.info(
        '===================================================='
    )

    logging.info(
        'Process completed successfully'
    )


# ---------------------------------------------------------------------
# Entrypoint
# ---------------------------------------------------------------------
if __name__ == '__main__':
    main()  # noqa: W292