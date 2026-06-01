"""Download SFMC files from SFTP and upload to GCS."""

# ---------------------------------------------------------------------
# Imports
# ---------------------------------------------------------------------
import os
import time
import logging
import argparse
import subprocess
from datetime import datetime, timezone

import paramiko

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
    """Main process."""

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
        'alvi',
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

    bucket_name = 'unidata_clientes'

    bucket_path = 'CRM/carga'

    local_temp_dir = 'C:/carga'

    os.makedirs(
        local_temp_dir,
        exist_ok=True
    )

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
    # Process formatos
    # -----------------------------------------------------------------
    for formato in formatos:

        logging.info(
            '========================================'
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

        local_file = (
            f'{local_temp_dir}/'
            f'CRM_DATA_SFMC_PUBLIST_{formato}.csv'
        )

        # -------------------------------------------------------------
        # Connect SFTP
        # -------------------------------------------------------------
        logging.info(
            'Connecting SFTP'
        )

        transport = paramiko.Transport(
            (sftp_host, sftp_port)
        )

        # -------------------------------------------------------------
        # Paramiko optimization
        # -------------------------------------------------------------
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
            'SFTP connected successfully'
        )

        # -------------------------------------------------------------
        # Remote file stats
        # -------------------------------------------------------------
        logging.info(
            'Getting remote file stats'
        )

        stats = sftp.stat(remote_file)

        file_size = stats.st_size

        file_size_mb = (
            file_size / 1024 / 1024
        )

        file_size_gb = (
            file_size / 1024 / 1024 / 1024
        )

        logging.info(
            f'Remote file size: '
            f'{file_size:,} bytes | '
            f'{file_size_mb:.2f} MB | '
            f'{file_size_gb:.2f} GB'
        )

        # -------------------------------------------------------------
        # Download local file
        # -------------------------------------------------------------
        logging.info(
            f'Downloading to local file: '
            f'{local_file}'
        )

        start_time = time.time()

        sftp.get(
            remote_file,
            local_file
        )

        elapsed = (
            time.time() - start_time
        )

        speed = (
            file_size_mb / elapsed
        )

        logging.info(
            f'Download completed | '
            f'{file_size_mb:.2f} MB | '
            f'{speed:.2f} MB/s | '
            f'{elapsed / 60:.1f} min'
        )

        # -------------------------------------------------------------
        # Close SFTP
        # -------------------------------------------------------------
        sftp.close()

        transport.close()

        logging.info(
            'SFTP connection closed'
        )

        # -------------------------------------------------------------
        # Upload to GCS
        # -------------------------------------------------------------
        gcs_uri = (
            f'gs://{bucket_name}/'
            f'{bucket_path}/'
            f'CRM_DATA_SFMC_PUBLIST_{formato}.csv'
        )

        logging.info(
            f'Uploading to GCS: '
            f'{gcs_uri}'
        )

        command = [
            'gsutil',
            '-m',
            'cp',
            local_file,
            gcs_uri
        ]

        subprocess.run(
            command,
            check=True
        )

        logging.info(
            'Upload completed successfully'
        )

        # -------------------------------------------------------------
        # Remove local file
        # -------------------------------------------------------------
        logging.info(
            f'Removing local file: '
            f'{local_file}'
        )

        os.remove(local_file)

        logging.info(
            'Local file removed'
        )

    logging.info(
        '========================================'
    )

    logging.info(
        'Process finished successfully'
    )


# ---------------------------------------------------------------------
# Entrypoint
# ---------------------------------------------------------------------
if __name__ == '__main__':

    main()
