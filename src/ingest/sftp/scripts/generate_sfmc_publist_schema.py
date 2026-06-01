"""Generate BigQuery JSON schema from SFMC Publication List CSV.

Flow:
SFTP -> Read sample -> Infer schema -> Generate JSON
"""

# ---------------------------------------------------------------------
# Imports
# ---------------------------------------------------------------------
import io
import json
import logging
import argparse
from datetime import datetime, timezone

import pandas as pd
import paramiko

# GCP / Common
import common.gcp_extended.secretsmanager as secretmanager


# ---------------------------------------------------------------------
# Logging config
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
    help='GCP project id'
)

parser.add_argument(
    '--execution_date',
    type=str,
    help='Execution date YYYYMMDD'
)


# ---------------------------------------------------------------------
# Infer BigQuery type
# ---------------------------------------------------------------------
def infer_bq_type(series: pd.Series) -> str:
    """Infer BigQuery datatype from pandas dataframe column."""
    non_null_series = series.dropna()

    if non_null_series.empty:
        return 'STRING'

    try:
        pd.to_numeric(non_null_series)

        if (
            non_null_series.astype(str)
            .str.contains(r'\.')
            .any()
        ):
            return 'FLOAT64'

        return 'INT64'

    except ValueError:
        pass

    try:
        pd.to_datetime(non_null_series.iloc[:10])

        return 'TIMESTAMP'

    except ValueError:
        pass

    return 'STRING'


# ---------------------------------------------------------------------
# Normalize columns
# ---------------------------------------------------------------------
def normalize_columns(df_sf: pd.DataFrame) -> pd.DataFrame:
    """Normalize dataframe columns."""
    df_sf.columns = [
        col.strip()
        .replace('\ufeff', '')
        .replace('"', '')
        .upper()
        .replace(' ', '_')
        .replace('-', '_')
        for col in df_sf.columns
    ]

    return df_sf


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
    # Get SFTP credentials
    # -----------------------------------------------------------------
    logging.info('Getting SFTP credentials from Secret Manager')

    sftp_secret = secretmanager.getSecret(
        'salesforce_sftp_credentials',
        project=gcp_project_id
    )

    formatos = [
        'unimarc',
        'alvi',
        'unipay',
        'm10s10'
    ]

    dfs = []

    # -----------------------------------------------------------------
    # Common file config
    # -----------------------------------------------------------------
    remote_path = '/Import/PublicationListAutomation'

    csv_name = (
        f'PUBLICATION_LIST_AUTOMATION_{execution_date}.csv'
    )

    remote_file = f'{remote_path}/{csv_name}'

    output_json = 'CRM_DATA_SFMC_PUBLIST.json'

    sample_output = 'SAMPLE_CONSOLIDADO.csv'

    # -----------------------------------------------------------------
    # Expected columns
    # -----------------------------------------------------------------
    expected_columns = []

    # -----------------------------------------------------------------
    # Process formats
    # -----------------------------------------------------------------
    for formato in formatos:

        logging.info(
            f'Processing formato: {formato}'
        )

        # -------------------------------------------------------------
        # Credentials
        # -------------------------------------------------------------
        sftp_host = sftp_secret['host']

        sftp_port = int(
            sftp_secret['port']
        )

        sftp_user = sftp_secret[
            f'user_{formato}'
        ]

        sftp_password = sftp_secret[
            f'pass_{formato}'
        ]

        # -------------------------------------------------------------
        # Connect SFTP
        # -------------------------------------------------------------
        transport = paramiko.Transport(
            (sftp_host, sftp_port)
        )

        transport.connect(
            username=sftp_user,
            password=sftp_password
        )

        sftp = paramiko.SFTPClient.from_transport(
            transport
        )

        # -------------------------------------------------------------
        # Read sample
        # -------------------------------------------------------------
        remote_csv = sftp.open(
            remote_file,
            'rb'
        )

        sample_bytes = remote_csv.read(
            500000
        )

        decoded_text = sample_bytes.decode(
            'utf-16',
            errors='ignore'
        )

        df_sf = pd.read_csv(
            io.StringIO(decoded_text),
            sep=',',
            nrows=10,
            quotechar='"',
            skip_blank_lines=True
        )

        # -------------------------------------------------------------
        # Normalize columns
        # -------------------------------------------------------------
        df_sf = normalize_columns(df_sf)

        # -------------------------------------------------------------
        # Use unimarc as official structure
        # -------------------------------------------------------------
        if formato == 'unimarc':

            expected_columns = list(df_sf.columns)

        else:

            df_sf.columns = expected_columns

        # -------------------------------------------------------------
        # Keep only expected columns
        # -------------------------------------------------------------
        df_sf = df_sf[expected_columns]

        # -------------------------------------------------------------
        # Add formato
        # -------------------------------------------------------------
        df_sf['FORMATO'] = formato

        # -------------------------------------------------------------
        # Save dataframe
        # -------------------------------------------------------------
        dfs.append(df_sf)

        # -------------------------------------------------------------
        # Generate JSON ONLY using unimarc
        # -------------------------------------------------------------
        if formato == 'unimarc':

            logging.info(
                'Generating BigQuery schema'
            )

            fields = []

            for column in df_sf.columns:

                bq_type = infer_bq_type(
                    df_sf[column]
                )

                field = {
                    'name': column,
                    'type': bq_type,
                    'mode': 'NULLABLE'
                }

                fields.append(field)

            # ---------------------------------------------------------
            # Audit fields
            # ---------------------------------------------------------
            fields.extend([
                {
                    'name': 'FECHA_CARGA',
                    'type': 'DATE',
                    'mode': 'NULLABLE'
                }
            ])

            # ---------------------------------------------------------
            # Final JSON
            # ---------------------------------------------------------
            schema_json = {
                'table_name': 'CRM_DATA_SFMC_PUBLIST',
                'schema': 'CRM',
                'description': (
                    'Salesforce SFMC Publication List'
                ),
                'partition_by': 'FECHA_CARGA',
                'fields': fields
            }

            # ---------------------------------------------------------
            # Save JSON
            # ---------------------------------------------------------
            with open(
                output_json,
                'w',
                encoding='utf-8'
            ) as file:

                json.dump(
                    schema_json,
                    file,
                    indent=4,
                    ensure_ascii=False
                )

        # -------------------------------------------------------------
        # Close connections
        # -------------------------------------------------------------
        remote_csv.close()

        sftp.close()

        transport.close()

    # -----------------------------------------------------------------
    # Consolidate samples
    # -----------------------------------------------------------------
    logging.info(
        'Generating consolidated sample'
    )

    df_final = pd.concat(
        dfs,
        ignore_index=True
    )

    # -----------------------------------------------------------------
    # Save consolidated CSV
    # -----------------------------------------------------------------
    df_final.to_csv(
        sample_output,
        index=False,
        encoding='utf-8'
    )

    logging.info(
        'Process finished successfully'
    )


# ---------------------------------------------------------------------
# Entrypoint
# ---------------------------------------------------------------------
if __name__ == '__main__':

    main()
