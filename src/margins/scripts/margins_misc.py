# Default
import os
import logging
import argparse
from logging import config

# pip
from google.cloud import bigquery

import common.gcp_extended.bigquery as gbq_extended
import common.gcp_extended.secretsmanager as secretmanager

# Own
import common.office365_extended.sharepoint as sp
from common.constants import LOGGING_CONFIG


# -------------------------------------------------------------------------
#  Config
# -------------------------------------------------------------------------
# Logging config
config.dictConfig(LOGGING_CONFIG)
# Parser config
parser = argparse.ArgumentParser()
parser.add_argument(
    '--project_id', type=str,
    help='GCP project in which the script will be executed'
)
parser.add_argument(
    '--execution_date', type=str,
    help='DAG execution date'
)

# -------------------------------------------------------------------------
# Main function
# -------------------------------------------------------------------------
def main() -> None:  # noqa: D103
    # Parse input variables
    args = vars(parser.parse_args())
    execution_date: str = args['execution_date']
    gcp_project_id: str = args['project_id']
    logging.info(f'execution_date: {execution_date}')

    # Set all clients
    sp_cred = secretmanager.getSecret('bdaa_sharepoint_credentials')
    gbq_client = bigquery.Client()
    #ILA
    input_files_ila = '/sites/BigDatayAdvancedAnalytics/Documentos compartidos/Paneles Power BI/Reporte de Margen/Unimarc/Fuentes datos manuales/ILA/ILA_UNIMARC.xlsx'  # noqa: E501

    sharepoint = sp.SharePointFile(sp_cred['client_id'],sp_cred['client_secert'],input_files_ila)
    #TODO(csotob): agregar checkfileupdate
    df_ila = sharepoint.toFrame()

    # Create GBQ table if does not exist
    logging.info('Creating GBQ table using JSON')
    gbq_extended.createTableFromJSON(
        ddl_json_config_path=os.path.join('gbq_objects', 'ila_proyectado_unimarc.json'),
        project=gcp_project_id,
        gbq_client=gbq_client,
        if_exists='rebuild',
    )

    # Upload data
    logging.info('Uploading data')
    gbq_extended.uploadFrame(
        df_ila,
        table_ref=f'{gcp_project_id}.ML_LAB.ILA_PROYECTADO',
        table_ddl_json_path=os.path.join('gbq_objects', 'ila_proyectado_unimarc.json'),
        gbq_client=gbq_client,
        if_exists='replace',
    )
    logging.info('Process ended!')


if __name__ == '__main__':
    main()
