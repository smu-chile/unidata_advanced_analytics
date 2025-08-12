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
# Cleaning Func
# -------------------------------------------------------------------------
def cleaning_func(df_file):
    print('Before cleaning:', df_file)
    df_file = df_file.iloc[:, :7]
    df_file = df_file.replace('|', '', regex=True)
    df_file = df_file.replace('\n', '', regex=True)

    print('After cleaning:', df_file)
    return df_file

# -------------------------------------------------------------------------
# Main function
# -------------------------------------------------------------------------
def main() -> None:  # noqa: D103
    # Parse input variables
    args = vars(parser.parse_args())
    gcp_project_id: str = args['project_id']

    # Set all clients
    sp_cred = secretmanager.getSecret('bdaa_sharepoint_credentials')
    gbq_client = bigquery.Client()
    #input files
    file_site = '/sites/BigDatayAdvancedAnalytics/Documentos compartidos/Pricing/Genfix'
    input_file =  f'{file_site}/Genfix Equipo.xlsx'
    #table definitions jsons
    json = 'pricing_genfix.json'


    logging.info(f'Starting extraction of -- informe pricing {input_file} -- from Sharepoint')
    sharepoint = sp.SharePointFile(sp_cred['client_id'],
                                   sp_cred['client_secret'],
                                   input_file)
    last_time_modified = sharepoint.lastTimeModified()
    logging.info(f'Last time modified: {last_time_modified}')
    df_file = sharepoint.toFrame()
    df_file =cleaning_func(input_file,df_file)
    # Upload data
    logging.info('Uploading data')
    gbq_extended.uploadFrame(
        df_file,
        table_ddl_json_path=os.path.join('gbq_objects', json),
        project=gcp_project_id,
        gbq_client=gbq_client,
        if_exists='replace',
    )
    logging.info('Process ended!')

if __name__ == '__main__':
    main()
