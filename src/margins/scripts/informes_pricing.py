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
def cleaning_func(formato,df):
    print('Before cleaning:', df)
    if formato == 'unimarc':
        df = df.iloc[:, :26]  # noqa: PD901
        df = df.replace('|', '', regex=True)  # noqa: PD901
        df['Solo 1 barra por linea'] = df['Solo 1 barra por linea'].astype('Float64').astype('Int64')  # noqa: E501
        print('After cleaning:', df)
        return df
    if formato == 's10':
        df.columns = df.iloc[5,]
        df = df.iloc[6:, :25]  # noqa: PD901
        df = df.replace('|', '', regex=True)  # noqa: PD901
        df['Solo 1 barra por linea'] = df['Solo 1 barra por linea'].astype('Float64').astype('Int64')  # noqa: E501
        print('After cleaning:', df)
        return df
    if formato == 'm10':
        df.columns = df.iloc[5,]
        df = df[6:]  # noqa: PD901
        df = df.iloc[:, :22]  # noqa: PD901
        df = df.replace('|', '', regex=True)  # noqa: PD901
        df['Solo 1 barra por linea'] = df['Solo 1 barra por linea'].astype('Float64').astype('Int64')  # noqa: E501
        print('After cleaning:', df)
        return df
    return df

# -------------------------------------------------------------------------
# Main function
# -------------------------------------------------------------------------
def main() -> None:  # noqa: D103
    # Parse input variables
    args = vars(parser.parse_args())
    gcp_project_id: str = args['project_id']
    formatos = ['unimarc','s10','m10']

    # Set all clients
    sp_cred = secretmanager.getSecret('bdaa_sharepoint_credentials',
                                      project=gcp_project_id)
    gbq_client = bigquery.Client()
    #input files
    file_site = '/sites/BigDatayAdvancedAnalytics/Documentos compartidos/Pricing/Informes Pricing/'
    input_files = {
        'unimarc': f'{file_site}/Unimarc/InformePricing_Unimarc.xlsx',
        's10': f'{file_site}/S10/InformePricing_S10.xlsx',
        'm10': f'{file_site}/M10/InformePricing_M10.xlsx',
            }
    #table definitions jsons
    jsons = {
        'unimarc' : 'informe_pricing_unimarc.json',
        's10' : 'informe_pricing_s10.json',
        'm10' : 'informe_pricing_m10.json'
    }

    for file in formatos:
        logging.info(f'Starting extraction of -- informe pricing {file} -- from Sharepoint')
        sharepoint = sp.SharePointFile(
            **sp_cred,
            server_relative_path=input_files[file]
            )

        last_time_modified = sharepoint.lastTimeModified()

        logging.info(f'Last time modified: {last_time_modified}')

        df_file = sharepoint.toFrame()
        df_file =cleaning_func(file,df_file)

        # Upload data
        logging.info('Uploading data')
        gbq_extended.uploadFrame(
            df_file,
            table_ddl_json_path=os.path.join('gbq_objects', jsons[file]),
            project=gcp_project_id,
            gbq_client=gbq_client,
            if_exists='replace',
        )
    logging.info('Process ended!')

if __name__ == '__main__':
    main()
