"""Script Carga Informe Pricing."""
# Default
import os
import logging
import argparse
from logging import config

import pandas as pd

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
def cleaning_func(formato: str,df_file: pd.DataFrame) -> pd.DataFrame:
    """Transform Dataframe into expected format for uploading into BQ.

    Parameters
    ----------
    df_file : pd.DataFrame
        Input DataFrame to transform.
    formato : str
        Business format to be added as a new field.
    """
    logging.info(f'Before cleaning: {df_file}')
    if formato == 'unimarc':
        df_file = df_file.iloc[:, :26]
        df_file = df_file.replace('|', '', regex=True)
        df_file = df_file[['CODIGO SAP SMU', 'Solo 1 barra por linea', 'DESCRIPCION', 'UM',
       'Unnamed: 4', 'PRECIO NORMAL', 'P/F', 'FACTOR', 'OFERTA',
       'PACK VIRTUAL', 'ID 1', 'ID 2', 'BONIFICACION', 'LISTA DE PRODUCTOS',
       'LISTA DE LOCALES', 'DESCRIPCION_LS_SELL_OUT', 'NOMBRE DEL PROVEEDOR',
       'RUT PROVEEDOR', 'IMPORTE SELL OUT', 'FINANCIAMIENTO', 'INICIO',
       'TERMINO', 'FECHA DE INCORPORACION ','ID ALTERNATIVO WF', 'NUMERO CABECERA',
       'TIPO PROMOCIÓN']]

        df_file['FACTOR'] = df_file['FACTOR'].astype('Float64')
        df_file['PRECIO NORMAL'] = df_file['PRECIO NORMAL'].astype('Float64')
        df_file['P/F'] = df_file['P/F'].astype('Float64')
        df_file['OFERTA'] = df_file['OFERTA'].astype('str')
        df_file['ID ALTERNATIVO WF'] = df_file['ID ALTERNATIVO WF'].astype('str')
        df_file['NUMERO CABECERA'] = df_file['NUMERO CABECERA'].astype('str')
        logging.info(f'TYPES: {df_file.dtypes}')
        logging.info(f'After cleaning: {df_file}')
        return df_file
    if formato == 's10':
        df_file.columns = df_file.iloc[5,]
        df_file = df_file.iloc[6:, :25]
        df_file = df_file.replace('|', '', regex=True)

        logging.info(f'After cleaning: {df_file}')
        return df_file
    if formato == 'm10':
        df_file.columns = df_file.iloc[5,]
        df_file = df_file[6:]
        df_file = df_file.iloc[:, :22]
        df_file = df_file.replace('|', '', regex=True)

        logging.info(f'After cleaning: {df_file}')
        return df_file
    return df_file

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
