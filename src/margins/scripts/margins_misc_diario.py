"""Script Carga Diaria Fuentes Manuales Reporte de Margen (SharePoint)."""
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
parser.add_argument(
    '--load_files', type=str,
    help='Files to be bq loaded'
)

month_dict = {
    'Enero' : '01',
    'Febrero' : '02',
    'Marzo' : '03',
    'Abril' : '04',
    'Mayo' : '05',
    'Junio' : '06',
    'Julio' : '07',
    'Agosto' : '08',
    'Septiembre' : '09',
    'Octubre' : '10',
    'Noviembre' : '11',
    'Diciembre' : '12'
}

# -------------------------------------------------------------------------
# Cleaning Func
# -------------------------------------------------------------------------
def cleaning_func(file: str,df: pd.DataFrame) -> pd.DataFrame:
    """Transform Dataframe into expected format for uploading into BQ.

    Parameters
    ----------
    df : pd.DataFrame
        Input DataFrame to transform.
    file : str
        Short name of the type of file associated to the dataframe
    """
    logging.info('Before cleaning:', df)
    if file.startswith('admg'):
        df['MES'] = df['MES'].astype('Int64')
        df['Material'] = df['Material'].astype('Int64')
        df['SELLOUT'] = df['SELLOUT'].astype('Float64')
        df = df.dropna(axis=0,subset=['MES'])  # noqa: PD901
        logging.info('After cleaning:', df)
        return df
    return df

# -------------------------------------------------------------------------
# Main function
# -------------------------------------------------------------------------
def main() -> None:  # noqa: D103
    # Parse input variables
    args = vars(parser.parse_args())
    gcp_project_id: str = args['project_id']
    load_files: list[str] = args['load_files'].split(':')
    reference_files = [
                    'admg','admg_alvi','admg_m10','admg_s10']

    #Default: tomar todo
    if 'all' in load_files:
        load_files = reference_files

    # Set all clients
    sp_cred = secretmanager.getSecret('bdaa_sharepoint_credentials',
                                      project=gcp_project_id)
    gbq_client = bigquery.Client()
    #input files
    site = (
        '/sites/BigDatayAdvancedAnalytics/Documentos compartidos/'
        'Paneles Power BI/Reporte de Margen'
    )
    input_files = {
            'admg': f'{site}/Unimarc/Fuentes datos manuales/Adicional al Margen/Adicional al margen.xlsx',  # noqa: E501
            'admg_alvi' : f'{site}/Alvi/Fuentes manuales de informacion/Adicional al margen/Adicional al margen Alvi.xlsx',  # noqa: E501
            'admg_m10' : f'{site}/M10/Fuentes manuales de informacion/Adicional al margen/Adicional al margen M10.xlsx',  # noqa: E501
            'admg_s10': f'{site}/S10/Fuentes manuales de informacion/Adicional al margen/Adicional al margen S10.xlsx'  # noqa: E501
            }
    #table definitions jsons
    jsons = {
        'admg' : 'adicional_al_margen_unimarc.json',
        'admg_alvi' : 'adicional_al_margen_alvi.json',
        'admg_m10' : 'adicional_al_margen_m10.json',
        'admg_s10' : 'adicional_al_margen_s10.json'

    }

    for file in load_files:
        logging.info(f'Starting extraction of {file} from Sharepoint')
        sharepoint = sp.SharePointFile(
            **sp_cred,
            server_relative_path=input_files[file]
            )
        sheet_name = 0
        df_file = sharepoint.toFrame(sheet_name = sheet_name)
        df_file =cleaning_func(file,df_file)

        # Upload data w/replace
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
