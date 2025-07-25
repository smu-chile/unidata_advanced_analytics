# Default
import os
import logging
import argparse
from string import Template
from logging import config
from datetime import datetime

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
    '--proc_years', type=str,
    help='Years to process'
)
# -------------------------------------------------------------------------
# Cleaning Func
# -------------------------------------------------------------------------
def cleaning_func(df, year,file):
    print('Before cleaning:', df)
    if file == 'jerarquia_ytd':
        #Drop first 8 rows and empty right columns
        df = df[9:]  # noqa: PD901
        df = df.iloc[:, :15]  # noqa: PD901
        logging.info('shape: %s', df.shape)
        #rename columns
        df.columns = ['departamento','cl_xc_categoria', 'segmento',
                    'negocio', 'item_code', 'item', 'subsegmento',
                    'tamano', 'tipo', 'variedad', 'periodos',
                    'total_mercado_vtas_valor', 'total_mercado_vtas_unit',
                    'total_supermercados_amp_internet_vtas_valor',
                    'total_supermercados_amp_internet_vtas_unit'
                    ]
    if file == 'jerarquia_upc':
        df = df[9:]  # noqa: PD901
        df = df.iloc[:, :20]  # noqa: PD901
        logging.info('shape: %s', df.shape)
        #rename columns
        df.columns = ['departamento','cl_xc_categoria', 'segmento',
                     'negocio', 'item_code', 'item', 'subsegmento',
                     'tamano', 'tipo', 'variedad', 'upc', 'cl_xc_marca',
                      'dermo', 'envase', 'level_4_nielsen', 'sabores',
                    'submarca', 'periodos', 'total_mercado_vtas_valor',
                    'total_chile_cadena_unimarc_vtas_valor'
                     ]
    #Drop trailing rows
    df = df.dropna(axis=0,subset=['departamento','cl_xc_categoria'])  # noqa: PD901
    df = df.replace('|', '', regex=True)  # noqa: PD901
    #Add column out of periodos
    df['periodos'] = df['periodos'].str.lower()#
    df['year'] = datetime.strptime(year,'%Y')  # noqa: DTZ007
    df['year'] = pd.to_datetime(df.year)
    df = df.replace(' nan', 0.0)  # noqa: PD901
    df = df.replace('nan', 0.0)  # noqa: PD901
    df['item_code'] = df['item_code'].astype('Float64').astype('Int64')  # noqa: E501

    print('After cleaning:', df)
    return df

# -------------------------------------------------------------------------
# Main function
# -------------------------------------------------------------------------
def main() -> None:  # noqa: D103
    # Parse input variables
    args = vars(parser.parse_args())
    gcp_project_id: str = args['project_id']
    proc_years: list[str] = args['proc_years'].split(':')
    load_files = ['jerarquia_ytd','jerarquia_upc']
    # Set all clients
    sp_cred = secretmanager.getSecret('bdaa_sharepoint_credentials')
    gbq_client = bigquery.Client()
    site_root = (
    '/sites/'
    'BigDatayAdvancedAnalytics/'
    'Documentos compartidos/'
    'Pricing/Forecast Venta/'
    'Data Nielsen/Uni M10 Internet'
    )
    #input files
    input_files = {
        'jerarquia_ytd' :  Template('$site_root/Jerarquia Mercado YTD/VENTA MERCADO JERARQUIA NIELSEN $year.xlsx'),  # noqa: E501
        'jerarquia_upc' : Template('$site_root/Jerarquia Mercado YTD/VENTA MERCADO JERARQUIA NIELSEN UPC $year.xlsx'),  # noqa: E501
       }
    #table definitions jsons
    jsons = {
        'jerarquia_ytd' : 'nielsen_anual_venta_mercado_jerarquia_ytd.json',
        'jerarquia_upc' : 'nielsen_anual_venta_mercado_jerarquia_upc.json'

    }
    schema = 'ML_LAB'
    table_ref = {
        'jerarquia_ytd' : f'{gcp_project_id}.{schema}.NIELSEN_ANUAL_VENTA_MERCADO_JERARQUIA_YTD',
        'jerarquia_upc' : f'{gcp_project_id}.{schema}.NIELSEN_ANUAL_VENTA_MERCADO_JERARQUIA_UPC'
    }


    for file in load_files:
        for year in proc_years:
            input_file_ytd = input_files[file].substitute(site_root=site_root,
                                                          year=year)
            logging.info(f'Starting extraction of {input_file_ytd} from Sharepoint')
            sharepoint = sp.SharePointFile(sp_cred['client_id'],
                                       sp_cred['client_secret'],input_file_ytd)
            df_file = sharepoint.toFrame()
            df_file =cleaning_func(df_file,year, file)

            # Upload data
            logging.info('Uploading data')
            gbq_extended.createTableFromJSON(
                    ddl_json_config_path=os.path.join('gbq_objects', jsons[file]),
                    project=gcp_project_id,
                    gbq_client=gbq_client,
                    if_exists='ignore',
                )

            #Delete from table so that data is not duplicated
            gbq_extended.deleteFromTable(table_ref=table_ref[file],
                                         where_clause=f'year="{year}"',
                                         gbq_client=gbq_client
                                         )

            gbq_extended.uploadFrame(
                df_file,
                table_ddl_json_path=os.path.join('gbq_objects', jsons[file]),
                project=gcp_project_id,
                gbq_client=gbq_client,
                if_exists='append',
            )
            logging.info(f'File {input_file_ytd} uploaded')
    logging.info('Process ended!')


if __name__ == '__main__':
    main()
