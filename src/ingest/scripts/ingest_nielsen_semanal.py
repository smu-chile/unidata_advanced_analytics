# Default
import os
import logging
import argparse
from string import Template
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
    '--execution_week', type=str,
    help='Week(s) to process'
)
parser.add_argument(
    '--load_files', type=str,
    help='File(s) to be bq loaded'
)

# -------------------------------------------------------------------------
# Cleaning Func
# -------------------------------------------------------------------------
def cleaning_func(df_file, week,file):
    print('Before cleaning:', df_file)
    if file == 'categoria_item':
        n_columns = df_file.shape[1]
        #data does not contain unimarc emcommerce
        if n_columns == 16:
            df_file.columns = ['departamento', 'cl_xc_categoria', 'negocio', 'item_code',
                        'item', 'periodos', 'total_mercado_vtas_valor', 'total_mercado_vtas_unit',
                        'unimarc_vtas_valor', 'unimarc_vtas_unit',
                        'm10s10_vtas_valor', 'm10s10_vtas_unit',
                        'total_internet_vtas_valor','total_internet_vtas_unit',
                        'total_mercado_internet_vtas_valor','total_mercado_internet_vtas_unit']
            df_file = df_file.insert(12,'unimarc_internet_vtas_valor',0)
            df_file = df_file.insert(13,'unimarc_internet_vtas_unit',0)
        elif n_columns == 18:
            df_file.columns = ['departamento', 'cl_xc_categoria', 'negocio', 'item_code',
                        'item', 'periodos', 'total_mercado_vtas_valor', 'total_mercado_vtas_unit',
                        'unimarc_vtas_valor', 'unimarc_vtas_unit',
                        'm10s10_vtas_valor', 'm10s10_vtas_unit',
                        'unimarc_internet_vtas_valor','unimarc_internet_vtas_unit',
                        'total_internet_vtas_valor','total_internet_vtas_unit',
                        'total_mercado_internet_vtas_valor','total_mercado_internet_vtas_unit']
        else:
            err_msg = 'Unexpected DataFrame Shape'
            raise Exception(err_msg)
                # Drop trailing rows
        df_file = df_file.dropna(axis=0,subset=['departamento','cl_xc_categoria'])
        df_file = df_file.replace('|', '', regex=True)
        df_file['item_code'] = df_file['item_code'].astype('Float64').astype('Int64')


    if file == 'venta_categoria':
        df_file.columns = ['departamento', 'cl_xc_categoria', 'negocio',
                     'periodos', 'total_mercado_vtas_valor', 'total_mercado_vtas_unit',
                     'unimarc_vtas_valor', 'unimarc_vtas_unit',
                     'm10s10_vtas_valor', 'm10s10_vtas_unit',
                     'unimarc_internet_vtas_valor','unimarc_internet_vtas_unit',
                     'total_internet_vtas_valor','total_internet_vtas_unit',
                     'total_mercado_internet_vtas_valor','total_mercado_internet_vtas_unit']
        #Drop trailing rows
        df_file = df_file.dropna(axis=0,subset=['departamento','cl_xc_categoria'])
        df_file = df_file.replace('|', '', regex=True)
    if file == 'venta_negocio':
        #rename columns
        df_file.columns = ['negocio','cl_total_store',
                     'periodos', 'total_mercado_vtas_valor', 'total_mercado_vtas_unit',
                     'unimarc_vtas_valor', 'unimarc_vtas_unit',
                     'm10s10_vtas_valor', 'm10s10_vtas_unit',
                     'unimarc_internet_vtas_valor','unimarc_internet_vtas_unit',
                     'total_internet_vtas_valor','total_internet_vtas_unit',
                     'total_mercado_internet_vtas_valor','total_mercado_internet_vtas_unit']
        #Drop trailing rows
        df_file = df_file.dropna(axis=0,subset=['cl_total_store'])
        df_file = df_file.replace('|', '', regex=True)

    # Add column out of periodos
    df_file['periodos'] = df_file['periodos'].str.lower()#
    df_file['fin_periodo'] = df_file['periodos'].str.split('fin ',expand=True).iloc[:,[1]]
    df_file['fin_periodo'] = pd.to_datetime(df_file.fin_periodo, dayfirst= True)
    df_file['semana_carga'] = week
    print('After cleaning:', df_file)
    return df_file

# -------------------------------------------------------------------------
# Main function
# -------------------------------------------------------------------------
def main() -> None:  # noqa: D103
    # Parse input variables
    args = vars(parser.parse_args())
    gcp_project_id: str = args['project_id']
    execution_week: list[str] = args['execution_week'].split(':')
    load_files: list[str] = args['load_files'].split(':')
    reference_files = ['categoria_item','venta_categoria','venta_negocio']
    #Default: tomar todo
    if 'all' in load_files:
        load_files = reference_files
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
        'categoria_item' :  Template('$site_root/Semana Categoria Item/VENTA MERCADO Y UNIMARC M10 S10 INTERNET POR SEMANA, CATEGORIA Y Item $week.xlsx'),  # noqa: E501
        'venta_categoria' : Template('$site_root/Semana Categoria/VENTA MERCADO Y UNIMARC M10 S10 INTERNET POR SEMANA, CATEGORIA $week.xlsx'),  # noqa: E501
        'venta_negocio' : Template('$site_root/Semana Negocio/VENTA MERCADO Y UNIMARC M10 S10 INTERNET POR SEMANA, NEGOCIO $week.xlsx')  # noqa: E501
       }
    #table definitions jsons
    jsons = {
        'categoria_item' : 'nielsen_semanal_venta_categoria_item.json',
        'venta_categoria' : 'nielsen_semanal_venta_categoria.json',
        'venta_negocio' : 'nielsen_semanal_venta_negocio.json'

    }
    schema = 'ML_LAB'
    table_ref = {
        'categoria_item' : f'{gcp_project_id}.{schema}.NIELSEN_SEMANAL_VENTA_CATEGORIA_ITEM',
        'venta_categoria' : f'{gcp_project_id}.{schema}.NIELSEN_SEMANAL_VENTA_CATEGORIA',
        'venta_negocio' : f'{gcp_project_id}.{schema}.NIELSEN_SEMANAL_VENTA_NEGOCIO'
    }


    for file in load_files:
        for week in execution_week:
            input_file_wk = input_files[file].substitute(site_root=site_root,
                                                          week=week)
            logging.info(f'Starting extraction of {input_file_wk} from Sharepoint')
            sharepoint = sp.SharePointFile(sp_cred['client_id'],
                                       sp_cred['client_secret'],input_file_wk)
            df_file = sharepoint.toFrame()
            df_file =cleaning_func(df_file,week, file)

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
                                         where_clause=f'semana_carga="{week}"',
                                         gbq_client=gbq_client
                                         )

            gbq_extended.uploadFrame(
                df_file,
                table_ddl_json_path=os.path.join('gbq_objects', jsons[file]),
                project=gcp_project_id,
                gbq_client=gbq_client,
                if_exists='append',
            )
            logging.info(f'File {input_file_wk} uploaded')
    logging.info('Process ended!')


if __name__ == '__main__':
    main()
