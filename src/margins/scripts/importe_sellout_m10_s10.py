"""Script Carga Menaual Consolidado Sellout M10/S10."""
# Default
import os
import logging
import argparse
from logging import config

import pandas as pd
import pendulum

# pip
from google.cloud import bigquery
from office365.runtime.client_request_exception import ClientRequestException

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
    '--execution_month', type=str,
    help='DAG execution month'
)



# -------------------------------------------------------------------------
# Cleaning Func
# -------------------------------------------------------------------------
def cleaning_func(df_file:pd.DataFrame,mes_carga:str)->pd.DataFrame:
    """Transform Dataframe into expected format for uploading into BQ.

    Parameters
    ----------
    df_file : pd.DataFrame
        Input DataFrame to transform.
    mes_carga : str
        Upload month to be added as a new field.
    """
    logging.info(f'Before cleaning: {df_file}')
    try:
        mes = df_file.columns[1].split(' ')[3]
    except IndexError:
        mes = df_file.iloc[0,1].split(' ')[3]

    #Drop first 2 rows and last 4 columns
    df_file = df_file[2:]
    df_file = df_file.iloc[:, :17]
    logging.info(f'shape: {df_file.shape}')
    #rename columns
    df_file.columns = ['id','nombre_proveedor', 'proveedor','n_cabecera','tipo_evento',
                  'fecha_inicio','fecha_termino','articulo','ean','descripcion',
                  'umv','importe_sell_out','id_promotions_consola_3','promocion',
                  'pvp_normal','factor','locales']

    #Limpiar
    df_file['n_cabecera'] = df_file['n_cabecera'].replace('-', '')
    #df_file = df_file.astype('str')  # noqa: ERA001
    df_file = df_file.replace('nan', '')


    df_file['importe_sell_out'] = pd.to_numeric(df_file['importe_sell_out']).astype('Int64')
    df_file['factor'] = pd.to_numeric(df_file['factor']).astype('Int64')
    df_file['ean'] = pd.to_numeric(df_file['ean']).astype('Int64')
    df_file['n_cabecera'] = pd.to_numeric(df_file['n_cabecera']).astype('Int64')
    #Agregar mes
    df_file['mes'] = mes
    df_file['mes_carga'] = pd.to_datetime(mes_carga,format='%Y%m')
    logging.info(f'After cleaning: {df_file}')
    return df_file

# -------------------------------------------------------------------------
# Main function
# -------------------------------------------------------------------------
def main() -> None:  # noqa: D103
    # Parse input variables
    args = vars(parser.parse_args())
    gcp_project_id: str = args['project_id']
    execution_date: str = args['execution_date']
    execution_month: str = args['execution_month']
    formatos = ['s10','m10']

    # Set all clients
    sp_cred = secretmanager.getSecret('bdaa_sharepoint_credentials',
                                      project= gcp_project_id)
    gbq_client = bigquery.Client()
    #input files
    file_site = (
        '/sites/BigDatayAdvancedAnalytics/Documentos compartidos/'
        'Pricing/SellOut/SellOut Consolidado/'
    )
    input_files = {
        's10': f'{file_site}/S10/Sellout_S10_{execution_month}.xlsx',
        'm10': f'{file_site}/M10/Sellout_M10_{execution_month}.xlsx',
            }
    #table definitions jsons
    jsons = {
        's10' : 'sellout_id0_s10.json',
        'm10' : 'sellout_id0_m10.json'
    }
    schema = 'REPORTE_MARGEN'
    table_ref = {
        's10' : f'{gcp_project_id}.{schema}.REPORTE_MARGEN_SELLOUT_ID0_S10',
        'm10' : f'{gcp_project_id}.{schema}.REPORTE_MARGEN_SELLOUT_ID0_M10'
    }
    for file in formatos:
        logging.info(f'Starting extraction of sellout id0 {input_files[file]} {execution_month} from SP')  # noqa: E501
        sharepoint = sp.SharePointFile(
            **sp_cred,
            server_relative_path=input_files[file]
            )
        modified = False
        try:
            last_time_modified = sharepoint.lastTimeModified()
            logging.info(f'Last time modified: {last_time_modified}')
            logging.info(f'Execution date: {execution_date}')
            if last_time_modified.strftime('%Y-%m-%d') == execution_date:
                modified = True
        except ClientRequestException:
            logging.info('Error getting file')
            return

        if modified:
            logging.info('Archivo existe y fue modificado hoy')
            df_file = sharepoint.toFrame(sheet_name = 'ID 0')
            file_name = os.path.basename(sharepoint.server_relative_path)

            df_file =cleaning_func(df_file,execution_month)
            # Upload data
            logging.info('Uploading data')
            logging.info('Create table')
            gbq_extended.createTableFromJSON(
                    table_ddl_json_path=os.path.join('gbq_objects', jsons[file]),
                    project=gcp_project_id,
                    gbq_client=gbq_client,
                    if_exists='ignore',
                )
            logging.info('Delete from table to avoid duplication')
            #Delete from table so that data is not duplicated
            gbq_extended.deleteFromTable(table_ref=table_ref[file],
                                        where_clause=f'mes_carga=CAST(CONCAT(SUBSTRING("{execution_month}",0,4),"-",SUBSTRING("{execution_month}",5,2),"-01") AS DATE)',  # noqa: E501
                                        gbq_client=gbq_client
                                        )
            logging.info('Uploading dataframe')
            gbq_extended.uploadFrame(
                df_file,
                table_ddl_json_path=os.path.join('gbq_objects', jsons[file]),
                project=gcp_project_id,
                gbq_client=gbq_client,
                if_exists='append',
            )
            logging.info('Data uploaded')
            new_name =  f'PROCESADO-{file_name}-{pendulum.now().to_date_string()}'
            sharepoint.rename(new_name=new_name)
    logging.info('Process ended!')

if __name__ == '__main__':
    main()
