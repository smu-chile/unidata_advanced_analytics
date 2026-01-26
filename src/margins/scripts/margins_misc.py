"""Script Carga On-Demand Fuentes Manuales Reporte de Margen (SP)."""
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
ppto_year = '2026' #sorry about this, will think of something, thx
# -------------------------------------------------------------------------
# Cleaning Func
# -------------------------------------------------------------------------
def cleaning_func(file: str,df_file: pd.DataFrame) -> pd.DataFrame :
    """Transform Dataframe into expected format for uploading into BQ.

    Parameters
    ----------
    df_file : pd.DataFrame
        Input DataFrame to transform.
    file : str
        Short name of the type of file associated to the dataframe
    """
    logging.info(f'Before cleaning: {df_file}')
    if file == 'ppto':
        df_file = df_file[['Formato', 'Mes', 'Categoria', 'Venta_Neta', 'Costo_Neto',
       'Margen Comercial 1', 'Recupero', 'Sell Out']]
        float_columns = 	['Venta_Neta',
                    'Costo_Neto',
                    'Margen Comercial 1',
                    'Recupero',
                    'Sell Out']
        for c in float_columns:
            df_file[c] = df_file[c].astype('Float64')
        df_file = df_file.replace('nan', 0.0)
        df_file['año'] = ppto_year
        df_file['Mes'] = df_file['año'] + df_file['Mes'].map(month_dict)
        df_file['año'] = df_file['año'].astype('Int64')
        logging.info(f'After cleaning: {df_file}')
        return df_file
    if file == 'ppto_mg1':
        df_file.columns = ['Mes', 'Formato', 'Categoria', 'ppto_venta_neta',
                      'ppto_contrib', 'ppto_costo_neto']
        float_columns = ['ppto_venta_neta',
                    'ppto_contrib',
                    'ppto_costo_neto']
        for c in float_columns:
            df_file[c] = df_file[c].astype('Float64')
        df_file = df_file.replace('nan', 0.0)
        df_file['año'] = ppto_year
        df_file['Mes'] =  pd.to_datetime(df_file['Mes'], format='%Y-%m-%d').dt.strftime('%Y%m')
        df_file['año'] = df_file['año'].astype('Int64')
        logging.info(f'After cleaning: {df_file}')
        return df_file
    if file.startswith('admg'):
        df_file['MES'] = df_file['MES'].astype('Int64')
        df_file['Material'] = df_file['Material'].astype('Int64')
        df_file['SELLOUT'] = df_file['SELLOUT'].astype('Float64')
        df_file = df_file.dropna(axis=0,subset=['MES'])
        logging.info(f'After cleaning: {df_file}')
        return df_file
    if file == 'est_com_alvi':
        df_file = df_file.replace('#N/D', '')
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
    load_files: list[str] = args['load_files'].split(':')
    reference_files = ['ppto','ppto_mg1', 'cat_h',
                       'est_com','est_com_alvi',
                       'ila','ila_m10','ila_s10',
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
            'ppto' :f'{site}/Unimarc/Fuentes datos manuales/Presupuesto/2026/PPTO FORMATOS SELLOUT RECUPERO 2026.xlsx',  # noqa: E501
            'ppto_mg1' : f'{site}/Unimarc/Reporte Unimarc MMPP/PPTO/PPTO 2025.xlsx',
            'cat_h' : f'{site}/Unimarc/Fuentes datos manuales/Categoria H/Categoria_H_Estructura_Comercial.xlsx',  # noqa: E501
            'est_com' : f'{site}/Unimarc/Fuentes datos manuales/Estructura comercial/Estructura Comercial.xlsx',  # noqa: E501
            'est_com_alvi' : f'{site}/Alvi/Fuentes manuales de informacion/Estructura Comercial/Estructura Comercial Alvi.xlsx',  # noqa: E501
            'ila' : f'{site}/Unimarc/Fuentes datos manuales/ILA/ILA_UNIMARC.xlsx',
            'ila_m10': f'{site}/M10/Fuentes manuales de informacion/ILA/ILA_M10.xlsx' ,
            'ila_s10' : f'{site}/S10/Fuentes manuales de informacion/ILA/ILA_S10.xlsx',
            'admg': f'{site}/Unimarc/Fuentes datos manuales/Adicional al Margen/Adicional al margen.xlsx',  # noqa: E501
            'admg_alvi' : f'{site}/Alvi/Fuentes manuales de informacion/Adicional al margen/Adicional al margen Alvi.xlsx',  # noqa: E501
            'admg_m10' : f'{site}/M10/Fuentes manuales de informacion/Adicional al margen/Adicional al margen M10.xlsx',  # noqa: E501
            'admg_s10': f'{site}/S10/Fuentes manuales de informacion/Adicional al margen/Adicional al margen S10.xlsx',  # noqa: E501
            }
    #table definitions jsons
    jsons = {
        'ppto' : 'ppto_formatos_sellout_recupero.json',
        'ppto_mg1' : 'ppto_mmpp_mg1.json',
        'cat_h' : 'categoria_h.json',
        'est_com' : 'estructura_comercial.json',
        'est_com_alvi' : 'estructura_comercial_alvi.json',
        'ila' : 'ila_proyectado_unimarc.json',
        'ila_m10' : 'ila_proyectado_m10.json',
        'ila_s10' : 'ila_proyectado_s10.json',
        'admg' : 'adicional_al_margen_unimarc.json',
        'admg_alvi' : 'adicional_al_margen_alvi.json',
        'admg_m10' : 'adicional_al_margen_m10.json',
        'admg_s10' : 'adicional_al_margen_s10.json',

    }

    for file in load_files:
        logging.info(f'Starting extraction of {file} from Sharepoint')
        sharepoint = sp.SharePointFile(
            **sp_cred,
            server_relative_path=input_files[file]
            )
        sheet_name = 0
        if file == 'ppto_mg1':
            sheet_name = 'MG1'
        if file == 'ppto':
            sheet_name = 'PPTO Formatos'
        df_file = sharepoint.toFrame(sheet_name = sheet_name)
        df_file =cleaning_func(file,df_file)

        if file.startswith('ppto'):
            logging.info('Creating table if not exists')
            gbq_extended.createTableFromJSON(
                        table_ddl_json_path=os.path.join('gbq_objects', jsons[file]),
                        project=gcp_project_id,
                        gbq_client=gbq_client,
                        if_exists='ignore',
                    )
            #Delete from table so that data is not duplicated
            schema = 'REPORTE_MARGEN'
            table_suffix = jsons[file].split('.')[0].upper()
            #REPORTE_MARGEN_PPTO_FORMATOS_SELLOUT_RECUPERO_2025
            table = f'REPORTE_MARGEN_{table_suffix}'
            table_ref = f'{gcp_project_id}.{schema}.{table}'
            logging.info(f'Delete from {table_ref} to avoid duplicates')  # noqa: S608
            gbq_extended.deleteFromTable(table_ref=table_ref,
                                        where_clause= f'anio = {ppto_year}',
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
        else:
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
