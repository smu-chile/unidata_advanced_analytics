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
parser.add_argument(
    '--load_files', type=str,
    help='Files to be bq loaded'
)


# -------------------------------------------------------------------------
# Cleaning Func
# -------------------------------------------------------------------------
def cleaning_func(file,df):
    print('Before cleaning:', df)
    if file == 'ppto':
        df['Nuevo Proveedor'] = df['Nuevo Proveedor'].astype('Int64')
        df['den pos'] = df['den pos'].astype('Int64')
        float_columns = 	['Venta_Neta',
                    'Costo_Neto',
                    'Margen Comercial 1',
                    'Recupero',
                    'aux',
                    'aux2',
                    'Aporte Fijo',
                    'Aporte Variable (Cumpl.Meta)',
                    'Devolucion y Merma 0',
                    'Inauguración',
                    'Reinaguración',
                    'Servicio de Reposición',
                    'Extranet',
                    ' Inclusión productos',
                    'Concursos/Sorteos',
                    'Bonificacion de Mercaderia',
                    'Sell Out',
                    'Ingreso Diferido',
                    'Recuperación Campaña',
                    'Distribucion y Centralizacion (Back Houl)',
                    'Merma 0 Manual',
                    'Rebate',
                    'Margen Comercial 2',
                    'Venta 2023',
                    'Venta MMPP',
                    'Mg1+SO MMPP',
                    'Impacto 2024',
                    'Gasto SellOut',
                    'Total Importe',
                    'Venta 2019',
                    'Mg1+SO 2019',
                    'Venta pricing',
                    'Mg1+so pricing',
                    'Num pos']
        for c in float_columns:
            df[c] = df[c].apply(lambda x: f'{x:.8f}')
        df = df.replace('nan', 0.0)  # noqa: PD901
        df = df.drop(['aux100'], axis=1)  # noqa: PD901
        print('After cleaning:', df)
        return df
    if file == 'ppto_mg1':
        df.columns = ['Mes', 'Formato', 'Categoria', 'ppto_venta_neta',
                      'ppto_contrib', 'ppto_costo_neto']
        float_columns = ['ppto_venta_neta',
                    'ppto_contrib',
                    'ppto_costo_neto']
        for c in float_columns:
            df[c] = df[c].apply(lambda x: f'{x:.8f}')
        df = df.replace('nan', 0.0)  # noqa: PD901
        print('After cleaning:', df)
        return df
    if file.startswith('admg'):
        df['Material'] = df['Material'].astype('Int64')
        df['SELLOUT'] = df['SELLOUT'].astype('Float64')
        print('After cleaning:', df)
        return df
    if file == 'est_com_alvi':
        df = df.replace('#N/D', '')  # noqa: PD901
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
    load_files: list[str] = args['load_files'].split(':')
    reference_files = ['ppto','ppto_mg1',
                       'est_com','est_com_alvi',
                       'ila','ila_m10','ila_s10',
                       'admg','admg_alvi','admg_m10','admg_s10']
    #Default: tomar todo
    if 'all' in load_files:
        load_files = reference_files

    # Set all clients
    sp_cred = secretmanager.getSecret('bdaa_sharepoint_credentials')
    gbq_client = bigquery.Client()
    #input files
    input_files = {
            'ppto' :'/sites/BigDatayAdvancedAnalytics/Documentos compartidos/Paneles Power BI/Reporte de Margen/Unimarc/Fuentes datos manuales/Presupuesto/2025/PPTO FORMATOS SELLOUT RECUPERO 2025.xlsx',  # noqa: E501
            'ppto_mg1' : '/sites/BigDatayAdvancedAnalytics/Documentos compartidos/Paneles Power BI/Reporte de Margen/Unimarc/Reporte Unimarc MMPP/PPTO/PPTO 2025.xlsx',  # noqa: E501
            'est_com' : '/sites/BigDatayAdvancedAnalytics/Documentos%20compartidos/Paneles%20Power%20BI/Reporte%20de%20Margen/Unimarc/Fuentes%20datos%20manuales/Estructura comercial/Estructura Comercial.xlsx',  # noqa: E501
            'est_com_alvi' : '/sites/BigDatayAdvancedAnalytics/Documentos compartidos/Paneles Power BI/Reporte de Margen/Alvi/Fuentes manuales de informacion/Estructura Comercial/Estructura Comercial Alvi.xlsx',  # noqa: E501
            'ila' : '/sites/BigDatayAdvancedAnalytics/Documentos compartidos/Paneles Power BI/Reporte de Margen/Unimarc/Fuentes datos manuales/ILA/ILA_UNIMARC.xlsx',  # noqa: E501
            'ila_m10': '/sites/BigDatayAdvancedAnalytics/Documentos compartidos/Paneles Power BI/Reporte de Margen/M10/Fuentes manuales de informacion/ILA/ILA_M10.xlsx' ,  # noqa: E501
            'ila_s10' : '/sites/BigDatayAdvancedAnalytics/Documentos compartidos/Paneles Power BI/Reporte de Margen/S10/Fuentes manuales de informacion/ILA/ILA_S10.xlsx',  # noqa: E501
            'admg': '/sites/BigDatayAdvancedAnalytics/Documentos%20compartidos/Paneles%20Power%20BI/Reporte%20de%20Margen/Unimarc/Fuentes%20datos%20manuales/Adicional%20al%20Margen/Adicional%20al%20margen.xlsx',  # noqa: E501
            'admg_alvi' : '/sites/BigDatayAdvancedAnalytics/Documentos compartidos/Paneles Power BI/Reporte de Margen/Alvi/Fuentes manuales de informacion/Adicional al margen/Adicional al margen Alvi.xlsx',  # noqa: E501
            'admg_m10' : '/sites/BigDatayAdvancedAnalytics/Documentos compartidos/Paneles Power BI/Reporte de Margen/M10/Fuentes manuales de informacion/Adicional al margen/Adicional al margen M10.xlsx',  # noqa: E501
            'admg_s10': '/sites/BigDatayAdvancedAnalytics/Documentos compartidos/Paneles Power BI/Reporte de Margen/S10/Fuentes manuales de informacion/Adicional al margen/Adicional al margen S10.xlsx'  # noqa: E501
            }
    #table definitions jsons
    jsons = {
        'ppto' : 'ppto_formatos_sellout_recupero_2025.json',
        'ppto_mg1' : 'ppto_mmpp_mg1.json',
        'est_com' : 'estructura_comercial.json',
        'est_com_alvi' : 'estructura_comercial_alvi.json',
        'ila' : 'ila_proyectado_unimarc.json',
        'ila_m10' : 'ila_proyectado_m10.json',
        'ila_s10' : 'ila_proyectado_s10.json',
        'admg' : 'adicional_al_margen_unimarc.json',
        'admg_alvi' : 'adicional_al_margen_alvi.json',
        'admg_m10' : 'adicional_al_margen_m10.json',
        'admg_s10' : 'adicional_al_margen_s10.json'

    }

    for file in load_files:
        logging.info(f'Starting extraction of {file} from Sharepoint')
        sharepoint = sp.SharePointFile(sp_cred['client_id'],
                                       sp_cred['client_secret'],input_files[file])
        sheet_name = 0
        if file == 'ppto_mg1':
            sheet_name = 'MG1'
        df_file = sharepoint.toFrame(sheet_name = sheet_name)
        df_file =cleaning_func(file,df_file)
        # Create GBQ table if does not exist
        logging.info('Creating GBQ table using JSON')
        gbq_extended.createTableFromJSON(
            ddl_json_config_path=os.path.join('gbq_objects', jsons[file]),
            project=gcp_project_id,
            gbq_client=gbq_client,
            if_exists='rebuild',
        )

        # Upload data
        logging.info('Uploading data')
        gbq_extended.uploadFrame(
            df_file,
            table_ddl_json_path=os.path.join('gbq_objects', jsons[file]),
            project=gcp_project_id,
            gbq_client=gbq_client,
            if_exists='append',
        )
    logging.info('Process ended!')


if __name__ == '__main__':
    main()
