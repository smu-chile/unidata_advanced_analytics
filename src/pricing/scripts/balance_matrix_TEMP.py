# Default
from __future__ import annotations

import io
import os
import logging
import argparse
from logging import config

import numpy as np

# Pip
import pandas as pd
from google.cloud.bigquery import Client

import common.office365_extended.sharepoint as sp

# Own
from common.constants import LOGGING_CONFIG
from common.databases.queries import QueryDict
from common.gcp_extended.bigquery import (
    uploadFrame,
    readBigQuery,
    deleteFromTable,
)
from common.gcp_extended.secretsmanager import getSecret


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
    '--store_banner', type=str,
    help='Store banner'
)

#######
# -------------------------------------------------------------------------
#  SQL Queries
# -------------------------------------------------------------------------
SQL_QUERIES = QueryDict({    # Region: Explicación de query

 'query_sensibilidad':
"""
SELECT * FROM `${proyecto}.PRECIO_PROMOCIONES.PRODUCT_SENSIBILITY`
where STORE_BANNER = '${store_banner}'
""",

'query_elasticidad':
"""
SELECT * FROM `${proyecto}.PRECIO_PROMOCIONES.ELASTICITY_PR`
where STORE_BANNER = '${store_banner}'
""",

'query_ventas':
"""
WITH tabla_fecha_max AS (
  SELECT
    MAX(P_DATE) AS fecha_max
  FROM `${proyecto}.TMP.TMP_REGRESSION_PROCESSED_DATA_ELASTICITY`
  WHERE STORE_BANNER = '${store_banner}'
)

SELECT
  MATERIAL,
  EAN,
  SUM(VENTAS_TOTALES_PRODUCTO) AS ventas_totales,
  MAX(SUB_CATEGORY_DESCRIPTION) AS SUB_CATEGORY_DESCRIPTION,
FROM `${proyecto}.TMP.TMP_REGRESSION_PROCESSED_DATA_ELASTICITY`
CROSS JOIN tabla_fecha_max
WHERE STORE_BANNER = '${store_banner}'
  AND P_DATE BETWEEN DATE_SUB(
  tabla_fecha_max.fecha_max, INTERVAL 12 MONTH) AND tabla_fecha_max.fecha_max
GROUP BY MATERIAL, EAN;

"""
})


# -------------------------------------------------------------------------
# Functions and Classes
# -------------------------------------------------------------------------

# Se agrega cuadrante de BM
def asignar_segmento_bm(row):
    if row['kvi'] == 'BKG' and row['segmento_elasticidad'] == 'high':
        return 'Hi-Lo'
    if row['kvi'] == 'BKG' and row['segmento_elasticidad'] == 'low':
        return 'Margin'
    if row['kvi'] in ['KCI', 'KVI'] and row['segmento_elasticidad'] == 'low':
        return 'EDLP'
    if row['kvi'] in ['KCI', 'KVI'] and row['segmento_elasticidad'] == 'high':
        return 'Low-Lower'
    return 'Otro'  # En caso de que haya algún valor inesperado

def asignar_segmento_bm_NUEVO_METODO(row):
    if row['NUEVOS_KVI'] == 'BKG' and row['segmento_elasticidad'] == 'high':
        return 'Hi-Lo'
    if row['NUEVOS_KVI'] == 'BKG' and row['segmento_elasticidad'] == 'low':
        return 'Margin'
    if row['NUEVOS_KVI'] in ['KCI', 'KVI'] and row['segmento_elasticidad'] == 'low':
        return 'EDLP'
    if row['NUEVOS_KVI'] in ['KCI', 'KVI'] and row['segmento_elasticidad'] == 'high':
        return 'Low-Lower'
    return 'Otro'  # En caso de que haya algún valor inesperado


# -------------------------------------------------------------------------
# Main function
# -------------------------------------------------------------------------
def main() -> None:  # noqa: D103


    # Parse input variables
    args = vars(parser.parse_args())
    execution_date: str = args['execution_date']
    proyecto: str = args['project_id']  # noqa: F841
    store_banner:str = args['store_banner']
    logging.info(f'execution_date: {execution_date}')
    logging.info(f'proyecto: {proyecto}')


    # Set gbq client for all subsequent queries
    gbq_client = Client()


    # REGION: Inputs del proceso
    #----------------------------------------------------------------------

    # Usuario
    usuario = 'balance_matrix'

    #~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    # ENDREGION

    # REGION: Querys de GCP
    #----------------------------------------------------------------------

    # SENSIBILIDAD

    query_sensibilidad = SQL_QUERIES['query_sensibilidad'].substitute(
        proyecto = proyecto,
        store_banner = store_banner)

    df_sensibilidad = readBigQuery(
            query=query_sensibilidad,
            user=usuario,
            gbq_client=gbq_client)

    print('[PARCHE] Query Sensibilidad Info: ')
    print(df_sensibilidad.info())

    df_sensibilidad.columns = df_sensibilidad.columns.str.lower()
    logging.info('Consulta de sensibilidad lista')

    # ELASTICIDAD

    query_elasticidad = SQL_QUERIES['query_elasticidad'].substitute(
        proyecto = proyecto,
        store_banner = store_banner)

    df_elasticidad = readBigQuery(
            query=query_elasticidad,
            user=usuario,
            gbq_client=gbq_client)

    print('[PARCHE] Query Elasticidad Info: ')
    print(df_elasticidad.info())

    df_elasticidad.columns = df_elasticidad.columns.str.lower()
    logging.info('Consulta de elasticidad lista')

    # VENTAS

    query_ventas = SQL_QUERIES['query_ventas'].substitute(
        proyecto = proyecto,
        store_banner = store_banner)

    df_ventas = readBigQuery(
            query=query_ventas,
            user=usuario,
            gbq_client=gbq_client)

    print('[PARCHE] Query Ventas Info: ')
    print(df_ventas.info())

    df_ventas.columns = df_ventas.columns.str.lower()
    logging.info('Consulta de ventas lista')

    #----------------------------------------------------------------------
    # ENDREGION


    # REGION: Se crea Balance Matrix BM
    #----------------------------------------------------------------------

    df_balance_matrix = df_elasticidad.merge(
        df_sensibilidad[['material', 'indice_sensibilidad', 'indice_sensibilidad_familia','kvi']],
        on='material',
        how='left'
    )

    mask_fillna_is  = df_balance_matrix['indice_sensibilidad'].isna()
    mask_fillna_isf = df_balance_matrix['indice_sensibilidad_familia'].isna()

    df_balance_matrix['indice_sensibilidad'] = df_balance_matrix['indice_sensibilidad'].fillna(0)

    #[PARCHE] En estrico rigor se debería rellenar ISF yendo a buscar los padres ¿?  # noqa: W505

    df_balance_matrix['indice_sensibilidad_familia'] = df_balance_matrix['indice_sensibilidad_familia'].fillna(0)  # noqa: E501

    print('#(IS nans) agregados: ', mask_fillna_is.sum())
    print('#(ISF nans) agregados: ', mask_fillna_isf.sum())

    df_balance_matrix['kvi'] = df_balance_matrix['kvi'].fillna('BKG')

    #Parche: agregamos columna subcat description
    df_balance_matrix = df_balance_matrix.merge(
          df_ventas[['ean','ventas_totales','sub_category_description']], on = 'ean', how='left')

    logging.info('Merge de tablas listo')

    #----------------------------------------------------------------------
    # ENDREGION

    # REGION: Se agregan parametros
    #----------------------------------------------------------------------

    # Para la sensibilidad se asignan los nombres del equipo Comercial
    mapa = {'KVI': 'SE', 'KCI': 'SG', 'BKG': 'FS'}
    pos = df_balance_matrix.columns.get_loc('kvi') + 1
    df_balance_matrix.insert(
        pos,
        'codigo_sensibilidad',
        df_balance_matrix['kvi'].astype(str).str.upper().str.strip().map(mapa))


    # Aplicar la función al dataframe
    df_balance_matrix['segmento_bm'] = df_balance_matrix.apply(asignar_segmento_bm, axis=1)

    logging.info('Parámetros adicionales listos')
    #----------------------------------------------------------------------
    # ENDREGION

    # REGION: Se ordenan las columnas
    #----------------------------------------------------------------------

    df_balance_matrix_sp = df_balance_matrix[['store_banner',
                                            'categoria',
                                            'sub_category_description',
                                            'descripcion_material',
                                            'material',
                                            'umv',
                                            'ean',
                                            'ventas_totales',
                                            'indice_sensibilidad',
                                            'indice_sensibilidad_familia',
                                            'elasticidad',
                                            'kvi',
                                            'codigo_sensibilidad',
                                            'segmento_elasticidad',
                                            'segmento_bm']]


    def clasificar_kvi(df_temp, ventas='ventas_totales',
                    sensibilidad='indice _sensibilidad_familia'):
        total_ventas = df_temp[ventas].sum()

        if total_ventas <= 0:
            msg = f"'{ventas}' debe sumar más de 0."
            raise ValueError(msg)

        df_temp = df_temp.sort_values(
            sensibilidad,
            ascending=False,
            kind='stable'
        ).copy()

        df_temp['pct_ventas'] = df_temp[ventas] / total_ventas
        df_temp['pct_ventas_acumulado'] = df_temp['pct_ventas'].cumsum()

        df_temp['NUEVOS_KVI'] = np.select(
            [
                df_temp['pct_ventas_acumulado'] <= 0.30,
                df_temp['pct_ventas_acumulado'] <= 0.60,
            ],
            ['KVI', 'KCI'],
            default='BKG'
        )

        return df_temp

    clasificar_kvi(df_balance_matrix_sp, sensibilidad='indice_sensibilidad_familia')
    df_balance_matrix['segmento_bm_new'] = df_balance_matrix_sp.apply(asignar_segmento_bm_NUEVO_METODO, axis=1)  # noqa: E501

    print('info df_temp post nuevos KVI: ', df_balance_matrix_sp.info())

    df_balance_matrix_sp = df_balance_matrix_sp.rename(columns={
        'store_banner':'Formato',
        'categoria':'Categoria',
        'sub_category_description': 'Grupo artículo',
        'descripcion_material': 'Descripción material',
        'material':'Material',
        'umv':'UMV',
        'ean':'EAN',
        'ventas_totales': 'Ventas EAN (12 meses)',
        'indice_sensibilidad': 'Índice sensibilidad',
        'indice_sensibilidad_familia': 'Índice sensibilidad familia',
        'elasticidad': 'Elasticidad',
        'kvi':'KVI',
        'codigo_sensibilidad': 'Código sensibilidad',
        'segmento_elasticidad': 'Segmento elasticidad',
        'segmento_bm': 'Segmento Balance Matrix',
        'segmento_bm_new': 'Segmento Balance Matrix Nuevo'
    })

    #df_balance_matrix_sp.sort_values(by='Categoria')  # noqa: ERA001

    logging.info('Cambio de nombres para Excel listo')
    #----------------------------------------------------------------------
    # ENDREGION


    # REGION: Se sube a sharepoint
    #----------------------------------------------------------------------

    # ordenar antes por Categoria

    # df_balance_matrix_sp = df_balance_matrix_sp.sort_values(by='Categoria')  # noqa: ERA001, W505

    print(f'[PARCHE] Balance Matrix Dimensiones: {df_balance_matrix_sp.shape}')
    cantidad_eliminadas = df_balance_matrix_sp['Elasticidad'].isna().sum()
    df_balance_matrix_sp = df_balance_matrix_sp[df_balance_matrix_sp['Elasticidad'].notna()]
    print(f'Se eliminaron {cantidad_eliminadas} filas con Elasticidad nula')
    print(f'[PARCHE] Balance Matrix Dimensiones: {df_balance_matrix_sp.shape}')


    buffer = io.BytesIO()

    with pd.ExcelWriter(buffer, engine='xlsxwriter') as writer:
        nombre_hoja = f'BM {store_banner}'
        df_balance_matrix_sp.to_excel(writer, index=False, sheet_name=nombre_hoja)
        sheet = writer.sheets[nombre_hoja]
        workbook = writer.book

        # formato general centrado
        formato_centrado = workbook.add_format({'align': 'center'})

        # formato dinero para Ventas EAN (12 meses)
        formato_moneda = workbook.add_format({
            'num_format': '$#,##0',
            'align': 'center'
        })

        # formato número para material
        formato_material = workbook.add_format({
            'num_format': '#,##0',
            'align': 'center'
        })

        columnas = list(df_balance_matrix_sp.columns)

        for i, col in enumerate(columnas):
            serie = df_balance_matrix_sp[col].astype(str)
            max_len = max(serie.map(len).max(), len(col))
            width = max_len + 2

            # aplicar formato según columna
            if col == 'Ventas EAN (12 meses)':
                sheet.set_column(i, i, width, formato_moneda)
            elif col == 'material':
                sheet.set_column(i, i, width, formato_material)
            else:
                sheet.set_column(i, i, width, formato_centrado)

        sheet.freeze_panes(1, 0)

    buffer.seek(0)



    sp.SharePointFile(
        **getSecret(
            'bdaa_sharepoint_credentials',
            proyecto,
        ),
        server_relative_path=(
            '/sites/'
            'BigDatayAdvancedAnalytics/'
            'Documentos%20compartidos/'
            'Pricing/'
            'Balance Matrix AA - GCP/'
            f'Balance_Matrix_AA_{store_banner}_TEMP_SENSIBILITY_3030.xlsx'
        )
    ).upload(buffer)
    logging.info('Tabla subida en Sharepoint')

    #----------------------------------------------------------------------
    # ENDREGION


    # REGION: Se sube a GCP
    #----------------------------------------------------------------------
    # Definir el WHERE
    where_clause = f"store_banner = '{store_banner}'"

    # Parametros
    esquema = 'PRECIO_PROMOCIONES'
    tabla = 'BALANCE_MATRIX_TEMP_3030'

    # Se elimina los datos para cierto store_banner y rango (si existen)
    deleteFromTable(table_ref=f'{proyecto}.{esquema}.{tabla}',
                    where_clause=where_clause,
                    gbq_client=gbq_client)



    # Se carga en BQ con los datos recalculados
    uploadFrame(
        df_balance_matrix_sp,
        table_ddl_json_path=os.path.join('gbq_objects',
                                         'ingest_product_balance_matrix_TEMP.json'),
        project=proyecto,
        gbq_client=gbq_client,
        if_exists='append'
    )

    logging.info('Se sube la tabla a GCP')

    #----------------------------------------------------------------------
    # ENDREGION

if __name__ == '__main__':
    main()
