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
SELECT
* EXCEPT (MATERIAL),
  CAST(MATERIAL AS INT64) AS MATERIAL
FROM `${proyecto}.PRECIO_PROMOCIONES.PRODUCT_SENSIBILITY`
where STORE_BANNER = '${store_banner}'
""",

'query_elasticidad':
"""
SELECT * FROM `${proyecto}.PRECIO_PROMOCIONES.PRODUCT_ELASTICITY`
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

""",

'query_genfix':
"""
WITH ean_con_sensibilidad AS (
    SELECT
    DISTINCT( CAST(MATERIAL AS INT64) )  AS MATERIAL
    FROM `${proyecto}.PRECIO_PROMOCIONES.PRODUCT_SENSIBILITY`
    where STORE_BANNER = '${store_banner}'
    )

SELECT
    SKU_PADRE,
    MATERIAL
FROM `cl-bigdata-analytics-preprod.PRECIO_PROMOCIONES.TBL_PRICING_GENFIX`
"""
})


# -------------------------------------------------------------------------
# Functions and Classes
# -------------------------------------------------------------------------

# Se agrega cuadrante de BM
def asignar_segmento_bm(row):
    if row['KVI'] == 'BKG' and row['segmento_elasticidad'] == 'high':
        return 'Hi-Lo'
    if row['KVI'] == 'BKG' and row['segmento_elasticidad'] == 'low':
        return 'Margin'
    if row['KVI'] in ['KCI', 'KVI'] and row['segmento_elasticidad'] == 'low':
        return 'EDLP'
    if row['KVI'] in ['KCI', 'KVI'] and row['segmento_elasticidad'] == 'high':
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


def crear_kvi_con_contagio(
    BM: pd.DataFrame,  # noqa: N803
    genfix: pd.DataFrame,
    corte_kvi: float = 0.33,
    corte_kci: float = 0.66
) -> pd.DataFrame:
    """Crea/reemplaza la columna 'KVI' en BM usando:

    1. Orden descendente por 'Índice de sensibilidad'.
    2. Desempate descendente por 'pct_ventas'.
    3. Clasificación según venta acumulada:
       - KVI: hasta superar corte_kvi.
       - KCI: desde después de KVI hasta superar corte_kci.
       - BKG: productos restantes.
    4. Contagio de KVI:
       - Se identifican los SKU_PADRE asociados a los Material KVI.
       - Todos los Material de BM que compartan esos SKU_PADRE
         pasan a ser KVI.
    5. Crea 'flag_contagiados':
       - 1: el producto se convirtió en KVI por contagio.
       - 0: era KVI por el corte inicial o no fue contagiado.

    Parameters
    ----------
    BM : pd.DataFrame
        Debe contener las columnas:
        'Material', 'Índice de sensibilidad' y 'pct_ventas'.

    genfix : pd.DataFrame
        Debe contener las columnas:
        'Material' y 'SKU_PADRE'.

    corte_kvi : float, default=0.33
        Umbral acumulado de pct_ventas para definir KVI.

    corte_kci : float, default=0.66
        Umbral acumulado de pct_ventas para definir KCI.

    Returns
    -------
    pd.DataFrame
        Copia de BM con las columnas:
        'Orden KVI Nuevo',
        'pct_ventas_acumuladas',
        'KVI',
        'flag_contagiados'.
    """

    # Validaciones de columnas requeridas
    columnas_bm = {'Material', 'Índice de sensibilidad', 'pct_ventas'}
    columnas_genfix = {'Material', 'SKU_PADRE'}

    faltantes_bm = columnas_bm.difference(BM.columns)
    faltantes_genfix = columnas_genfix.difference(genfix.columns)

    if faltantes_bm:
        msg = f'BM no contiene las columnas requeridas: {sorted(faltantes_bm)}'
        raise ValueError(
            msg
        )

    if faltantes_genfix:
        msg_0 = (
            'genfix no contiene las columnas requeridas: '
            f'{sorted(faltantes_genfix)}'
        )
        raise ValueError(
            msg_0
        )

    if not 0 < corte_kvi < corte_kci <= 1:
        msg_1 = 'Los cortes deben cumplir: 0 < corte_kvi < corte_kci <= 1.'
        raise ValueError(
            msg_1
        )

    # Copias para no modificar los dataframes originales
    bm_kvi = BM.copy()
    genfix_kvi = genfix.copy()

    # Asegurar que pct_ventas sea numérico
    bm_kvi['pct_ventas'] = pd.to_numeric(
        bm_kvi['pct_ventas'],
        errors='coerce'
    ).fillna(0)

    # Normalizar Material para realizar cruces seguros
    bm_kvi['Material'] = bm_kvi['Material'].astype('string').str.strip()
    genfix_kvi['Material'] = (
        genfix_kvi['Material']
        .astype('string')
        .str.strip()
    )

    # Mantener orden original de BM
    bm_kvi['_orden_original'] = np.arange(len(bm_kvi))

    # Ordenar por sensibilidad y, ante empate, por pct_ventas
    bm_kvi = bm_kvi.sort_values(
        by=['Índice de sensibilidad', 'pct_ventas', '_orden_original'],
        ascending=[False, False, True],
        kind='mergesort'
    ).reset_index(drop=True)

    # Guardar orden final de priorización
    bm_kvi['orden_kvi'] = np.arange(1, len(bm_kvi) + 1)

    # Calcular ventas acumuladas según el nuevo orden
    bm_kvi['pct_ventas_acumuladas'] = bm_kvi['pct_ventas'].cumsum()

    # Clasificación inicial
    bm_kvi['KVI'] = 'BKG'

    # Posición que supera/alcanza el corte KVI
    supera_corte_kvi = bm_kvi['pct_ventas_acumuladas'].ge(corte_kvi)

    if supera_corte_kvi.any():
        pos_fin_kvi = supera_corte_kvi.idxmax()
        bm_kvi.loc[:pos_fin_kvi, 'KVI'] = 'KVI'
    else:
        # Si las ventas acumuladas no llegan al corte, todos son KVI
        bm_kvi['KVI'] = 'KVI'

    # Posición que supera/alcanza el corte KCI
    supera_corte_kci = bm_kvi['pct_ventas_acumuladas'].ge(corte_kci)

    if supera_corte_kci.any():
        pos_fin_kci = supera_corte_kci.idxmax()

        # Clasifica como KCI desde el siguiente registro posterior
        # al bloque KVI hasta el registro que alcanza/supera corte_kci.
        inicio_kci = pos_fin_kvi + 1 if supera_corte_kvi.any() else len(bm_kvi)

        if inicio_kci <= pos_fin_kci:
            bm_kvi.loc[inicio_kci:pos_fin_kci, 'KVI'] = 'KCI'

    # -------------------------------------------------
    # CONTAGIO KVI POR SKU_PADRE
    # -------------------------------------------------

    # Materiales KVI definidos directamente por el corte de ventas
    materiales_kvi_originales = bm_kvi.loc[
        bm_kvi['KVI'].eq('KVI'),
        'Material'
    ].dropna().unique()

    # SKU_PADRE asociados a los materiales KVI
    sku_padres_kvi = genfix_kvi.loc[
        genfix_kvi['Material'].isin(materiales_kvi_originales),
        'SKU_PADRE'
    ].dropna().unique()

    # Todos los materiales que pertenecen a SKU_PADRE con al menos un KVI
    materiales_hijos_kvi = genfix_kvi.loc[
        genfix_kvi['SKU_PADRE'].isin(sku_padres_kvi),
        'Material'
    ].dropna().unique()

    # Materiales de BM que comparten SKU_PADRE con un KVI
    es_hijo_de_kvi = bm_kvi['Material'].isin(materiales_hijos_kvi)

    # Se marca solamente a los productos que no eran KVI por corte
    bm_kvi['flag_contagiados'] = (
        es_hijo_de_kvi
        & bm_kvi['KVI'].ne('KVI')
    ).astype(int)

    # Contagio: convertir hijos de SKU_PADRE KVI en KVI
    bm_kvi.loc[es_hijo_de_kvi, 'KVI'] = 'KVI'

    # Restaurar el orden original de BM
    return (
        bm_kvi
        .sort_values('_orden_original')
        .drop(columns='_orden_original')
        .reset_index(drop=True)
    )


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

    print('\n' + '=' * 70)
    logging.info('PARTE 1: EJECUCIÓN QUERYS')
    print('=' * 70)

    print('P1.1: QUERY SENSIBILIDAD (1/4)')
    query_sensibilidad = SQL_QUERIES['query_sensibilidad'].substitute(
        proyecto = proyecto,
        store_banner = store_banner)

    df_sensibilidad = readBigQuery(
            query=query_sensibilidad,
            user=usuario,
            gbq_client=gbq_client)

    print('Dimensiones df: ', df_sensibilidad.shape)
    print('Cantidad de eans únicos: ', df_sensibilidad['EAN'].nunique())
    print('Query Sensibilidad Info: \n', df_sensibilidad.info())

    df_sensibilidad.columns = df_sensibilidad.columns.str.lower()


    # ELASTICIDAD

    print('=' * 70)
    print('P1.1: QUERY ELASTICIDAD (2/4)')
    query_elasticidad = SQL_QUERIES['query_elasticidad'].substitute(
        proyecto = proyecto,
        store_banner = store_banner)

    df_elasticidad = readBigQuery(
            query=query_elasticidad,
            user=usuario,
            gbq_client=gbq_client)

    print('Dimensiones df: ', df_elasticidad.shape)
    print('Cantidad de eans únicos: ', df_elasticidad['EAN'].nunique())
    print('Query Elasticidad Info: \n', df_elasticidad.info())

    df_elasticidad.columns = df_elasticidad.columns.str.lower()



    # VENTAS

    print('=' * 70)
    print('P1.1: QUERY VENTAS (3/4)')

    query_ventas = SQL_QUERIES['query_ventas'].substitute(
        proyecto = proyecto,
        store_banner = store_banner)

    df_ventas = readBigQuery(
            query=query_ventas,
            user=usuario,
            gbq_client=gbq_client)

    print('Dimensiones df: ', df_ventas.shape)
    print('Cantidad de eans únicos: ', df_ventas['EAN'].nunique())
    print('Query Ventas Info: \n', df_ventas.info())

    df_ventas.columns = df_ventas.columns.str.lower()

    print('=' * 70)
    print('P1.1: QUERY GENFIX (4/4)')

    query_genfix = SQL_QUERIES['query_genfix'].substitute(
        proyecto = proyecto,
        store_banner = store_banner)

    df_genfix = readBigQuery(
            query=query_genfix,
            user=usuario,
            gbq_client=gbq_client)

    print('Dimensiones df: ', df_genfix.shape)
    print('Cantidad de materiales únicos: ', df_genfix['MATERIAL'].nunique())
    print('Query Genfix Info: \n', df_genfix.info())

    df_genfix.columns = df_genfix.columns.str.lower()

    print('=' * 70)

    #----------------------------------------------------------------------
    # ENDREGION


    # REGION: Se crea Balance Matrix BM
    #----------------------------------------------------------------------

    df_balance_matrix = df_elasticidad.merge(
        df_sensibilidad[['material', 'material_padre', 'indice_sensibilidad', 'indice_sensibilidad_familia']],  # noqa: E501
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


    #----------------------------------------------------------------------
    # CREACIÓN KVI: cortes 0.33-0.66 + nuevo contagio

    df_balance_matrix = crear_kvi_con_contagio(
        BM=df_balance_matrix,
        genfix=df_genfix,
        corte_kvi=0.33,
        corte_kci=0.66
    )
    # REGION: Se agregan parametros
    #----------------------------------------------------------------------

    # Para la sensibilidad se asignan los nombres del equipo Comercial
    mapa = {'KVI': 'SE', 'KCI': 'SG', 'BKG': 'FS'}
    pos = df_balance_matrix.columns.get_loc('KVI') + 1
    df_balance_matrix.insert(
        pos,
        'codigo_sensibilidad',
        df_balance_matrix['KVI'].astype(str).str.upper().str.strip().map(mapa))


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
                                            'material_padre',
                                            'elasticidad',
                                            'KVI',
                                            'codigo_sensibilidad',
                                            'segmento_elasticidad',
                                            'segmento_bm']]


    # Orden Columnas y renombramiento para Excel
    df_balance_matrix_sp = df_balance_matrix_sp[
        ['store_banner', 'categoria', 'sub_category_description',
         'descripcion_material', 'material', 'umv', 'ean',
         'ventas_totales', 'indice_sensibilidad', 'indice_sensibilidad_familia',
         'elasticidad','codigo_sensibilidad', 'segmento_elasticidad',
         'pct_ventas', 'pct_ventas_acumulado', 'orden_kvi']
    ]

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
        'codigo_sensibilidad': 'Código sensibilidad',
        'segmento_elasticidad': 'Segmento elasticidad',
        'segmento_bm': 'Segmento Balance Matrix',
        'orden_kvi': 'Orden KVI'
    })

    #df_balance_matrix_sp.sort_values(by='Categoria')  # noqa: ERA001

    logging.info('Cambio de nombres para Excel listo')
    #----------------------------------------------------------------------
    # ENDREGION


    # REGION: Se sube a sharepoint
    #----------------------------------------------------------------------

    # ordenar antes por Categoria

    #df_balance_matrix_sp = df_balance_matrix_sp.sort_values(by='Categoria')  # noqa: ERA001, W505

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
            f'Balance_Matrix_AA_{store_banner}_{execution_date}_last_bm.xlsx'
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
    tabla = 'BALANCE_MATRIX_TESTING_LAST_BM_JULIO_2026'

    # Se elimina los datos para cierto store_banner y rango (si existen)
    deleteFromTable(table_ref=f'{proyecto}.{esquema}.{tabla}',
                    where_clause=where_clause,
                    gbq_client=gbq_client)



    # Se carga en BQ con los datos recalculados
    uploadFrame(
        df_balance_matrix_sp,
        table_ddl_json_path=os.path.join('gbq_objects',
                                         'ingest_product_balance_matrix.json'),
        project=proyecto,
        gbq_client=gbq_client,
        if_exists='append'
    )

    logging.info('Se sube la tabla a GCP')

    #----------------------------------------------------------------------
    # ENDREGION

if __name__ == '__main__':
    main()
