# Default
from __future__ import annotations  # noqa: I001

import io
import os
import time
import logging
import argparse
from logging import config

import numpy as np

# Pip
import pandas as pd
import pendulum
import common.office365_extended.sharepoint as sp

# Own
from common.constants import LOGGING_CONFIG
from google.cloud.bigquery import Client
from common.databases.queries import QueryDict
from google.api_core.exceptions import Conflict
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
parser.add_argument(
    '--zona', type=str,
    help='Zona comercial (Unimarc)'
)
parser.add_argument(
    '--subir_a_sharepoint', type=str, default='False',
    help="'True' o 'False' -- si se sube el Excel a Sharepoint ademas de BigQuery"
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
FROM `${proyecto}.PRECIO_PROMOCIONES.PRODUCT_SENSIBILITY_ZONA`
where STORE_BANNER = '${store_banner}' AND ZONA = '${zona}'
""",

'query_elasticidad':
"""
SELECT * FROM `${proyecto}.PRECIO_PROMOCIONES.ELASTICITY_ZONA`
where STORE_BANNER = '${store_banner}' AND ZONA = '${zona}'
""",

'query_ventas':
"""
WITH tabla_fecha_max AS (
  SELECT
    MAX(P_DATE) AS fecha_max
  FROM `${proyecto}.PRECIO_PROMOCIONES.TMP_REGRESSION_PROCESSED_DATA_ELASTICITY_ZONA`
  WHERE STORE_BANNER = '${store_banner}' AND ZONA = '${zona}'
)

SELECT
  MATERIAL,
  EAN,
  SUM(VENTAS_TOTALES_PRODUCTO) AS ventas_totales,
  MAX(SUB_CATEGORY_DESCRIPTION) AS SUB_CATEGORY_DESCRIPTION,
FROM `${proyecto}.PRECIO_PROMOCIONES.TMP_REGRESSION_PROCESSED_DATA_ELASTICITY_ZONA`
CROSS JOIN tabla_fecha_max
WHERE STORE_BANNER = '${store_banner}' AND ZONA = '${zona}'
  AND P_DATE BETWEEN DATE_SUB(
  tabla_fecha_max.fecha_max, INTERVAL 12 MONTH) AND tabla_fecha_max.fecha_max
GROUP BY MATERIAL, EAN;

""",

'query_genfix':
"""
WITH ean_con_sensibilidad AS (
    SELECT
    DISTINCT( CAST(MATERIAL AS INT64) )  AS MATERIAL
    FROM `${proyecto}.PRECIO_PROMOCIONES.PRODUCT_SENSIBILITY_ZONA`
    where STORE_BANNER = '${store_banner}' AND ZONA = '${zona}'
    )

SELECT
    sku_padre,
    MATERIAL
FROM `${proyecto}.PRECIO_PROMOCIONES.TBL_PRICING_GENFIX`
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


def crear_kvi_con_contagio(
    BM: pd.DataFrame,  # noqa: N803
    genfix: pd.DataFrame,
    corte_kvi: float = 0.33,
    corte_kci: float = 0.66,
) -> pd.DataFrame:
    """Crea/reemplaza la columna 'NUEVOS_KVI' usando sensibilidad y ventas.

    Combina sensibilidad, ventas acumuladas y contagio por sku_padre
    -- requerimiento de negocio (mismo cambio aplicado a la version de
    banner completo).

    Clasificación base (a nivel de PRODUCTO, no de familia):
        - KVI: productos desde el inicio hasta alcanzar/superar corte_kvi.
        - KCI: productos posteriores a KVI hasta alcanzar/superar
          corte_kci.
        - BKG: productos restantes.

    Contagio (asimetrico, solo hacia KVI):
        - Identifica los sku_padre asociados a materiales KVI.
        - Todos los materiales de BM que compartan esos sku_padre
          pasan a ser KVI, aunque individualmente no calificaran.
        - 'flag_contagiados' identifica los KVI generados por contagio.

    NOTA (zona): el contagio via sku_padre NO esta acotado por zona --
    TBL_PRICING_GENFIX es una relacion de producto (sku_padre/material)
    sin dimension geografica, igual que en la version de banner
    completo. El contagio se calcula sobre los materiales de ESTA
    zona especifica (BM ya viene filtrado a la zona desde main()), asi
    que el resultado de por si queda acotado a esa zona.
    """
    # Validaciones
    columnas_bm = {
        'material',
        'indice_sensibilidad',
        'ventas_totales',
    }
    columnas_genfix = {
        'material',
        'sku_padre',
    }

    faltantes_bm = columnas_bm.difference(BM.columns)
    faltantes_genfix = columnas_genfix.difference(genfix.columns)

    if faltantes_bm:
        msg = f'BM no contiene las columnas requeridas: {sorted(faltantes_bm)}'
        raise ValueError(
            msg
        )

    if faltantes_genfix:
        msg = (
            'genfix no contiene las columnas requeridas: '
            f'{sorted(faltantes_genfix)}'
        )
        raise ValueError(
            msg
        )

    if not 0 < corte_kvi < corte_kci <= 1:
        msg = 'Los cortes deben cumplir: 0 < corte_kvi < corte_kci <= 1.'
        raise ValueError(
            msg
        )

    # Copias para no modificar los dataframes originales
    bm_kvi = BM.copy()
    genfix_kvi = genfix.copy()

    # Convertir ventas a formato numérico
    bm_kvi['ventas_totales'] = pd.to_numeric(
        bm_kvi['ventas_totales'],
        errors='coerce',
    ).fillna(0)

    # Calcular porcentaje de ventas por producto
    total_ventas = bm_kvi['ventas_totales'].sum()

    if total_ventas <= 0:
        msg = 'La suma de ventas_totales debe ser mayor que cero.'
        raise ValueError(
            msg
        )

    bm_kvi['pct_ventas'] = bm_kvi['ventas_totales'] / total_ventas

    # Normalizar materiales para cruzar BM con genfix
    bm_kvi['material'] = bm_kvi['material'].astype('string').str.strip()
    genfix_kvi['material'] = (
        genfix_kvi['material']
        .astype('string')
        .str.strip()
    )

    # Guardar el orden original para restaurarlo al finalizar
    bm_kvi['_orden_original'] = np.arange(len(bm_kvi))

    # Ordenar por sensibilidad y ventas como criterio de desempate
    bm_kvi = bm_kvi.sort_values(
        by=[
            'indice_sensibilidad',
            'pct_ventas',
            '_orden_original',
        ],
        ascending=[False, False, True],
        kind='mergesort',
    ).reset_index(drop=True)

    # Posición definitiva usada para clasificar KVI
    bm_kvi['orden_kvi'] = np.arange(1, len(bm_kvi) + 1)

    # Porcentaje acumulado de ventas según el orden anterior
    bm_kvi['pct_ventas_acumuladas'] = bm_kvi['pct_ventas'].cumsum()

    # Clasificación base: KVI / KCI / BKG
    bm_kvi['NUEVOS_KVI'] = 'BKG'

    # KVI: hasta incluir el producto que alcanza/supera corte_kvi
    supera_corte_kvi = bm_kvi['pct_ventas_acumuladas'].ge(corte_kvi)

    if supera_corte_kvi.any():
        pos_fin_kvi = supera_corte_kvi.idxmax()
        bm_kvi.loc[:pos_fin_kvi, 'NUEVOS_KVI'] = 'KVI'
    else:
        pos_fin_kvi = len(bm_kvi) - 1
        bm_kvi['NUEVOS_KVI'] = 'KVI'

    # KCI: desde después de KVI hasta incluir el producto que
    # alcanza/supera corte_kci
    supera_corte_kci = bm_kvi['pct_ventas_acumuladas'].ge(corte_kci)

    if supera_corte_kci.any():
        pos_fin_kci = supera_corte_kci.idxmax()
        inicio_kci = pos_fin_kvi + 1

        if inicio_kci <= pos_fin_kci:
            bm_kvi.loc[inicio_kci:pos_fin_kci, 'NUEVOS_KVI'] = 'KCI'

    # Materiales definidos como KVI antes del contagio
    materiales_kvi_originales = bm_kvi.loc[
        bm_kvi['NUEVOS_KVI'].eq('KVI'),
        'material',
    ].dropna().unique()

    # sku_padre asociados a los materiales KVI
    sku_padres_kvi = genfix_kvi.loc[
        genfix_kvi['material'].isin(materiales_kvi_originales),
        'sku_padre',
    ].dropna().unique()

    # Materiales asociados a sku_padre que contienen algún KVI
    materiales_hijos_kvi = genfix_kvi.loc[
        genfix_kvi['sku_padre'].isin(sku_padres_kvi),
        'material',
    ].dropna().unique()

    # Materiales de BM que comparten sku_padre con un KVI
    pertenece_familia_kvi = bm_kvi['material'].isin(materiales_hijos_kvi)

    # Flag: KVI generado por contagio, no por el corte inicial
    bm_kvi['flag_contagiados'] = (
        pertenece_familia_kvi
        & bm_kvi['NUEVOS_KVI'].ne('KVI')
    ).astype(int)

    # Aplicar contagio
    bm_kvi.loc[pertenece_familia_kvi, 'NUEVOS_KVI'] = 'KVI'

    # Restaurar orden original
    return (
        bm_kvi
        .sort_values('orden_kvi', ascending=True)
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
    zona: str = args['zona']
    subir_a_sharepoint = str(args['subir_a_sharepoint']).strip().lower() == 'true'
    # Granularidad MES, no dia -- '2026-07-02' -> '2026-07'. Mismo
    # patron que elasticidad_general.py/product_sensibility.py.
    periodo_ejecucion = pendulum.parse(execution_date).format('YYYY-MM')
    logging.info(f'execution_date: {execution_date}')
    logging.info(f'proyecto: {proyecto}')
    logging.info(f'zona: {zona}')


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
        store_banner = store_banner,
        zona = zona)

    df_sensibilidad = readBigQuery(
            query=query_sensibilidad,
            user=usuario,
            gbq_client=gbq_client)

    print('[PARCHE] Query Sensibilidad Info: ')
    print(df_sensibilidad.info())

    df_sensibilidad.columns = df_sensibilidad.columns.str.lower()
    # Forzar 'material' a string -- evita el ValueError de pandas al
    # unir columnas de distinto tipo (object vs Int64) mas adelante,
    # sin importar de que lado venga el tipo inesperado.
    df_sensibilidad['material'] = df_sensibilidad['material'].astype(str)
    logging.info('Consulta de sensibilidad lista')

    # ELASTICIDAD

    query_elasticidad = SQL_QUERIES['query_elasticidad'].substitute(
        proyecto = proyecto,
        store_banner = store_banner,
        zona = zona)

    df_elasticidad = readBigQuery(
            query=query_elasticidad,
            user=usuario,
            gbq_client=gbq_client)

    print('[PARCHE] Query Elasticidad Info: ')
    print(df_elasticidad.info())

    df_elasticidad.columns = df_elasticidad.columns.str.lower()
    df_elasticidad['material'] = df_elasticidad['material'].astype(str)
    logging.info('Consulta de elasticidad lista')

    # VENTAS

    query_ventas = SQL_QUERIES['query_ventas'].substitute(
        proyecto = proyecto,
        store_banner = store_banner,
        zona = zona)

    df_ventas = readBigQuery(
            query=query_ventas,
            user=usuario,
            gbq_client=gbq_client)

    print('[PARCHE] Query Ventas Info: ')
    print(df_ventas.info())

    df_ventas.columns = df_ventas.columns.str.lower()
    df_ventas['material'] = df_ventas['material'].astype(str)
    logging.info('Consulta de ventas lista')

    query_genfix = SQL_QUERIES['query_genfix'].substitute(
        proyecto = proyecto,
        store_banner = store_banner,
        zona = zona)

    df_genfix = readBigQuery(
            query=query_genfix,
            user=usuario,
            gbq_client=gbq_client)

    print('[PARCHE] Query Genfix Info: ')
    print(df_genfix.info())

    df_genfix.columns = df_genfix.columns.str.lower()
    df_genfix['material'] = df_genfix['material'].astype(str)
    logging.info('Consulta de genfix lista')

    #----------------------------------------------------------------------
    # ENDREGION


    # REGION: Se crea Balance Matrix BM
    #----------------------------------------------------------------------

    df_balance_matrix = df_elasticidad.merge(
        df_sensibilidad[['material', 'material_padre', 'indice_sensibilidad', 'indice_sensibilidad_familia','kvi']],  # noqa: E501
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

    # Periodo de ejecucion -- valor literal de execution_date (ej.
    # '2026-07-02'), no un mes agregado.
    # Granularidad MES, no dia -- ya calculado arriba periodo_ejecucion.
    df_balance_matrix['periodo_ejecucion'] = periodo_ejecucion
    df_balance_matrix['zona'] = zona

    df_balance_matrix_sp = df_balance_matrix[['store_banner',
                                            'zona',
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
                                            'kvi',
                                            'codigo_sensibilidad',
                                            'segmento_elasticidad',
                                            'segmento_bm',
                                            'periodo_ejecucion']]




    # CREACION KVI: cortes 0.33-0.66 + contagio por sku_padre --
    # requerimiento de negocio, reemplaza la clasificacion anterior
    # por familia (material_padre). Mismo cambio aplicado a la
    # version de banner completo.
    df_balance_matrix_sp = crear_kvi_con_contagio(
        BM=df_balance_matrix_sp,
        genfix=df_genfix,
        corte_kvi=0.33,
        corte_kci=0.66,
    )

    df_balance_matrix_sp = df_balance_matrix_sp.drop(columns=['material_padre'], errors='ignore')  # noqa: E501

    df_balance_matrix_sp['segmento_bm_new'] = df_balance_matrix_sp.apply(asignar_segmento_bm_NUEVO_METODO, axis=1)  # noqa: E501

    print('[PATCH N] Balance Matrix Info pre drop: ', df_balance_matrix_sp.info())
    df_balance_matrix_sp = df_balance_matrix_sp.drop(columns=['kvi', 'segmento_bm'], errors='ignore')  # noqa: E501, ERA001

    print('[PATCH N] Balance Matrix Info post drop: ', df_balance_matrix_sp.info())
    #Nota: codigo sensibilidad viene con la frecuencia de la sensibilidad
    # sin forzados
    mapa = {'BKG': 'FS', 'KCI': 'SG', 'KVI': 'SE'}
    df_balance_matrix_sp['codigo_sensibilidad'] = df_balance_matrix_sp['NUEVOS_KVI'].map(mapa)
    print('info df_temp post nuevos KVI: ', df_balance_matrix_sp.info())

    # Orden Columnas y renombramiento para Excel
    df_balance_matrix_sp = df_balance_matrix_sp[
        ['store_banner', 'zona', 'categoria', 'sub_category_description',
         'descripcion_material', 'material', 'umv', 'ean',
         'ventas_totales', 'indice_sensibilidad', 'indice_sensibilidad_familia',
         'elasticidad', 'NUEVOS_KVI','codigo_sensibilidad', 'segmento_elasticidad',
         'segmento_bm_new', 'pct_ventas', 'pct_ventas_acumulado', 'orden_kvi',
         'periodo_ejecucion']
    ]

    df_balance_matrix_sp = df_balance_matrix_sp.rename(columns={
        'store_banner':'Formato',
        'zona':'Zona',
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
        #'kvi':'KVI',  # noqa: ERA001
        'NUEVOS_KVI': 'KVI',
        'codigo_sensibilidad': 'Código sensibilidad',
        'segmento_elasticidad': 'Segmento elasticidad',
        # 'segmento_bm': 'Segmento Balance Matrix',  # noqa: ERA001
        'segmento_bm_new': 'Segmento Balance Matrix',
        'orden_kvi': 'Orden KVI',
        'periodo_ejecucion': 'Periodo Ejecución'
    })

    #df_balance_matrix_sp.sort_values(by='Categoria')  # noqa: ERA001

    logging.info('Cambio de nombres para Excel listo')
    #----------------------------------------------------------------------
    # ENDREGION


        # REGION: Se sube a sharepoint
    #----------------------------------------------------------------------

    print(f'[PARCHE] Balance Matrix Dimensiones: {df_balance_matrix_sp.shape}')
    cantidad_eliminadas = df_balance_matrix_sp['Elasticidad'].isna().sum()
    df_balance_matrix_sp = df_balance_matrix_sp[df_balance_matrix_sp['Elasticidad'].notna()]
    print(f'Se eliminaron {cantidad_eliminadas} filas con Elasticidad nula')
    print(f'[PARCHE] Balance Matrix Dimensiones: {df_balance_matrix_sp.shape}')

    if subir_a_sharepoint:
        buffer = io.BytesIO()

        with pd.ExcelWriter(buffer, engine='xlsxwriter') as writer:
            nombre_hoja = f'BM {store_banner} {zona}'
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
                f'Balance_Matrix_AA_{store_banner}_{zona}_{execution_date}_v3.xlsx'
            )
        ).upload(buffer)
        logging.info('Tabla subida en Sharepoint')
    else:
        logging.info('SUBIR_A_SHAREPOINT=False -- se omite la subida a Sharepoint.')

    #----------------------------------------------------------------------
    # ENDREGION


    # REGION: Se sube a GCP
    #----------------------------------------------------------------------
    # Definir el WHERE
    where_clause = (
        f"store_banner = '{store_banner}' AND zona = '{zona}' "
        f"AND periodo_ejecucion = '{periodo_ejecucion}'"
    )

    # Parametros
    esquema = 'PRECIO_PROMOCIONES'
    tabla = 'BALANCE_MATRIX_ZONA'

    # Se elimina los datos para cierto store_banner y rango (si existen)
    deleteFromTable(table_ref=f'{proyecto}.{esquema}.{tabla}',
                    where_clause=where_clause,
                    gbq_client=gbq_client)



    # Se carga en BQ con los datos recalculados
    #
    # Si varias zonas corren en simultaneo (concurrency>1) y la tabla
    # BALANCE_MATRIX_ZONA no existe todavia, es posible que 2 zonas
    # vean "la tabla no existe" al mismo tiempo y ambas intenten
    # crearla -- la 2da choca con un 409 Conflict genuino (condicion
    # de carrera de arranque, no un error de datos). Se reintenta 1
    # vez: para cuando se reintenta, la tabla ya deberia existir
    # (creada por la otra zona), y uploadFrame simplemente hace el
    # insert en vez de intentar crearla de nuevo.
    kwargs_upload = {
        'table_ddl_json_path': os.path.join(
            'gbq_objects', 'ingest_product_balance_matrix_zona.json'
        ),
        'project': proyecto,
        'gbq_client': gbq_client,
        'if_exists': 'append',
    }
    try:
        uploadFrame(df_balance_matrix_sp, **kwargs_upload)
    except Conflict:
        logging.warning(
            'uploadFrame choco con 409 Conflict al crear '
            'BALANCE_MATRIX_ZONA -- probable condicion de carrera con '
            'otra zona corriendo en simultaneo. Se espera 10s y se '
            'reintenta 1 vez.'
        )
        time.sleep(10)
        uploadFrame(df_balance_matrix_sp, **kwargs_upload)

    logging.info('Se sube la tabla a GCP')

    #----------------------------------------------------------------------
    # ENDREGION

if __name__ == '__main__':
    main()
