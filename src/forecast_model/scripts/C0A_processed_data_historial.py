from __future__ import annotations

import os
import logging
import argparse
from logging import config
from datetime import date, timedelta

# Pip
import pandas as pd
import pendulum
from google.cloud.bigquery import Client
from google.api_core.exceptions import NotFound

# Own
from common.constants import LOGGING_CONFIG
from common.databases.queries import QueryDict
from common.gcp_extended.bigquery import (
    uploadFrame,
    readBigQuery,
    setTableExpiration,
    createTableAsSelect,
)


# =========================================================================
#  INPUTS (todo lo modificable está en esta sección)
# =========================================================================

# --- Argumentos que entrega el DAG ---
parser = argparse.ArgumentParser()
parser.add_argument('--project_id', type=str,
                    help='GCP project in which the script will be executed')
parser.add_argument('--execution_date', type=str, help='DAG execution date')
parser.add_argument('--store_banner', type=str, help='Store banner')
parser.add_argument('--use', type=str, default='FORECAST', help='Solo FORECAST')

# --- Ventana de historia (M meses hacia atrás desde execution_date - 1)
MESES_HISTORIA = 24


# --- Tabla destino (solo append).Debe coincidir con la definida en el JSON
ESQUEMA_DESTINO = 'FORECAST_ML'
TABLA_DESTINO = 'TMP_HISTORIALES_TESTEO'
NOMBRE_JSON = 'C0A_processed_data_historial.json'
# --- Tabla auxiliar (maestra temporal) ---
ESQUEMA_AUX = 'TMP'
EXPIRACION_AUX_MINUTOS = 200

# --- Usuario BigQuery ---
USUARIO = 'pricing'

# --- Filtros de limpieza ---
PRECIO_MAXIMO = 1000000
CANTIDAD_MINIMA = 3          # No aplica a KG / KGV

# --- Sustitutos ---
CANT_SUST = 20

# --- Apoteósicos ---
CODIGO_APO = {'Unimarc': 1000}   # Formatos con apoteósicos y su organizacion_ventas
FECHA_INICIAL_APO = None         # None = inicio de la ventana. '2023-02-01' = original

# --- Columnas finales ---
COLUMNAS_FINALES = [
    'store_banner',
    'category_description', 'sub_category_description', 'material', 'product_description',
    'ean', 'sales_uom', 'sales_unit', 'p_date',
    'ventas_totales_producto', 'cantidad_total', 'precio_promedio',
    'apo',
    *[f'ean_sustituto_{i}' for i in range(1, CANT_SUST + 1)],
]

# =========================================================================
#  Config
# =========================================================================
config.dictConfig(LOGGING_CONFIG)


# =========================================================================
#  SQL Queries
# =========================================================================
SQL_QUERIES = QueryDict({
'query_master_table':
"""
-- Eliminar duplicidades de la dim products + ean_default
WITH distinct_products AS (
  SELECT DISTINCT
    EAN,
    CAT_DSC AS CATEGORY_DESCRIPTION,
    GRUPO_DSC AS SUB_CATEGORY_DESCRIPTION,
    NM AS PRODUCT_DESCRIPTION,
    SKU_PRODUCT AS PRODUCT_ID,
    NEG_DSC,
    CONTENIDO_BRUTO,
    CONT_CONV_UMB AS sales_unit,
    UNIDAD_DE_MEDIDA AS sales_uom,
    -- EAN por defecto dentro del grupo (SKU_PRODUCT, UNIDAD_DE_MEDIDA),
    -- priorizando el que tiene INDIC_EAN_PPAL='X'
    FIRST_VALUE(EAN) OVER (
      PARTITION BY SKU_PRODUCT, UNIDAD_DE_MEDIDA
      ORDER BY CASE WHEN INDIC_EAN_PPAL = 'X' THEN 0 ELSE 1 END, EAN
    ) AS ean_default
  FROM `${proyecto}.CDA_VISTAS.VW_DIM_PRODUCT`
)

-- Consulta principal
SELECT
  A.CUSTOMER_KEY AS CUSTOMER_ID,
  A.STORE_ID,
  A.MARKET_BASKET_KEY,
  P.PRODUCT_DESCRIPTION,
  P.PRODUCT_ID,
  -- usar el EAN canónico
  P.ean_default AS EAN,
  P.CATEGORY_DESCRIPTION,
  P.SUB_CATEGORY_DESCRIPTION,
  A.QUANTITY,
  A.VALUE,
  P.sales_uom,
  P.sales_unit,
  CAST(P.CONTENIDO_BRUTO AS NUMERIC) * CAST(P.sales_unit AS INTEGER) AS WEIGHT_UPC,
  CAST(A.WEIGHT AS NUMERIC) AS SALE_WEIGHT,
  A.TRANSACTION_DATE AS P_DATE
FROM `${proyecto}.CDA_VISTAS.VW_SALES_ITEM` A
INNER JOIN distinct_products P
  ON A.EAN = P.EAN
INNER JOIN `${proyecto}.CDA_VISTAS.VW_DIM_STORE` D
  ON A.STORE_ID = D.STORE_ID
WHERE
  A.TRANSACTION_DATE >= DATE('${fecha_inicial}')
  AND A.TRANSACTION_DATE <= DATE('${fecha_final}')
  -- Solo las fechas faltantes en la tabla destino
  AND A.TRANSACTION_DATE IN (${lista_fechas})
  AND A.SKU_PRODUCT IS NOT NULL
  AND A.SKU_PRODUCT != 'None'
  AND A.TRANSACTION_TYPE IN ('BX', 'BE', 'TF')
  AND A.ITM_TXN_FCN_TP_DSC = 'V'
  AND A.UNIT_PRICE > 0
  AND A.VALUE > 0
  AND P.NEG_DSC NOT IN ('SERVICIOS COMERCIALES', 'NO RETAIL', 'None')
  AND A.MARKET_BASKET_KEY NOT IN (
    SELECT MARKET_BASKET_KEY
    FROM `cl-cda-prod.DS_CDA_VW_SMU.DW_VW_FACT_MARKET_BASKET_E_COMMERCE`
    WHERE CANAL_VENTA IN ('PEDIDOS YA','UBER EATS','RAPPI','RAPPI TURBO')
  )
  AND D.STORE_BANNER = '${store_banner}'
""",

'query_principal':
"""
SELECT
  P_DATE,
  CATEGORY_DESCRIPTION,
  SUB_CATEGORY_DESCRIPTION,
  PRODUCT_ID,
  PRODUCT_DESCRIPTION,
  EAN,
  sales_uom,
  sales_unit,
  WEIGHT_UPC,
  SUM(`VALUE`) AS ventas_totales_producto,

  -- Cantidad total por unidad correcta (segura ante divisiones por 0)
  SUM(
    CASE
      WHEN sales_uom IN ('KG','KGV') THEN SALE_WEIGHT
      ELSE SAFE_DIVIDE(QUANTITY, CAST(sales_unit AS INT64))
    END
  ) AS cantidad_total,

  -- Precio promedio por unidad correcta (ignora casos con denominador 0)
  AVG(
    SAFE_DIVIDE(
      `VALUE`,
      CASE
        WHEN sales_uom IN ('KG','KGV') THEN SALE_WEIGHT
        ELSE QUANTITY / CAST(sales_unit AS INT64)
      END
    )
  ) AS precio_promedio

FROM ${table_master}
GROUP BY
  CATEGORY_DESCRIPTION,
  SUB_CATEGORY_DESCRIPTION,

  PRODUCT_ID,
  PRODUCT_DESCRIPTION,
  EAN,

  sales_unit,
  sales_uom,
  WEIGHT_UPC,
  P_DATE
ORDER BY
  PRODUCT_ID,
  P_DATE
""",

'query_apoteosico':
"""
SELECT material, fecha_inicio_de_promocion,fecha_fin_de_promocion
FROM `${proyecto}.CDA_VISTAS.VW_FACT_WORKFLOW`
WHERE FECHA_INICIO_DE_PROMOCION >  DATE('${fecha_inicial_ano}')
  AND FECHA_INICIO_DE_PROMOCION < DATE_ADD(DATE('${fecha_inicial_ano}'),
  INTERVAL ${cant_meses} MONTH)
  AND descripcion_evento_promocional = 'UNI APOTEOSICO'
  AND registro_valido = 'X'
  AND organizacion_ventas='${store_banner_codigo}'
  AND canal_distribucion='10'
ORDER BY fecha_fin_de_promocion
""",

'query_athena_sust':
"""
SELECT
    sku as material,
    substitute,
    substitution_score as score,
    substitution_rank as relevance,
    FORMAT_DATE('%Y%m', date) as p_month
FROM `cl-bigdata-analytics-preprod.ML_LAB.SKU_SUBSTITUTES_BY_CATEGORY`
WHERE store_banner = '${store_banner}'
AND FORMAT_DATE('%Y%m', date) >= '${first_month}'
AND FORMAT_DATE('%Y%m', date) <= '${last_month}'
AND substitution_rank <= ${cant_sust}
""",

# Fechas ya cargadas en la tabla destino dentro de la ventana
'query_fechas_existentes':
"""
SELECT DISTINCT CAST(p_date AS DATE) AS p_date
FROM `${tabla_destino}`
WHERE store_banner = '${store_banner}'
  AND CAST(p_date AS DATE) BETWEEN DATE('${fecha_inicial}') AND DATE('${fecha_final}')
""",

# Material-EAN-peso de los EANs ya cargados en la ventana(candidatos a sus)
'query_material_ean_historico':
"""
WITH distinct_products AS (
  SELECT DISTINCT
    EAN,
    SKU_PRODUCT AS PRODUCT_ID,
    NEG_DSC,
    CONTENIDO_BRUTO,
    CONT_CONV_UMB AS sales_unit,
    FIRST_VALUE(EAN) OVER (
      PARTITION BY SKU_PRODUCT, UNIDAD_DE_MEDIDA
      ORDER BY CASE WHEN INDIC_EAN_PPAL = 'X' THEN 0 ELSE 1 END, EAN
    ) AS ean_default
  FROM `${proyecto}.CDA_VISTAS.VW_DIM_PRODUCT`
)
SELECT DISTINCT
  P.PRODUCT_ID AS material,
  CAST(P.ean_default AS STRING) AS ean,
  SAFE_CAST(P.CONTENIDO_BRUTO AS NUMERIC) * SAFE_CAST(P.sales_unit AS INTEGER) AS weight_upc
FROM distinct_products P
WHERE P.NEG_DSC NOT IN ('SERVICIOS COMERCIALES', 'NO RETAIL', 'None')
  AND CAST(P.ean_default AS STRING) IN (
    SELECT DISTINCT CAST(ean AS STRING)
    FROM `${tabla_destino}`
    WHERE store_banner = '${store_banner}'
      AND CAST(p_date AS DATE) BETWEEN DATE('${fecha_inicial}') AND DATE('${fecha_final}')
  )
""",
})


# =========================================================================
#  Functions
# =========================================================================
def aFecha(fecha) -> date:
    """Convierte pendulum/datetime a datetime.date."""
    return date(fecha.year, fecha.month, fecha.day)


def rangoFechas(desde: date, hasta: date) -> set[date]:
    """Conjunto de fechas entre desde y hasta (inclusive)."""
    return {desde + timedelta(days=i) for i in range((hasta - desde).days + 1)}


def listaFechasSql(fechas: list[date]) -> str:
    """Lista de fechas en formato SQL:
    DATE(2026, 10, 1), DATE(2026, 10, 2), ..."""
    return ', '.join(f'DATE({f.year}, {f.month}, {f.day})' for f in fechas)


def tablaExiste(gbq_client: Client, table_ref: str) -> bool:
    """True si la tabla existe en BigQuery."""
    try:
        gbq_client.get_table(table_ref)
    except NotFound:
        return False
    return True


def obtenerFechasFaltantes(gbq_client: Client, tabla_destino: str,
                           store_banner: str, fecha_inicial: date,
                           fecha_final: date, existe_destino: bool) -> list[date]:
    """Fechas de la ventana que aún no tienen filas en la tabla destino."""
    fechas_ventana = rangoFechas(fecha_inicial, fecha_final)
    if not existe_destino:
        return sorted(fechas_ventana)

    df_fechas = readBigQuery(
        query=SQL_QUERIES['query_fechas_existentes'].substitute(
            tabla_destino=tabla_destino,
            store_banner=store_banner,
            fecha_inicial=fecha_inicial,
            fecha_final=fecha_final),
        user=USUARIO,
        gbq_client=gbq_client)

    df_fechas.columns = df_fechas.columns.str.lower()
    fechas_existentes = set(pd.to_datetime(df_fechas['p_date']).dt.date)
    return sorted(fechas_ventana - fechas_existentes)


def obtenerParejasPorRelevance(df_sust: pd.DataFrame,
                               df_material_ean: pd.DataFrame,
                               relevance_val: int) -> pd.DataFrame:
    """Obtiene la mejor pareja sustituta por relevancia y peso.

    Filtra por relevance, une datos de material y sustituto,
    y selecciona la pareja con menor diferencia de peso. En otras palabras
    transforma el sustituto material a sustituto ean según el EAN con mayor
    similitud de peso.
    """
    df_sust_x = df_sust[df_sust['relevance'] == relevance_val].copy()

    df_ean = df_material_ean.rename(columns={
        'material': 'material_original',
        'ean': 'ean_original',
        'weight_upc': 'weight_original'
    })
    df_ean_sub = df_material_ean.rename(columns={
        'material': 'material_substituto',
        'ean': 'ean_substituto',
        'weight_upc': 'weight_sustituto'
    })

    df_expandido = df_sust_x.merge(df_ean, left_on='material',
                                   right_on='material_original', how='left')
    df_expandido = df_expandido.merge(df_ean_sub, left_on='substitute',
                                      right_on='material_substituto', how='left')

    df_expandido = df_expandido.dropna(subset=['weight_original', 'weight_sustituto'])

    df_expandido['diferencia_peso'] = (
        df_expandido['weight_original'] - df_expandido['weight_sustituto']).abs()

    idx_min = df_expandido.groupby(['ean_original', 'p_month'])['diferencia_peso'].idxmin()

    columnas_necesarias = [
        'ean_original', 'ean_substituto', 'material', 'substitute',
        'score', 'p_month', 'weight_original', 'weight_sustituto', 'diferencia_peso'
    ]
    columnas_disponibles = [col for col in columnas_necesarias if col in df_expandido.columns]

    df_parejas = df_expandido.loc[idx_min, columnas_disponibles].reset_index(drop=True)

    return df_parejas.rename(columns={
        'ean_original': 'ean',
        'ean_substituto': f'ean_sustituto_{relevance_val}',
        'material': 'material',
        'substitute': f'material_sustituto_{relevance_val}'
    })


# =========================================================================
#  Main function
# =========================================================================
def main() -> None:  # noqa: D103

    # Parse input variables
    args = vars(parser.parse_args())
    execution_date: str = args['execution_date']
    proyecto: str = args['project_id']
    store_banner: str = args['store_banner']
    use: str = args['use']
    logging.info(f'execution_date: {execution_date}')
    logging.info(f'proyecto: {proyecto}')

    if use != 'FORECAST':
        msg = f'Este script solo soporta use = FORECAST (recibido: {use})'
        raise ValueError(msg)

    gbq_client = Client()

    # REGION: Tablas  # noqa: ERA001
    # ---------------------------------------------------------------------
    store_banner_tabla = 'Super_10' if store_banner == 'Super 10' else store_banner
    tmp_path_table_aux = (
        f'{proyecto}.{ESQUEMA_AUX}.TMP_REGRESSION_DATA_{use}_aux2_{store_banner_tabla}')
    tabla_destino = f'{proyecto}.{ESQUEMA_DESTINO}.{TABLA_DESTINO}'
    # ENDREGION

    # REGION: Ventana temporal
    # ---------------------------------------------------------------------
    fecha_ejecucion = pendulum.parse(execution_date)
    fecha_final_p = fecha_ejecucion.subtract(days=1)
    fecha_inicial_p = fecha_final_p.subtract(months=MESES_HISTORIA).add(days=1)
    fecha_final = aFecha(fecha_final_p)
    fecha_inicial = aFecha(fecha_inicial_p)

    logging.info(f'Se inicia el proceso para {store_banner.upper()}')
    logging.info(f'Ventana requerida: {fecha_inicial} -> {fecha_final}')
    # ENDREGION

    # REGION: (1) Existe la tabla? (2) Qué fechas faltan?
    # ---------------------------------------------------------------------
    existe_destino = tablaExiste(gbq_client, tabla_destino)
    logging.info(f'Tabla destino existe: {existe_destino}')

    fechas_faltantes = obtenerFechasFaltantes(
        gbq_client, tabla_destino, store_banner,
        fecha_inicial, fecha_final, existe_destino)

    if not fechas_faltantes:
        logging.info('La historia está completa. No hay fechas que agregar.')
        return

    fecha_min, fecha_max = fechas_faltantes[0], fechas_faltantes[-1]
    logging.info(f'Fechas faltantes: {len(fechas_faltantes)} ({fecha_min} -> {fecha_max})')
    # ENDREGION

    # REGION: Query principal
    # ---------------------------------------------------------------------
    query_master_table = SQL_QUERIES['query_master_table'].substitute(
        fecha_inicial=fecha_min,
        fecha_final=fecha_max,
        lista_fechas=listaFechasSql(fechas_faltantes),
        store_banner=store_banner,
        proyecto=proyecto
    )

    createTableAsSelect(query=query_master_table,
                        gbq_client=gbq_client,
                        table_ref=tmp_path_table_aux,
                        use_legacy_sql=False)
    logging.info('Tabla auxiliar/maestra creada...')

    setTableExpiration(
        table_ref=tmp_path_table_aux,
        expiration=pendulum.now().add(minutes=EXPIRACION_AUX_MINUTOS),
        gbq_client=gbq_client
    )
    logging.info('Se setea la expiracion de la tabla maestra...')

    query_principal = SQL_QUERIES['query_principal'].substitute(
        table_master=tmp_path_table_aux
    )
    df_datos = readBigQuery(
        query=query_principal,
        user=USUARIO,
        gbq_client=gbq_client
    )
    logging.info('Consulta principal lista...')

    if df_datos.empty:
        logging.info('La fuente no tiene ventas para las fechas faltantes. Nada que agregar.')
        return
    # ENDREGION

    # REGION: Se limpian y configuran los datos
    # ---------------------------------------------------------------------
    df_datos.columns = df_datos.columns.str.lower()
    df_datos['p_date'] = pd.to_datetime(df_datos['p_date'], format='%Y-%m-%d')
    df_datos['store_banner'] = store_banner

    df_datos = df_datos.rename(columns={'product_id': 'material'})
    df_datos['material'] = df_datos['material'].astype(int)
    df_datos['ean'] = df_datos['ean'].astype(str)

    # Eliminar precios con precio infinito
    df_datos = df_datos[df_datos['precio_promedio'] < PRECIO_MAXIMO].copy()

    df_datos['precio_promedio'] = df_datos['precio_promedio'].astype(int)
    df_datos['sales_unit'] = df_datos['sales_unit'].astype(int)
    df_datos['ventas_totales_producto'] = df_datos['ventas_totales_producto'].astype(int)
    df_datos['cantidad_total'] = df_datos['cantidad_total'].astype(float).round(2)
    df_datos = df_datos[df_datos['cantidad_total'] > 0].copy()
    df_datos['weight_upc'] = df_datos['weight_upc'].astype(float)

    # p_month solo se usa internamente para cruzar sus (no se reporta)
    df_datos['p_month'] = df_datos['p_date'].dt.strftime('%Y%m').astype(int)

    logging.info('Limpieza inicial realizada...')
    # ENDREGION

    # REGION: Agregar sustitutos
    # ---------------------------------------------------------------------
    # Candidatos material-ean-peso: lote nuevo + EANs ya cargados en la
    #  ventana
    df_material_ean = df_datos[['material', 'ean', 'weight_upc']].drop_duplicates()

    if existe_destino:
        df_material_ean_hist = readBigQuery(
            query=SQL_QUERIES['query_material_ean_historico'].substitute(
                tabla_destino=tabla_destino,
                store_banner=store_banner,
                fecha_inicial=fecha_inicial,
                fecha_final=fecha_final,
                proyecto=proyecto),
            user=USUARIO,
            gbq_client=gbq_client)
        df_material_ean_hist.columns = df_material_ean_hist.columns.str.lower()
        df_material_ean_hist['material'] = pd.to_numeric(
            df_material_ean_hist['material'], errors='coerce')
        df_material_ean_hist = df_material_ean_hist.dropna(subset=['material'])
        df_material_ean_hist['material'] = df_material_ean_hist['material'].astype(int)
        df_material_ean_hist['ean'] = df_material_ean_hist['ean'].astype(str)
        df_material_ean_hist['weight_upc'] = df_material_ean_hist['weight_upc'].astype(float)

        df_material_ean = pd.concat([df_material_ean, df_material_ean_hist]).drop_duplicates()

    df_material_ean = df_material_ean.sort_values(by=['material', 'weight_upc'])

    query_sust = SQL_QUERIES['query_athena_sust'].substitute(
        first_month=fecha_min.strftime('%Y%m'),
        last_month=fecha_max.strftime('%Y%m'),
        store_banner=store_banner,
        cant_sust=CANT_SUST)

    df_sust = readBigQuery(
        query=query_sust,
        user=USUARIO,
        gbq_client=gbq_client
    )

    for i in range(1, CANT_SUST + 1):
        df_parejas = obtenerParejasPorRelevance(df_sust, df_material_ean, i)
        df_parejas['p_month'] = df_parejas['p_month'].astype(int)

        df_datos = df_datos.merge(
            df_parejas[['ean', f'ean_sustituto_{i}', 'p_month']],
            on=['ean', 'p_month'],
            how='left'
        )

    logging.info('Se terminan de agregar los sustitutos...')
    # ENDREGION

    # REGION: Agregar APOTEOSICOS
    # ---------------------------------------------------------------------
    store_banner_codigo = CODIGO_APO.get(store_banner)

    if store_banner_codigo is not None:

        fecha_inicial_ano = (FECHA_INICIAL_APO if FECHA_INICIAL_APO is not None
                             else fecha_inicial.isoformat())

        query_apo = SQL_QUERIES['query_apoteosico'].substitute(
            store_banner_codigo=store_banner_codigo,
            cant_meses=MESES_HISTORIA + 1,
            fecha_inicial_ano=fecha_inicial_ano,
            proyecto=proyecto,
        )

        logging.info('Inicia la consulta de apoteosicos ...')

        df_apo = readBigQuery(
            query=query_apo,
            user=USUARIO,
            gbq_client=gbq_client
        )
        df_apo.columns = df_apo.columns.str.lower()

        if df_apo.empty:
            df_datos['apo'] = 0
        else:
            df_apo['fecha_inicio_de_promocion'] = pd.to_datetime(
                df_apo['fecha_inicio_de_promocion'])
            df_apo['fecha_fin_de_promocion'] = pd.to_datetime(
                df_apo['fecha_fin_de_promocion'])

            filas_expandidas = []
            for _, fila in df_apo.iterrows():
                rango_fechas = pd.date_range(start=fila['fecha_inicio_de_promocion'],
                                             end=fila['fecha_fin_de_promocion'])
                temp = pd.DataFrame({
                    'material': fila['material'],
                    'p_date': rango_fechas
                })
                filas_expandidas.append(temp)

            df_material_p_date = pd.concat(filas_expandidas,
                                           ignore_index=True).drop_duplicates()

            df_material_p_date['material'] = df_material_p_date['material'].astype(
                df_datos['material'].dtype)
            df_material_p_date['p_date'] = pd.to_datetime(df_material_p_date['p_date'])

            df_datos['clave'] = list(zip(df_datos['material'], df_datos['p_date']))
            df_material_p_date['clave'] = list(zip(df_material_p_date['material'],
                                                   df_material_p_date['p_date']))

            df_datos['apo'] = df_datos['clave'].isin(
                df_material_p_date['clave']).astype(int)
            df_datos = df_datos.drop(columns=['clave'])

    else:
        df_datos['apo'] = 0

    logging.info('Se agregan los apoteosicos...')
    # ENDREGION

    # REGION: Eliminar dias que no se vendieron al menos CANTIDAD_MINIMA
    # ---------------------------------------------------------------------
    # Se mantienen todas las filas KG / KGV sin importar la cantidad
    df_datos = df_datos[(df_datos['sales_uom'].isin(['KG', 'KGV'])) |
                        (df_datos['cantidad_total'] >= CANTIDAD_MINIMA)]
    # ENDREGION

    # REGION: Se agrega (append) a BigQuery
    # ---------------------------------------------------------------------
    df_final = df_datos[COLUMNAS_FINALES].copy()

    logging.info(f'PRE SUBIDA: {df_final.info()}')

    if df_final.empty:
        logging.info('No quedan filas tras los filtros. Nada que agregar.')
        return

    uploadFrame(
        df_final,
        table_ddl_json_path=os.path.join('gbq_objects', NOMBRE_JSON),
        project=proyecto,
        gbq_client=gbq_client,
        if_exists='append'
    )

    logging.info(f'Se agregan {len(df_final)} filas para '
                 f'{df_final["p_date"].nunique()} fechas del formato {store_banner}...')
    # ENDREGION


if __name__ == '__main__':
    main()
