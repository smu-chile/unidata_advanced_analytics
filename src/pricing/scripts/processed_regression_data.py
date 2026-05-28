# Default
from __future__ import annotations

import os
import logging
import argparse
from logging import config
from datetime import datetime

# Pip
import numpy as np
import pandas as pd
import pendulum
from google.cloud.bigquery import Client
from dateutil.relativedelta import relativedelta

# Own
from common.constants import LOGGING_CONFIG
from common.databases.queries import QueryDict
from common.gcp_extended.bigquery import (
    uploadFrame,
    readBigQuery,
    deleteFromTable,
    setTableExpiration,
    createTableAsSelect,
)
from common.gcp_extended.secretsmanager import getSecret  # noqa: F401


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
    '--use', type=str,
    help='Forecast or elasticity'
)

# -------------------------------------------------------------------------
#  SQL Queries
# -------------------------------------------------------------------------
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


'query_dias_venta_mayor':

"""
WITH VentasPorFecha AS (
  SELECT
    CATEGORY_DESCRIPTION,
    P_DATE,
    FORMAT_DATE('%A', P_DATE) AS dia_semana,   -- nombre del día (ej: Monday)
    SUM(VALUE) AS ventas_totales_producto
  FROM ${table_master}
  GROUP BY CATEGORY_DESCRIPTION, P_DATE
),

Promedios AS (
  SELECT
    CATEGORY_DESCRIPTION,
    dia_semana,
    AVG(ventas_totales_producto) AS promedio_ventas_dia
  FROM VentasPorFecha
  GROUP BY CATEGORY_DESCRIPTION, dia_semana
),

Resultados AS (
  SELECT
    a.CATEGORY_DESCRIPTION,
    a.P_DATE,
    a.dia_semana,
    a.ventas_totales_producto,
    b.promedio_ventas_dia,
    CASE
      WHEN a.ventas_totales_producto >= 6 * b.promedio_ventas_dia THEN 'x6'
      WHEN a.ventas_totales_producto >= 5 * b.promedio_ventas_dia THEN 'x5'
      WHEN a.ventas_totales_producto >= 4 * b.promedio_ventas_dia THEN 'x4'
      WHEN a.ventas_totales_producto >= 3 * b.promedio_ventas_dia THEN 'x3'
      WHEN a.ventas_totales_producto >= 2 * b.promedio_ventas_dia THEN 'x2'
      WHEN a.ventas_totales_producto >= 1.5 * b.promedio_ventas_dia THEN 'x1.5'
      WHEN a.ventas_totales_producto >= 0.5 * b.promedio_ventas_dia THEN 'x1'
      ELSE 'x0.5'
    END AS Multiplicador
  FROM VentasPorFecha a
  JOIN Promedios b
    ON a.CATEGORY_DESCRIPTION = b.CATEGORY_DESCRIPTION
   AND a.dia_semana = b.dia_semana
)

SELECT
  CATEGORY_DESCRIPTION,
  P_DATE,
  dia_semana,
  ventas_totales_producto / promedio_ventas_dia AS proporcion_categoria,
  Multiplicador
FROM Resultados
WHERE Multiplicador IS NOT NULL
ORDER BY CATEGORY_DESCRIPTION, P_DATE;
""",


'query_apoteosico':
"""
SELECT material, fecha_inicio_de_promocion,fecha_fin_de_promocion
FROM `cl-cda-prod.DS_CDA_VW_SMU.DW_VW_FACT_WORKFLOW`
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
AND substitution_rank <= 5
"""
})


# -------------------------------------------------------------------------
# Functions and Classes
# -------------------------------------------------------------------------


def calcular_año_mes_inicial(año_mes_final: str,
                              meses: int = 12) -> str:
    """Calcula la fecha inicial restando una cantidad de meses.

    Args:
    año_mes_final: str -> Fecha final en store_banner 'YYYY-MM'.
    meses: int -> Cantidad de meses que cubre el periodo (por defecto 12).

    Returns
    -------
    str: Fecha inicial en store_banner 'YYYY-MM' después de restar meses-1.

    Ejemplo:
    año_mes_final = '2024-07', meses = 12 → año_mes_inicial = '2023-08'
    """
    fecha_final = datetime.strptime(año_mes_final, '%Y-%m')  # noqa: DTZ007
    fecha_inicial = fecha_final - relativedelta(months=(meses - 1))
    return fecha_inicial.strftime('%Y-%m')

# Función para obtener el mejor ean_sustituto por relevance X
def obtenerParejasPorRelevance(df_sust: pd.DataFrame,
                                df_material_ean: pd.DataFrame,
                                relevance_val: int) -> pd.DataFrame:
    """Obtiene la mejor pareja sustituta por relevancia y peso.

    Filtra por relevance, une datos de material y sustituto,
    y selecciona la pareja con menor diferencia de peso. En otras palabras
    transforma el sustituto material a sustituto ean según el EAN con mayor
    similitud de peso

    Parameters
    ----------
    df_sust : pd.DataFrame
        DataFrame con columnas 'material', 'substitute', 'relevance', etc.
    df_material_ean : pd.DataFrame
        DataFrame con columnas 'material', 'ean', 'weight_upc'.
    relevance_val : int
        Valor de relevance a considerar (1, 2, o 3).

    Returns
    -------
    pd.DataFrame
        DataFrame con las mejores parejas sustitutas por ean y mes.
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

# Generar meses anteriores
def generarMesesPrevios(df: pd.DataFrame) -> pd.DataFrame:
    """Crea filas con desfases de 1, 2 y 3 meses hacia atrás.

    Para cada desfase, ajusta el campo 'p_month' manteniendo
    'p_month_ref' como referencia del mes original.

    Parameters
    ----------
    df : pd.DataFrame
        DataFrame con columnas 'ean' y 'p_month' en store_banner YYYYMM.

    Returns
    -------
    pd.DataFrame
        DataFrame con desfases mensuales agregados.
    """
    lista_frames = []
    for desfase in [1, 2, 3]:
        df_temp = df.copy()
        df_temp['p_month_ref'] = df_temp['p_month']

        año = df_temp['p_month_ref'] // 100
        mes = df_temp['p_month_ref'] % 100 - desfase

        año -= (mes <= 0)
        mes = (mes - 1) % 12 + 1
        df_temp['p_month'] = año * 100 + mes
        lista_frames.append(df_temp[['ean', 'p_month_ref', 'p_month']])
    return pd.concat(lista_frames)


# -------------------------------------------------------------------------
# Main function
# -------------------------------------------------------------------------
def main() -> None:  # noqa: D103


    # Parse input variables
    args = vars(parser.parse_args())
    execution_date: str = args['execution_date']
    proyecto: str = args['project_id']  # noqa: F841
    store_banner:str = args['store_banner']
    use:str = args['use']
    logging.info(f'execution_date: {execution_date}')
    logging.info(f'proyecto: {proyecto}')


    # Set gbq client for all subsequent queries
    gbq_client = Client()

    # REGION: Configuracion inicial
    #----------------------------------------------------------------------

    #---
    # Principales
    #---

    # Usuario
    usuario = 'pricing'

    # Tabla temporal aux
    store_banner_tabla = 'Super_10' if store_banner == 'Super 10' else store_banner

    esquema = 'TMP'
    #PARCHE TEMPORAL
    tabla = f'TMP_REGRESSION_DATA_{use}_aux_{store_banner_tabla}'
    tmp_path_table_aux = f'{proyecto}.{esquema}.{tabla}'

    # Tabla temporal final
    esquema = 'TMP'
    # PARCHE TEMPORAL
    tabla = f'TMP_REGRESSION_DATA_{use}'
    tmp_path_table = f'{proyecto}.{esquema}.{tabla}'


    # Nombre archivo Json
    if use == 'FORECAST':
        nombre_json = 'ingest_regression_processed_data_forecast.json'
    elif use == 'ELASTICITY':
        nombre_json = 'ingest_regression_processed_data_elasticity.json'
    else:
        msg = f"Valor inválido para 'use': {use}"
        raise ValueError(msg)

    #----------------------------------------------------------------------
    # ENDREGION

    # REGION: Parametros iniciales
    #----------------------------------------------------------------------
    # Formato en MAYUSCULAS
    store_banner_mayusculas = store_banner.upper()


    #----------------------------------------------------------------------
    # ENDREGION

    # REGION: Inputs del proceso
    #----------------------------------------------------------------------

    # Cantidad de meses
    # Parche
    cant_meses = 29

    if use == 'FORECAST':
        # Convertir fecha de ejecución
        fecha_ejecucion = pendulum.parse(execution_date)
        fecha_final = fecha_ejecucion.subtract(days=1)
        fecha_inicial = fecha_final.subtract(months=cant_meses).add(days=1)

        # Monthid respectivos
        monthid_final = fecha_final.format('YYYYMM')
        monthid_inicial = fecha_inicial.format('YYYYMM')

    elif use == 'ELASTICITY':
        # Convertir fecha de ejecución
        fecha_ejecucion = pendulum.parse(execution_date)
        fecha_final = fecha_ejecucion.start_of('month').subtract(days=1)
        fecha_inicial = fecha_final.subtract(months=cant_meses).add(months=1).start_of('month')

        # Monthid respectivos
        monthid_final = fecha_final.format('YYYYMM')
        monthid_inicial = fecha_inicial.format('YYYYMM')



    logging.info(' ')
    logging.info('--------------------')
    logging.info(f'Se inicia el proceso para {store_banner_mayusculas}')
    logging.info('--------------------')

    #----------------------------------------------------------------------
    # ENDREGION




    # REGION: Query  principal
    #----------------------------------------------------------------------

    logging.info(f'Fecha inicial: {fecha_inicial}')
    logging.info(f'Fecha final: {fecha_final}')



    # Se crea la query
    query_master_table = SQL_QUERIES['query_master_table'].substitute(
        fecha_inicial = fecha_inicial,
        fecha_final = fecha_final,
        cant_meses = cant_meses,
        store_banner = store_banner,
        proyecto = proyecto
    )

    createTableAsSelect(query = query_master_table,
                        gbq_client=gbq_client,
                        table_ref=tmp_path_table_aux,
                        use_legacy_sql=False)

    logging.info('Tabla auxiliar/maestra creada...')


    ahora = pendulum.now()
    expiration = ahora.add(minutes=200)

    setTableExpiration(
        table_ref = tmp_path_table_aux,
        expiration = expiration,
        gbq_client= gbq_client
    )

    logging.info('Se setea la expiracion de la tabla maestra...')

    # Se realiza la query en caso de no existir
    query_principal = SQL_QUERIES['query_principal'].substitute(
        table_master = tmp_path_table_aux
    )
    df_datos = readBigQuery(
            query=query_principal,
            user=usuario,
            gbq_client=gbq_client
        )

    logging.info('Consulta principal lista...')

    #----------------------------------------------------------------------
    # ENDREGION



    # REGION: Se limpian y configuran los datos
    #----------------------------------------------------------------------

    # Columnas se dejan en minuscula
    df_datos.columns = df_datos.columns.str.lower()

    # Se transforma a store_banner fecha
    df_datos['p_date'] = pd.to_datetime(df_datos['p_date'], format='%Y-%m-%d')

    # Agregar columnas adicionales
    df_datos['store_banner'] = store_banner

    # Transformar product_id a material
    df_datos = df_datos.rename(columns={'product_id':'material'})
    df_datos['material'] = df_datos['material'].astype(int)
    df_datos['ean'] = df_datos['ean'].astype(str)

    # Eliminar precios con precio infinito
    df_datos = df_datos[df_datos['precio_promedio'] < 1000000]

    # Se transforma precio promedio y otros campos a entero
    df_datos['precio_promedio'] = df_datos['precio_promedio'].astype(int)
    df_datos['sales_unit'] = df_datos['sales_unit'].astype(int)
    df_datos['ventas_totales_producto'] = df_datos['ventas_totales_producto'].astype(int)
    df_datos['cantidad_total'] = df_datos['cantidad_total'].astype(float).round(2)
    df_datos = df_datos[df_datos['cantidad_total'] > 0]
    df_datos['weight_upc'] = df_datos['weight_upc'].astype(float)

    logging.info('Limpieza inicial realizada...')

    #~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    # ENDREGION

    # REGION: Agregar columna p_month y p_week
    #--------------------------------------------------------------------------

    # Extraer el año y guardarlo en una nueva columna
    df_datos['p_year'] = df_datos['p_date'].dt.year.astype(str)
    logging.info('Agregados p_year...')

    # Crear la columna 'p_month' formateando 'p_date' como 'YYYYMM'
    df_datos['p_month'] = df_datos['p_date'].dt.strftime('%Y%m').astype(int)
    logging.info('Agregados p_month...')

    # Crear la columna 'p_week' con el store_banner año y semana ISO juntos
    df_datos['p_week'] = df_datos['p_date'].apply(
        lambda x: int(f'{x.isocalendar()[0]}{x.isocalendar()[1]:02}'))
    logging.info('Agregados p_week...')
    #~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    # ENDREGION

    # REGION: Dummy dias especiales
    #--------------------------------------------------------------------------

    # Se genera query definitiva
    query_dias_venta_mayor = SQL_QUERIES['query_dias_venta_mayor'].substitute(
        table_master = tmp_path_table_aux)


    df_dias_especiales = readBigQuery(
        query=query_dias_venta_mayor,
        user=usuario,
        gbq_client=gbq_client
    )


    df_dias_especiales.columns = df_dias_especiales.columns.str.lower()

    df_dias_especiales['p_date'] = pd.to_datetime(df_dias_especiales['p_date'],
                                                format='%Y-%m-%d')


    df_datos = df_datos.merge(df_dias_especiales[['category_description',
                                                'p_date',
                                                'proporcion_categoria',
                                                'multiplicador']],
                            on = ['category_description','p_date'],
                            how='left')

    # Reemplaza los valores NaN en la columna 'multiplicador' con 'x1'
    df_datos['multiplicador'] = df_datos['multiplicador'].fillna('x1')

    # Redondear
    df_datos['proporcion_categoria'] = np.round(df_datos['proporcion_categoria'],2)

    # Crear únicamente la dummy multiplicador_x05
    df_datos['multiplicador_x05'] = (df_datos['multiplicador'] == 'x0.5').astype(int)
    df_datos = df_datos.drop(columns=['multiplicador'])


    logging.info('Se agregan DUMMY dias especiales (x0.5, x1.5, x2 o x3)....')


    #~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    # ENDREGION


    # REGION: Agregar si es primer o ultimo dia del mes
    #--------------------------------------------------------------------------

    # Dummy: último día del mes
    df_datos['ultimo_dia_mes'] = df_datos['p_date'].dt.is_month_end.astype(int)

    # Dummy: primer día del mes
    df_datos['primer_dia_mes'] = df_datos['p_date'].dt.is_month_start.astype(int)

    logging.info('Se agregan DUMMIES: primer y último día del mes...')

    #~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    # ENDREGION



    # REGION: Calcular precio promedio de 3 meses anteriores
    #--------------------------------------------------------------------------

    # Crear tabla con precio promedio por ean y p_month
    df_promedios = (
        df_datos.groupby(['ean', 'p_month'])['precio_promedio']
        .mean()
        .reset_index()
        .rename(columns={'precio_promedio': 'precio_mensual'})
    )

    # Generar tabla auxiliar con cada fila del df original y
    # los tres meses anteriores
    df_aux = df_datos[['ean', 'p_month', 'precio_promedio']].copy()


    df_meses_anteriores = generarMesesPrevios(df_aux)

    # Unir con la tabla de promedios
    df_merged = df_meses_anteriores.merge(
        df_promedios,
        how='left',
        on=['ean', 'p_month']
    )

    # Calcular el promedio histórico de los tres meses anteriores
    df_historico = (
        df_merged.groupby(['ean', 'p_month_ref'])['precio_mensual']
        .mean()
        .reset_index()
        .rename(columns={'precio_mensual': 'precio_medio_anterior'})
    )

    # Unir con df_datos
    df_datos = df_datos.merge(
        df_historico,
        how='left',
        left_on=['ean', 'p_month'],
        right_on=['ean', 'p_month_ref']
    ).drop(columns='p_month_ref')

    logging.info('Se calcula precio de los meses anteriores...')

    #~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    # ENDREGION

    # REGION: Calculo de variación porcentual
    #--------------------------------------------------------------------------

    # Calcular variación porcentual
    df_datos['variacion_porcentual'] = (
        (df_datos['precio_promedio'] - df_datos['precio_medio_anterior']
        ) / df_datos['precio_medio_anterior']).fillna(0) * 100

    # Se redondea
    df_datos['variacion_porcentual'] = df_datos['variacion_porcentual'].round(2)


    # Obtener los tres p_month más antiguos
    primeros_tres = sorted(df_datos['p_month'].unique())[:3]

    # Filtrar el dataframe eliminando esos p_month
    df_datos = df_datos[~df_datos['p_month'].isin(primeros_tres)].copy()

    logging.info('Se obtiene la variacion porcentual del precio respecto al pasado...')

    #~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    # ENDREGION


    # REGION: Calculo de variación porcentual de la subcategoria
    #--------------------------------------------------------------------------

    # ---------------- claves, pesos y variación --------------------------
    columnas_grupo = ['sub_category_description', 'p_date']

    peso      = df_datos['ventas_totales_producto']
    variacion = df_datos['variacion_porcentual']

    # Contribución de cada fila al numerador
    aporte_fila = variacion * peso
    df_datos['__num__'] = aporte_fila  # columna temporal

    # ---------------- totales (incluyen la fila) -------------------------
    numerador_total   = df_datos.groupby(columnas_grupo)['__num__'].transform('sum')
    denominador_total = df_datos.groupby(columnas_grupo)[
        'ventas_totales_producto'].transform('sum')

    # ---------------- excluir la propia fila -----------------------------
    numerador_excl   = numerador_total   - df_datos['__num__']
    denominador_excl = denominador_total - peso

    df_datos['variacion_porcentual_subcategoria'] = numerador_excl / denominador_excl
    df_datos.loc[denominador_excl == 0, 'variacion_porcentual_subcategoria'] = 0

    # Limpieza: eliminar columna temporal
    df_datos = df_datos.drop(columns='__num__')

    df_datos['variacion_porcentual_subcategoria'] = (
        df_datos['variacion_porcentual_subcategoria'].round(2))

    logging.info('Se obtiene la variacion porcentual del precio de la subcategoria...')

    #~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    # ENDREGION


    # REGION: Agregar sustitutos y sus variaciones de precio
    #--------------------------------------------------------------------------

    # Obtener lista que empareka material-ean-peso
    df_material_ean = df_datos[['material','ean','weight_upc']
                            ].drop_duplicates().sort_values(by=['material','weight_upc'])

    # Query sustitutos
    query_sust = SQL_QUERIES['query_athena_sust'].substitute(
        first_month=monthid_inicial,
        last_month=monthid_final,
        store_banner = store_banner
    )

    df_sust = readBigQuery(
        query = query_sust,
        user = usuario,
        gbq_client=gbq_client
        )


    # Obtener las tres tablas de parejas
    df_parejas_1 = obtenerParejasPorRelevance(df_sust, df_material_ean, 1)
    df_parejas_2 = obtenerParejasPorRelevance(df_sust, df_material_ean, 2)
    df_parejas_3 = obtenerParejasPorRelevance(df_sust, df_material_ean, 3)
    df_parejas_4 = obtenerParejasPorRelevance(df_sust, df_material_ean, 4)
    df_parejas_5 = obtenerParejasPorRelevance(df_sust, df_material_ean, 5)


    # Agregar para las diferencias relevancias la columna con sustituto y
    # variacion de precio
    for i in [1, 2, 3, 4, 5]:
        df_p = locals()[f'df_parejas_{i}']
        df_p['p_month'] = df_p['p_month'].astype(int)

        df_datos = df_datos.merge(
            df_p[['ean', f'ean_sustituto_{i}', 'p_month']],
            on=['ean', 'p_month'],
            how='left'
        )

        # Preparar dataframe auxiliar con variaciones
        df_aux_var = df_datos[['ean', 'p_date', 'variacion_porcentual']].copy()
        df_aux_var = df_aux_var.rename(columns={
            'ean': f'ean_sustituto_{i}',
            'variacion_porcentual': f'variacion_porcentual_{i}'
        })
        df_aux_var = df_aux_var.drop_duplicates(subset=[f'ean_sustituto_{i}', 'p_date'])

        # Hacer merge por ean_sustituto_i y p_date
        df_datos = df_datos.merge(
            df_aux_var,
            on=[f'ean_sustituto_{i}', 'p_date'],
            how='left'
        )

    logging.info('Se terminan de agregar los sustitutos...')
    #~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    # ENDREGION


    # REGION: Generar variables de sust combinadas top1 y promedio_top3
    #--------------------------------------------------------------------------


    # Paso 1: Convertir a array
    cols_var = [
        'variacion_porcentual_1',
        'variacion_porcentual_2',
        'variacion_porcentual_3',
        'variacion_porcentual_4',
        'variacion_porcentual_5'
    ]
    var_array = df_datos[cols_var].to_numpy()

    # Paso 2: Crear máscara de no-nulos
    mask_validos = ~np.isnan(var_array)

    # Paso 3: Prealocar arrays vacíos para resultados
    top1 = np.full(var_array.shape[0], np.nan)
    top3 = np.full(var_array.shape[0], np.nan)

    # Paso 4: Recorrer por fila (rápido aún en bucle por NumPy)
    for i in range(var_array.shape[0]):
        valores = var_array[i][mask_validos[i]]
        if len(valores) >= 1:
            top1[i] = valores[0]
        if len(valores) >= 3:
            top3[i] = np.mean(valores[:3])

    # Paso 5: Agregar al dataframe
    df_datos['variacion_top1_sustituto'] = top1.round(2)
    df_datos['variacion_top3_sustitutos'] = top3.round(2)

    logging.info('Se terminan de agregar variables combinadas de sustitutos sustitutos...')
    #~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    # # ENDREGION





    # REGION: Agregar APOTEOSICOS
    #--------------------------------------------------------------------------

    if store_banner == 'Unimarc':

        # Query
        query_apo = SQL_QUERIES['query_apoteosico'].substitute(
                            store_banner_codigo=1000,
                            cant_meses = cant_meses,
                            fecha_inicial_ano = '2023-02-01')


        logging.info('Inicia la consulta de apoteosicos ...')

        df_apo = readBigQuery(
            query=query_apo,
            user=usuario,
            gbq_client=gbq_client
        )

        df_apo.columns = df_apo.columns.str.lower()

        # Asegurarte que las fechas están como datetime
        df_apo['fecha_inicio_de_promocion'] = pd.to_datetime(df_apo['fecha_inicio_de_promocion'])
        df_apo['fecha_fin_de_promocion'] = pd.to_datetime(df_apo['fecha_fin_de_promocion'])

        # Crear lista para guardar las expansiones
        filas_expandidas = []

        # Iterar sobre cada fila de df_apo
        for _, fila in df_apo.iterrows():
            material = fila['material']
            fecha_inicio = fila['fecha_inicio_de_promocion']
            fecha_fin = fila['fecha_fin_de_promocion']

            # Crear rango de fechas (incluyendo el último día)
            rango_fechas = pd.date_range(start=fecha_inicio, end=fecha_fin)

            # Crear un pequeño dataframe temporal para este material
            temp = pd.DataFrame({
                'material': material,
                'p_date': rango_fechas
            })
            filas_expandidas.append(temp)

        # Unir todo en un solo dataframe
        df_material_p_date = pd.concat(filas_expandidas, ignore_index=True).drop_duplicates()


        # Primero nos aseguramos que ambas columnas tengan los mismos tipos
        df_material_p_date['material'] = df_material_p_date['material'].astype(
            df_datos['material'].dtype)
        df_material_p_date['p_date'] = pd.to_datetime(df_material_p_date['p_date'])

        # Crear una clave combinada material + p_date en ambos dataframes
        df_datos['clave'] = list(zip(df_datos['material'], df_datos['p_date']))
        df_material_p_date['clave'] = list(zip(df_material_p_date['material'],
                                            df_material_p_date['p_date']))

        # Crear la columna 'apo'
        df_datos['apo'] = df_datos['clave'].isin(df_material_p_date['clave']).astype(int)

        # Finalmente eliminar la columna auxiliar 'clave' si no la quieres
        df_datos = df_datos.drop(columns=['clave'])


    else:
        df_datos['apo'] = 0


    logging.info('Se agregan los apoteosicos...')

    #~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    # ENDREGION

    # REGION: Eliminar dias que no se vendieron al menos 5 unidades
    #--------------------------------------------------------------------------


    # Filtrar filas eliminando aquellas que tienen cantidad_total < 3,
    # pero solo si su unidad de medida (sales_uom) NO es 'KG' o 'KGV'.
    # Se mantienen todas las filas con 'KG' o 'KGV',
    # sin importar la cantidad.
    df_datos = df_datos[(df_datos['sales_uom'].isin(['KG', 'KGV'])) |
                        (df_datos['cantidad_total'] >= 3)]

    #~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    # ENDREGION

    # REGION: Eliminar aquellos productos que no se venden hace 1 año
    #--------------------------------------------------------------------------

    cantidad_inicial = df_datos['ean'].nunique()

    # Paso 1: obtener el último p_date del dataframe
    fecha_maxima = df_datos['p_date'].max()

    # Paso 2: calcular el p_date máximo por ean
    fecha_maxima_por_ean = df_datos.groupby('ean')['p_date'].max()

    # Paso 3: eans que tienen su última venta dentro del último año
    eans_validos = fecha_maxima_por_ean[
        fecha_maxima_por_ean >= (fecha_maxima - pd.DateOffset(years=1))].index

    # Paso 4: filtrar el dataframe original
    df_datos = df_datos[df_datos['ean'].isin(eans_validos)].copy()

    cantidad_final = df_datos['ean'].nunique()
    cantidad_eliminados = cantidad_inicial-cantidad_final

    logging.info(
        f'Se eliminan los productos que no se han vendido hace 1 año: {cantidad_eliminados}...')

    #~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    # ENDREGION

    # REGION: Reordenación de dataframe principal
    #--------------------------------------------------------------------------




    # Lista de columnas deseadas
    columnas_deseadas = [
        'store_banner',
        'category_description', 'sub_category_description', 'material', 'product_description',
        'ean', 'sales_uom', 'sales_unit', 'p_date', 'p_week', 'p_month',
        'ventas_totales_producto', 'cantidad_total', 'precio_promedio',
        'primer_dia_mes', 'ultimo_dia_mes',
        'multiplicador_x05', 'apo', 'proporcion_categoria',
        'ean_sustituto_1', 'ean_sustituto_2', 'ean_sustituto_3',
        'ean_sustituto_4', 'ean_sustituto_5',
        'variacion_porcentual_subcategoria',
        'variacion_top1_sustituto', 'variacion_top3_sustitutos'
    ]


    # Filtrar solo las columnas que existen en df_datos
    columnas_existentes = [col for col in columnas_deseadas if col in df_datos.columns]

    # Crear el nuevo dataframe
    df_final = df_datos[columnas_existentes].copy()


    #~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    # ENDREGION

    # REGION: Ordenar EAN por ventas
    #--------------------------------------------------------------------------

    # Paso 1: Calcular la suma de ventas por material
    suma_ventas_por_material = df_final.groupby('ean')['ventas_totales_producto'].sum()

    # Paso 2: Crear una columna temporal con la suma de ventas y ordenar
    df_final['suma_ventas'] = df_final['ean'].map(suma_ventas_por_material)
    df_final = df_final.sort_values(by='suma_ventas', ascending=False)

    #Eliminar la columna temporal si no la necesitas más
    df_final = df_final.drop('suma_ventas', axis=1)


    logging.info('Se limpia y reordena por ventas el dataframe...')

    #~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    # ENDREGION

    # REGION: Eliminar inicio frio
    #--------------------------------------------------------------------------
    # Se elimina el primer mes de cada material, ya que en estos meses no
    # tendrá sustitutos.

    # Paso 1: obtener el primer p_month por material
    primer_p_month_por_material = (
        df_final.groupby('material', as_index=False)['p_month']
        .min()
        .rename(columns={'p_month': 'primer_p_month'})
    )

    # Paso 2: merge con el primer p_month de cada material
    df_final = df_final.merge(primer_p_month_por_material, on='material', how='left')

    # Paso 3: filtrar filas donde p_month != primer_p_month
    df_final = df_final.loc[df_final['p_month'] != df_final['primer_p_month']]

    # Paso 4: eliminar la columna auxiliar
    df_final = df_final.drop(columns=['primer_p_month'])

    logging.info('Se elimina inicio frio...')
    #~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    # ENDREGION

    # REGION: Se sube la tabla a BIG QUERY
    #--------------------------------------------------------------------------

    if use == 'FORECAST':
        uploadFrame(
            df_final,
            table_ddl_json_path=os.path.join('gbq_objects',
                                            nombre_json),
            project=proyecto,
            gbq_client=gbq_client,
            if_exists='replace'
        )

    if use == 'ELASTICITY':
        deleteFromTable(table_ref='cl-bigdata-analytics-preprod.TMP.TMP_REGRESSION_PROCESSED_DATA_ELASTICITY',
                where_clause=f"store_banner = '{store_banner}'",
                gbq_client=gbq_client)

        uploadFrame(
            df_final,
            table_ddl_json_path=os.path.join('gbq_objects',
                                            nombre_json),
            project=proyecto,
            gbq_client=gbq_client,
            if_exists='append'
        )

    logging.info(f'Se sube info actualizada del formato {store_banner}...')
    #~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    # ENDREGION


if __name__ == '__main__':
    main()
