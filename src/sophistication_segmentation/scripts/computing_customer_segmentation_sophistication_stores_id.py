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
from google.cloud import bigquery  # noqa: F401
from sklearn.cluster import KMeans
from google.cloud.bigquery import Client
from dateutil.relativedelta import relativedelta

# Own
import common.gcp_extended.bigquery as gbq_extended  # noqa: F401
from common.constants import LOGGING_CONFIG
from common.databases.queries import QueryDict
from common.gcp_extended.bigquery import (  # noqa: E402
    uploadFrame,
    readBigQuery,
    deleteFromTable,
)


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

#PARCHE 1
parser.add_argument(
    '--store_id', type=tuple,
    help='Store id'
)


# -------------------------------------------------------------------------
#  SQL Queries
# -------------------------------------------------------------------------
SQL_QUERIES = QueryDict({
    # ------------------------------------------------------------------
    # Query principal: calcula, a 12 meses desde ${fecha_inicial_ano},
    # métricas de precio unitario (PU) y precio por unidad de medida
    # (PPUM) por cliente y categoría, filtrando por banner y reglas
    # comerciales; excluye e-commerce específico.
    #
    # Parámetros:
    #   - ${proyecto}: dataset base (proyecto GCP).
    #   - ${fecha_inicial_ano}: mes inicial (YYYY-MM-DD); la ventana
    #       corre 12 meses hacia adelante.
    #   - ${formato}: banner de tienda (p. ej., 'Unimarc').
    #   - $(store_id): tuple con id de tiends. (ej: ('355))
    #   - ${minimo_items_categoria}: mínimo de ítems por cliente-categoría
    #       para ser considerado.
    #
    # Notas de cálculo:
    #   - PU_PRODUCTO = VALUE / QUANTITY.
    #   - PPUM_PRODUCTO = VALUE / (WEIGHT * QUANTITY).
    #   - PU_SUBCATEGORIA y PPUM_SUBCATEGORIA son promedios por
    #       subcategoría y unidad de medida (ventana GROUP BY).
    #   - PU_INDEXADO  = PU_PRODUCTO  / PU_SUBCATEGORIA.
    #   - PPUM_INDEXADO = PPUM_PRODUCTO / PPUM_SUBCATEGORIA.
    #   - Se deduplica DIM_PRODUCT por EAN (selección DISTINCT).
    #   - Se filtran transacciones válidas (tipos BX/BE/TF, venta 'V',
    #       UNIT_PRICE>0, VALUE>0) y se excluyen NEG_DSC no retail.
    #   - Se excluyen canastas e-commerce (PY, CornerShop, Rappi).
    #
    # Retorna (agregado por cliente y categoría):
    #   - customer_key
    #   - category_description
    #   - total_value: suma de VALUE del período.
    #   - ppum_customer_key_categoria: promedio ponderado por QUANTITY de
    #       PPUM_INDEXADO.
    #   - pu_customer_key_categoria: promedio ponderado por QUANTITY de
    #       PU_INDEXADO.
    # ------------------------------------------------------------------
    'query_sophistication':
    """

-- Eliminar duplicidades de la dim products

WITH distinct_products AS (

  SELECT DISTINCT
    EAN,
    CAT_DSC AS CATEGORY_DESCRIPTION,
    GRUPO_DSC as SUB_CATEGORY_DESCRIPTION,
    NM AS PRODUCT_DESCRIPTION,
    SKU_PRODUCT AS PRODUCT_ID,
    NEG_DSC,
    BRAND_DESC as BRAND,
    SAFE_CAST(CONTENIDO_BRUTO AS FLOAT64) as WEIGHT,
    UM_CONTENIDO as WEIGHT_UM -- L o KG


  FROM `${proyecto}.CDA_VISTAS.VW_DIM_PRODUCT`

),

-- Minimo de compras para considerar a un cliente en una categoria
category_counts AS (

  SELECT
    A.CUSTOMER_KEY,
    P.CATEGORY_DESCRIPTION,
    COUNT(*) AS CATEGORY_COUNT
  FROM `${proyecto}.CDA_VISTAS.VW_SALES_ITEM` A
  INNER JOIN distinct_products P
    ON A.EAN = P.EAN
  INNER JOIN `${proyecto}.CDA_VISTAS.VW_DIM_STORE` DS
    ON A.STORE_ID = DS.STORE_ID
  WHERE
    A.TRANSACTION_DATE >= DATE('${fecha_inicial_ano}')
    AND A.TRANSACTION_DATE < DATE_ADD(DATE('${fecha_inicial_ano}'), INTERVAL  12 MONTH)
    AND DS.STORE_BANNER = '${formato}'
    AND DS.STORE_ID IN '${store_id}'
  GROUP BY A.CUSTOMER_KEY, P.CATEGORY_DESCRIPTION
  HAVING COUNT(*) >= ${minimo_items_categoria}

),

-- Consulta principal
data_customer as (
SELECT
  A.CUSTOMER_KEY,
  A.STORE_ID,
  A.MARKET_BASKET_KEY,
  P.PRODUCT_DESCRIPTION,
  P.PRODUCT_ID,
  P.CATEGORY_DESCRIPTION,
  P.SUB_CATEGORY_DESCRIPTION,
  P.BRAND,
  A.QUANTITY,
  A.VALUE,
  P.WEIGHT * A.QUANTITY AS WEIGHT_TOTAL,

  -- PU_PRODUCTO
  A.VALUE / A.QUANTITY AS PU_PRODUCTO,

  -- PPUM_PRODUCTO
  A.VALUE / (P.WEIGHT * A.QUANTITY) AS PPUM_PRODUCTO,

  -- PU_SUBCATEGORIA
  AVG(A.VALUE / A.QUANTITY) OVER (
    PARTITION BY CONCAT(P.SUB_CATEGORY_DESCRIPTION, ' - ', P.WEIGHT_UM)
  ) AS PU_SUBCATEGORIA,

  -- PPUM_SUBCATEGORIA
  AVG(A.VALUE / (P.WEIGHT * A.QUANTITY)) OVER (
    PARTITION BY CONCAT(P.SUB_CATEGORY_DESCRIPTION, ' - ', P.WEIGHT_UM)
  ) AS PPUM_SUBCATEGORIA,

  -- PU_INDEXADO
  A.VALUE / A.QUANTITY
    / AVG(A.VALUE / A.QUANTITY) OVER (
        PARTITION BY CONCAT(P.SUB_CATEGORY_DESCRIPTION, ' - ', P.WEIGHT_UM)
      ) AS PU_INDEXADO,

  -- PPUM_INDEXADO
  A.VALUE / (P.WEIGHT * A.QUANTITY)
    / AVG(A.VALUE / (P.WEIGHT * A.QUANTITY)) OVER (
        PARTITION BY CONCAT(P.SUB_CATEGORY_DESCRIPTION, ' - ', P.WEIGHT_UM)
      ) AS PPUM_INDEXADO


FROM `${proyecto}.CDA_VISTAS.VW_SALES_ITEM` A
INNER JOIN distinct_products P
  ON A.EAN = P.EAN
INNER JOIN `${proyecto}.CDA_VISTAS.VW_DIM_STORE` D
  ON A.STORE_ID = D.STORE_ID
INNER JOIN category_counts CC
  ON A.CUSTOMER_KEY = CC.CUSTOMER_KEY
  AND P.CATEGORY_DESCRIPTION = CC.CATEGORY_DESCRIPTION
WHERE
  A.TRANSACTION_DATE >= DATE('${fecha_inicial_ano}')
  AND A.TRANSACTION_DATE < DATE_ADD(DATE('${fecha_inicial_ano}'), INTERVAL 12 MONTH)
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
  AND D.STORE_BANNER = '${formato}'
  AND D.STORE_ID IN '${store_id}'
  AND A.CUSTOMER_KEY <> MD5('CST^CL^-1')
  )

SELECT
    CUSTOMER_KEY,
    CATEGORY_DESCRIPTION,
    SUM(VALUE) AS TOTAL_VALUE,
    SUM(PPUM_INDEXADO * QUANTITY) / SUM(QUANTITY) AS PPUM_customer_key_CATEGORIA,
    SUM(PU_INDEXADO * QUANTITY) / SUM(QUANTITY) AS PU_customer_key_CATEGORIA
FROM data_customer
GROUP BY 1,2
ORDER BY CUSTOMER_KEY
    """
})


# -------------------------------------------------------------------------
# Functions and Classes
# -------------------------------------------------------------------------

def generar_año_mes(año_mes: str,
                    cantidad_meses: int) -> list:
    """Genera una lista con los Año-Mes de la forma XXXX-YY.

    Args:
    año_mes: string -> Año-Mes incial
    cantidad_meses: int -> Cantidad de meses a considerar

    Returns
    -------
    list: Lista con Año-Mes
    """
    # Convertir la entrada en un objeto pendulum
    fecha_inicial = pendulum.from_format(año_mes, 'YYYY-MM')
    # Crear una lista para almacenar los resultados
    lista_meses = []

    # Generar los meses
    for _i in range(cantidad_meses):
        # Añadir el mes actual en el formato 'AÑO-MES'
        lista_meses.append(fecha_inicial.format('YYYY-MM'))
        # Avanzar un mes
        fecha_inicial = fecha_inicial.add(months=1)

    return lista_meses


def calcular_año_mes_final(año_mes_inicial: str) -> str:
    """Calcula la fecha final después de 12 m dados un año y mes inicial.

    Args:
    año_mes_inicial: str -> Fecha inicial en formato 'YYYY-MM'.

    Returns
    -------
    str: Fecha final en formato 'YYYY-MM' después de añadir 12 meses.
    Ejemplo:
    año_mes_inicial = '2023-08'
    año_mes_final = '2024-07'
    """
    # Convertir el string a un objeto datetime
    fecha_inicial = datetime.strptime(año_mes_inicial, '%Y-%m')  # noqa: DTZ007

    # Añadir 12 meses a la fecha inicial
    fecha_final = fecha_inicial + relativedelta(months=11)

    # Convertir la fecha final de vuelta a string
    return fecha_final.strftime('%Y-%m')


def calcular_año_mes_inicial(año_mes_final: str) -> str:
    """Calcula fecha inicial retrocediendo 12 meses desde una fecha final.

    Args:
    año_mes_final: str -> Fecha final en formato 'YYYY-MM'.

    Returns
    -------
    str: Fecha inicial en formato 'YYYY-MM' después de restar 12 meses.
    Ejemplo:
    año_mes_final = '2024-07'
    año_mes_inicial = '2023-08'
    """
    # Convertir el string a un objeto datetime
    fecha_final = datetime.strptime(año_mes_final, '%Y-%m')  # noqa: DTZ007

    # Restar 12 meses a la fecha final
    fecha_inicial = fecha_final - relativedelta(months=11)

    # Convertir la fecha inicial de vuelta a string
    return fecha_inicial.strftime('%Y-%m')


def clasificar_categorias_kmeans(df: pd.DataFrame) -> pd.DataFrame:
    """Clasifica categorías mediante dos métodos K-means.

    Agrupa a las categorías en grupos utilizando K-means basado en las
    características de comb, con reordenamiento de etiquetas para
    que 1 sea el valor del grupo con menor valor de comb y 3 para
    el mayor.

    Args:
    df: pd.DataFrame -> DataFrame que contiene los datos de clientes.
    n_clusters: int -> Número de clusters para K-means (default: 3).

    Returns
    -------
    pd.DataFrame: DataFrame con dos nuevas columnas:
    - 'grupo_kmeans': Número del 1 al 3.
    - 'segmento_categoria': Segmento asignado
    """
    # Crear una copia del dataframe para evitar modificar el original.
    df_resultado = df.copy()

    # Número de clusters / segmentos para el 90%
    n_clusters = 3

    # Lista de categorías únicas
    categorias_unicas = df['category_description'].unique()

    # Total de categorías
    total_categorias = len(categorias_unicas)

    # Variables para seguir el progreso
    siguiente_umbral = 10

    # Inicializar el modelo de KMeans con el número de clusters deseado
    kmeans = KMeans(n_clusters=n_clusters, random_state=42)

    # Iterar sobre cada categoría
    for indice, categoria in enumerate(categorias_unicas):
        # Filtrar los datos para la categoría actual
        df_categoria = df_resultado[
             df_resultado['category_description'] == categoria]

        # Calcular percentiles para el 5% más bajo y más alto
        umbral_bajo = df_categoria['comb_customer_key_categoria'].quantile(0.05)
        umbral_alto = df_categoria['comb_customer_key_categoria'].quantile(0.95)

        # Dividir en tres grupos
        df_bajo = df_categoria[
                df_categoria['comb_customer_key_categoria'] <= umbral_bajo].copy()
        df_alto = df_categoria[
                df_categoria['comb_customer_key_categoria'] >= umbral_alto].copy()
        df_resto = df_categoria[
             (df_categoria['comb_customer_key_categoria'] > umbral_bajo) &
             (df_categoria['comb_customer_key_categoria'] < umbral_alto)].copy()

        # Aplicar K-means al resto (90%)
        if len(df_resto) >= n_clusters:

            df_resto['grupo_kmeans'] = kmeans.fit_predict(
                                       df_resto[['comb_customer_key_categoria']])

            # Ordenar grupos por mediana y asignar etiquetas nuevas
            # Es decir, asegurar que el grupo menor sea 1 y el mayor 3
            medianas = df_resto.groupby('grupo_kmeans')[
                            'comb_customer_key_categoria'].median().sort_values()
            etiquetas_ordenadas = {old_label: i + 1 for i,
                                   old_label in enumerate(medianas.index)}
            df_resto['grupo_kmeans'] = df_resto[
                            'grupo_kmeans'].map(etiquetas_ordenadas)

        else:
            df_resto['grupo_kmeans'] = np.nan

        # Asignar grupos extremos
        df_bajo['grupo_kmeans'] = 1  # Grupo con menor comb
        df_alto['grupo_kmeans'] = 3  # Grupo con mayor comb

        # Combinar de nuevo en un solo dataframe
        df_categoria = pd.concat([df_bajo, df_resto, df_alto])

        # Actualizar en el dataframe resultado
        df_resultado.loc[df_categoria.index,
                         'grupo_kmeans'] = df_categoria['grupo_kmeans']

        # Actualizar progreso y emitir mensaje si es necesario
        nuevo_progreso = (indice + 1) * 100 / total_categorias
        if nuevo_progreso >= siguiente_umbral:
            logging.info(f'Progreso: {siguiente_umbral}% completado.')
            siguiente_umbral += 10

    # Mapear grupo_kmeans a segmento_categoria
    mapeo_segmentos = {1: 'PRICE SENSITIVE', 2: 'MID MARKET', 3: 'UP MARKET'}
    df_resultado['clasificacion_categoria'] = (
        df_resultado['grupo_kmeans'].map(mapeo_segmentos))

    msj = 'Clasificación realizada... '
    msj += 'Ahora se calculan los indices por categoria'
    logging.info(msj)

    # Obtener los percentiles como DataFrame.
    percentiles = df_resultado.groupby('category_description')[  # noqa: PD010
        'comb_customer_key_categoria'].quantile([0.05, 0.95]).unstack()
    percentiles.columns = ['perc_5', 'perc_95']

    # Hacer un merge de los percentiles con el DataFrame principal.
    df_resultado = df_resultado.merge(percentiles, on='category_description',
                                      how='left')

    # Función vectorizada para calcular los índices.
    conditions = [
        df_resultado['comb_customer_key_categoria'] <= df_resultado['perc_5'],
        df_resultado['comb_customer_key_categoria'] >= df_resultado['perc_95']
    ]

    # Extremos
    choices = [1, 3]

    # Se calcula el indice por cliente de cada categoria
    df_resultado['indice_cliente_categoria'] = (
        np.select(conditions, choices,
        default=1 + 2 * (
            df_resultado['comb_customer_key_categoria'] - df_resultado['perc_5']
            ) / (df_resultado['perc_95'] - df_resultado['perc_5'])))

    # Limpiar el DataFrame eliminando las columnas de percentiles.
    df_resultado = df_resultado.drop(['perc_5', 'perc_95'], axis=1)


    logging.info('Indices por categoria cálculados')


    return df_resultado


def clasificar_clientes_kmeans(df: pd.DataFrame) -> pd.DataFrame:
    """Clasifica clientes mediante K-means.

    Utiliza la clasificación de la categoría 'grupo_kmeans',
    ponderada por el valor total de ventas.

    Args:
    df: pd.DataFrame -> DataFrame con las clasificaciones por categoría.
    n_clusters: int -> Número de clusters para K-means (default: 3).

    Returns
    -------
    pd.DataFrame: DataFrame las clasificaciones por cliente
    """
    df_resultado = df.copy().reset_index()

    # Numero de segmentos
    n_clusters = 3

    # Calcular el índice ponderado
    df_resultado['indice_kmeans_ponderado'] = (
        df_resultado['indice_cliente_categoria'] * (
             df_resultado['ponderador_indice_cliente_categoria']))

    # Sumar y normalizar el índice ponderado total por cliente
    suma_ventas_cliente = df_resultado.groupby('customer_key')[
        'ponderador_indice_cliente_categoria'].sum()
    indice_kmeans_ponderado_total = df_resultado.groupby('customer_key')[
        'indice_kmeans_ponderado'].sum() / suma_ventas_cliente

    df_indices = pd.DataFrame({
        'customer_key': indice_kmeans_ponderado_total.index,
        'indice_cliente': indice_kmeans_ponderado_total
    }).reset_index(drop=True)

    # Segmentar clientes con al menos 3 filas
    customer_counts = df['customer_key'].value_counts()
    customers_to_keep = customer_counts[customer_counts >= 3].index

    # Filtrar los clientes elegibles para KMeans
    df_indices_kmeans = df_indices[df_indices['customer_key'].isin(customers_to_keep)].copy()

    logging.info('Preprocesamiento del Kmeans listo')
    # Aplicar KMeans
    kmeans = KMeans(n_clusters=n_clusters, random_state=42)
    df_indices_kmeans['grupo_kmeans_final'] = kmeans.fit_predict(
                    df_indices_kmeans[['indice_cliente']  # noqa: PD011
                                      ].values.reshape(-1, 1))
    logging.info('Kmeans listo')
    # Clasificar y mapear las etiquetas de los clusters
    mediana_kmeans = df_indices_kmeans.groupby('grupo_kmeans_final')[
                                                      'indice_cliente'].median()

    logging.info('Medianas agrupadas')
    orden_grupos_kmeans = mediana_kmeans.sort_values().index
    mapeo_kmeans = {
        old_label: (
            'PRICE SENSITIVE' if new_label == 0
            else 'MID MARKET' if new_label == 1
            else 'UP MARKET'
        )
        for new_label, old_label in enumerate(orden_grupos_kmeans)
    }
    logging.info('Mapeo configurado')


    df_indices_kmeans['clasificacion_cliente'] = df_indices_kmeans[
                                        'grupo_kmeans_final'].map(mapeo_kmeans)

    logging.info('Mapeo aplicado')

    # Unir la clasificación con el DataFrame original
    df_resultado = df_resultado.merge(df_indices[['customer_key',
                                                  'indice_cliente']],
                                                  on='customer_key', how='left')

    logging.info('Merge 1 listo')
    df_resultado = df_resultado.merge(df_indices_kmeans[['customer_key',
                                                  'clasificacion_cliente']],
                                                  on='customer_key', how='left')

    logging.info('Se coloca segmentación de los clientes con 3 o más categorias')

    # Es necesario agregar el segmento a aquellos clientes que se aislaron
    # por tener menos de 3 categorias
    max_ps = df_resultado[
            df_resultado['clasificacion_cliente'] == 'PRICE SENSITIVE'][
                                                        'indice_cliente'].max()
    min_mm = df_resultado[
            df_resultado['clasificacion_cliente'] == 'MID MARKET'][
                                                        'indice_cliente'].min()
    max_mm = df_resultado[
            df_resultado['clasificacion_cliente'] == 'MID MARKET'][
                                                        'indice_cliente'].max()
    min_um = df_resultado[
            df_resultado['clasificacion_cliente'] == 'UP MARKET'][
                                                        'indice_cliente'].min()

    # Límites que señalan de cual estado esta más cerca
    limite1 = (max_ps+min_mm)/2
    limite2 = (max_mm+min_um)/2

    # Actualiza los valores NaN en la columna 'clasificacion_cliente'
    df_resultado.loc[
    (df_resultado['clasificacion_cliente'].isna()) &
    (df_resultado['indice_cliente'] < limite1),
    'clasificacion_cliente'] = 'PRICE SENSITIVE'

    df_resultado.loc[
    (df_resultado['clasificacion_cliente'].isna()) &
    (df_resultado['indice_cliente'] >= limite1) &
    (df_resultado['indice_cliente'] < limite2),
    'clasificacion_cliente'] = 'MID MARKET'

    df_resultado.loc[
    (df_resultado['clasificacion_cliente'].isna()) &
    (df_resultado['indice_cliente'] >= limite2),
    'clasificacion_cliente'] = 'UP MARKET'

    logging.info('Se coloca segmentación del resto de los clientes')


    # Se deja con 3 decimales
    df_resultado['indice_cliente'] = df_resultado['indice_cliente'].round(3)

    # Limpiar DataFrame antes de retornar
    return df_resultado[['customer_key',
                         'clasificacion_cliente',
                         'category_description',
                         'clasificacion_categoria',
                         'indice_cliente']].drop_duplicates()






# -------------------------------------------------------------------------
# Main function
# -------------------------------------------------------------------------
def main() -> None:  # noqa: D103


    # Parse input variables
    args = vars(parser.parse_args())
    execution_date: str = args['execution_date']
    proyecto: str = args['project_id']  # noqa: F841
    formato:str = args['store_banner']
    store_id:tuple = args['store_id'] #PARCHE

    logging.info(f'execution_date: {execution_date}')


    # Set gbq client for all subsequent queries
    gbq_client = Client()

    # REGION: Configuracion inicial
    #----------------------------------------------------------------------

    #---
    # Principales
    #---

    # Usuario
    # Parche 1: Usuario ajustado a stores_id
    usuario = 'sophistication_segmentation_stores_id'

    # Proyecto en que se almacena
    # Parche 2: tabla ajustada a sector oriente
    esquema = 'CONOCIMIENTO_CLIENTE'
    tabla = 'CUSTOMER_SEGMENTATION_SOPHISTICATION_STORES_ID' #PARCHE

    # Ruta completa
    path_table = f'{proyecto}.{esquema}.{tabla}'

    # Nombre archivo Json
    # Parche 3: nombre del json ajustado a sector oriente
    nombre_json = 'ingest_customer_segmentation_sophistication_stores_id.json'

    #----------------------------------------------------------------------
    # ENDREGION

    # REGION: Parametros iniciales
    #----------------------------------------------------------------------
    # Formato en MAYUSCULAS
    formato_mayusculas = formato.upper()

    # Cantidad de veces para cpmsoderar outloer
    factor_outlier = 10

    # Mínimo de elementos para considerar al usuario en cierta categoria
    # (Si no compras este minimo de "galletas", no vale pena considerarte)
    minimo_items_categoria = 3

    #----------------------------------------------------------------------
    # ENDREGION

    # REGION: Inputs del proceso
    #----------------------------------------------------------------------


    # Convertir fecha de ejecución a fecha pendulum y restar un mes
    fecha_ejecucion = pendulum.parse(execution_date)
    fecha_ejecucion_menos_un_mes = fecha_ejecucion.subtract(months=1)

    # Formatear la fecha para obtener solo el año y el mes
    año_mes_final = fecha_ejecucion_menos_un_mes.format('YYYY-MM')

    # ID del mes
    monthid = fecha_ejecucion.format('YYYYMM')

    # Calcular la fecha inicial 12 meses antes
    año_mes_inicial = calcular_año_mes_inicial(año_mes_final)

    logging.info(f'Fecha final (AÑO-MES): {año_mes_final}')
    logging.info(f'Fecha inicial (AÑO-MES): {año_mes_inicial}')


    # Día 1 del primer mes
    fecha_inicial_ano = año_mes_inicial+'-01'


    logging.info(' ')
    logging.info('--------------------')
    logging.info(f'Se inicia el proceso para {formato_mayusculas} en monthid {monthid}')
    logging.info('--------------------')

    #----------------------------------------------------------------------
    # ENDREGION




    # REGION: Query  principal
    #----------------------------------------------------------------------

    logging.info(f'Año y mes inicial: {año_mes_inicial}')
    logging.info(f'Año y mes final: {año_mes_final}')



    # Se crea la query
    # PARCHE
    query_sophistication = SQL_QUERIES['query_sophistication'].substitute(
            fecha_inicial_ano = fecha_inicial_ano,
            formato = formato,
            store_id = store_id,
            minimo_items_categoria = minimo_items_categoria,
            proyecto =  proyecto
    )


    # Se realiza la query en caso de no existir

    df_valores_clientes = readBigQuery(
            query=query_sophistication,
            user=usuario,
            gbq_client=gbq_client
        )

    df_valores_clientes.columns = df_valores_clientes.columns.str.lower()

    msj = 'Datos por cliente y categoria consultados '
    msj +=  f'({df_valores_clientes.shape[0] / 1_000_000:.2f} millones de filas).'
    logging.info(msj)


    #----------------------------------------------------------------------
    # ENDREGION




    # REGION: Filtros a aplicar
    #----------------------------------------------------------------------

    #--
    # Filtro 1
    #--

    logging.info('Comienza el filtrado de outliers y categorias insignificantes')

    # Se eliminan del análisis aquellas cat con menos de 500 clientes
    conteo_categorias = df_valores_clientes[
    'category_description'].value_counts()

    # Obtener las categorías con al menos 500 filas
    categorias_a_conservar = conteo_categorias[conteo_categorias >= 500].index

    # Filtrar el DataFrame para eliminar las categorías
    # con menos de 500 filas
    df_valores_clientes = df_valores_clientes[
    df_valores_clientes[
            'category_description'].isin(categorias_a_conservar)].copy()

    #--
    # Filtro 2
    #--

    # Filtrar los valores cant_veces veces mayor al promedio de la
    # categoria. Esto tiene la finalidad de no ensuciar la normalización
    cant_veces_ppum = factor_outlier
    cant_veces_pu = factor_outlier

    # Calcular el promedio de PPUM por cada categoría
    promedios_ppum_por_categoria = df_valores_clientes.groupby(
    'category_description')['ppum_customer_key_categoria'].transform('mean')

    # Calcular el promedio de PU_customer_key_categoria x cada categoría
    promedios_pu_por_categoria = df_valores_clientes.groupby(
    'category_description')['pu_customer_key_categoria'].transform('mean')

    # Filtrar las filas que no excedan cant_veces veces el promedio
    # por categoria

    num_clientes = df_valores_clientes['customer_key'].nunique()

    df_valores_clientes = df_valores_clientes[
    (df_valores_clientes[
            'ppum_customer_key_categoria'
            ] <= cant_veces_ppum * promedios_ppum_por_categoria) &
    (df_valores_clientes[
            'pu_customer_key_categoria'
            ] <= cant_veces_pu * promedios_pu_por_categoria)].copy()

    num_clientes_new = df_valores_clientes['customer_key'].nunique()

    msj = 'Filtros aplicados. '
    msj += f'Se eliminaron {num_clientes-num_clientes_new} clientes'
    logging.info(msj)


    #----------------------------------------------------------------------
    # ENDREGION


    # REGION: Combinacion armónica
    #----------------------------------------------------------------------

    # Comb = (2 x PPUM x PU) / (PPUM + PU)
    df_valores_clientes['comb_customer_key_categoria'] = (
    2 * (df_valores_clientes['ppum_customer_key_categoria'] * df_valores_clientes[
            'pu_customer_key_categoria']
    ) / (df_valores_clientes['ppum_customer_key_categoria'] + df_valores_clientes[
            'pu_customer_key_categoria']))

    logging.info('Combinación creada.')

    #----------------------------------------------------------------------
    # ENDREGION


    # REGION: Segmentación de categorias
    #----------------------------------------------------------------------

    logging.info('Se comienza la segmentación de categoria:\n')

    # Se usa la función que clasifica las categorias
    df_clasificacion_categorias = clasificar_categorias_kmeans(
                                                            df_valores_clientes)



    #----------------------------------------------------------------------
    # ENDREGION


    # REGION: Calculo de ponderador de variabilidad
    #----------------------------------------------------------------------

    logging.info('Comienza el calculo del ponderador por variabilidad.')

    # Calcular los percentiles y el ponderador
    porcentaje = 90
    limite_porcentual_inf = (100 - porcentaje) / 2
    limite_porcentual_sup = 100 - limite_porcentual_inf
    limits = df_clasificacion_categorias.groupby('category_description')[
            'comb_customer_key_categoria'].agg(
    limite_inf=lambda x: np.percentile(x, limite_porcentual_inf),
    limite_sup=lambda x: np.percentile(x, limite_porcentual_sup)
    ).reset_index()
    limits['proporcion'] = limits['limite_sup'] / limits['limite_inf']

    # Calcular el total_value por categoría
    total_value_by_category = df_clasificacion_categorias.groupby(
            'category_description')['total_value'].sum().reset_index()

    # Seleccionar el 90% de las categorías con mayor total_value
    percentile_10 = np.percentile(total_value_by_category['total_value'], 10)
    categorias_top = total_value_by_category[
            total_value_by_category['total_value'] > percentile_10
            ]['category_description']

    # Filtrar `limits` utilizando las categorías seleccionadas
    limits_filtrado = limits[limits['category_description'].isin(categorias_top)]

    min_val = np.percentile(limits_filtrado['proporcion'], 5)
    max_val = np.percentile(limits_filtrado['proporcion'], 95)
    n = max_val

    limits['ponderador'] = (
            1 + (limits['proporcion'] - min_val) / (max_val - min_val) * (n - 1))

    # Limitando el ponderador entre 1 y N
    limits['ponderador'] = limits['ponderador'].clip(lower=1, upper=max_val)

    # Merge y cálculo de nuevo_grupo_kmeans
    df_clasificacion_categorias = df_clasificacion_categorias.merge(
            limits[['category_description', 'ponderador']],
            on='category_description', how='left')

    df_clasificacion_categorias['ponderador_indice_cliente_categoria'] = (
            df_clasificacion_categorias['total_value']*df_clasificacion_categorias['ponderador'])

    mensaje = 'Termina el cálculo del ponderador por variabilidad '
    mensaje = mensaje + f'(entre 1 y {np.round(max_val,2)})'
    logging.info(mensaje)

    #----------------------------------------------------------------------
    # ENDREGION


    # REGION: Segmentación de clientes
    #----------------------------------------------------------------------

    logging.info(' ')
    logging.info('Se comienza la segmentación de clientes:')

    # Se hace la segmentacion de clientes
    df_clasificacion_clientes = (
            clasificar_clientes_kmeans(df_clasificacion_categorias))



    df_clasificacion_clientes=  df_clasificacion_clientes[[
    'customer_key','category_description',
    'clasificacion_cliente','clasificacion_categoria','indice_cliente']].copy()



    # Se agregan columnas importantes
    df_clasificacion_clientes['monthid'] = monthid
    df_clasificacion_clientes['formato'] = formato

    df_clasificacion_clientes['date'] = (
        pd.to_datetime(df_clasificacion_clientes['monthid'].astype(str) + '01', format='%Y%m%d')
        .dt.strftime('%Y-%m-%d')
    )

    print('CLASIFICACIÓN CLIENTES COLUMNAS: ',df_clasificacion_clientes.columns)

    logging.info('Segmentación lista, subiendo a BQ')

    #--------------------------------------------------------------------------
    # ENDREGION



    # REGION: Post-procesamiento final
    #----------------------------------------------------------------------

    df_final = df_clasificacion_clientes[['customer_key',
                                        'clasificacion_cliente',
                                        'category_description',
                                        'clasificacion_categoria',
                                        'formato',
                                        'monthid',
                                        'date']]

    df_final = df_final.rename(columns={'category_description':'categoria',
                                        'formato':'store_banner'})

    df_final.columns = df_final.columns.str.lower()

    #----------------------------------------------------------------------
    # ENDREGION


    # REGION: Eliminacion y subida a GCP
    #----------------------------------------------------------------------

    # Si ya existe entonces se borra
    deleteFromTable(
    table_ref=path_table,
    where_clause=f"monthid = '{monthid}' and store_banner = '{formato}' and store_id = '{store_id}'",  # noqa: E501
    gbq_client=gbq_client,
    )

    # Se sube por batches

    # Categorías únicas
    categorias = (
        df_final['categoria']
        .astype(str)
        .unique()
        .tolist()
    )

    total_categorias = len(categorias)
    tam_grupo = 10
    ruta_json = os.path.join('gbq_objects', nombre_json)

    for i in range(0, total_categorias, tam_grupo):
        grupo = categorias[i:i + tam_grupo]
        df_final_aux = df_final[df_final['categoria'].isin(grupo)].copy()

        procesadas = min(i + tam_grupo, total_categorias)  # cuántas llevas
        indice_grupo = (i // tam_grupo) + 1
        total_grupos = (total_categorias + tam_grupo - 1) // tam_grupo

        # Mostrar primeras 5 categorías para no alargar el log
        vista_cats = ', '.join(grupo[:5]) + ('...' if len(grupo) > 5 else '')

        logging.info(
            f'Grupo {indice_grupo}/{total_grupos} | '
            f'Categorías {procesadas}/{total_categorias} | '
            f'Filas {len(df_final_aux):,} | {vista_cats}'
        )

        if df_final_aux.empty:
            logging.info(f'Grupo {indice_grupo}: sin filas, se omite upload.')
            continue

        try:
            uploadFrame(
                df_final_aux,
                table_ddl_json_path=ruta_json,
                project=proyecto,
                gbq_client=gbq_client,
                if_exists='append'
            )
            logging.info(f'Grupo {indice_grupo}: upload OK.')
        except Exception as e:  # noqa: BLE001
            logging.info(f'Grupo {indice_grupo}: error en upload -> {e}')





    #----------------------------------------------------------------------
    # ENDREGION

if __name__ == '__main__':
    main()
