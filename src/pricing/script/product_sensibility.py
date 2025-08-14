
# Default
from __future__ import annotations

import os
import sys
from datetime import datetime

# Pip
import numpy as np
import pandas as pd
from boto3 import Session
from sklearn.cluster import KMeans
from google.cloud.bigquery import Client
from sklearn.preprocessing import MinMaxScaler
from dateutil.relativedelta import relativedelta


directorio_actual = os.path.abspath(os.curdir)

while directorio_actual != os.path.sep:
    try:
        sys.path.append(directorio_actual)
        from credentials import credentials  # noqa: F401
        break  # Si la importación es exitosa, sale del bucle
    except ModuleNotFoundError:
        sys.path.pop()  # Remueve el directorio que no contenía el módulo
        directorio_actual = os.path.dirname(directorio_actual)  # Retrocede


from src.common.databases.queries import QueryDict  # noqa: E402
from src.common.gcp_extended.bigquery import (  # noqa: E402
    uploadFrame,
    readBigQuery,
    deleteFromTable,
    createTableFromJSON,
)
from src.common.gcp_extended.secretsmanager import getSecret  # noqa: E402


#--------------------------------------------------------------------------
# FUNCTIONS
#--------------------------------------------------------------------------

def calcular_año_mes_inicial(año_mes_final: str) -> str:
    """Calcula el año y mes inicial a partir de un año-mes final.

    Resta 11 meses a la fecha entregada, devolviendo el string con
    formato 'YYYY-MM' correspondiente al inicio del periodo de 12 meses.

    Parameters
    ----------
    año_mes_final : str
        Fecha final en formato 'YYYY-MM'.

    Returns
    -------
    str
        Fecha inicial en formato 'YYYY-MM', 11 meses antes de la final.
    """
    # Convertir el string a un objeto datetime
    fecha_final = datetime.strptime(año_mes_final, '%Y-%m')  # noqa: DTZ007

    # Restar 12 meses a la fecha final
    fecha_inicial = fecha_final - relativedelta(months=11)

    # Convertir la fecha inicial de vuelta a string
    return fecha_inicial.strftime('%Y-%m')





def getClusters(df_info:pd.DataFrame,
                variable:str,
                porcentaje_top:float,
                porcentaje_bottom:float,
                nombre_cluster:str)-> pd.DataFrame:
    """Segmenta datos numéricos en tres clusters: low, medium y high.

    Utiliza K-means sobre los valores sin outliers (según percentiles) y
    asigna manualmente los valores extremos. La segmentación se realiza
    sobre la variable entregada y se retorna un dataframe con las etiquetas
    asignadas.

    Parameters
    ----------
    df_info : pd.DataFrame
        DataFrame original que contiene al menos las columnas 'material' y
        la variable numérica a segmentar.

    variable : str
        Nombre de la columna numérica sobre la que se aplicará clustering.

    porcentaje_top : float
        Porcentaje superior que se considera como outlier (ej. 5 para 5%).

    porcentaje_bottom : float
        Porcentaje inferior que se considera como outlier.

    nombre_cluster : str
        Nombre de la nueva columna que contendrá los clusters asignados.

    Returns
    -------
    pd.DataFrame
        DataFrame con columnas 'material', la variable original, y una
        columna adicional con el nombre de cluster
        ('low', 'medium', 'high').
    """
    df_segmentacion = df_info[['material',
                               variable,
                             ]].copy().drop_duplicates(keep='first')

    # Calculamos los percentiles extremos (para obtener outliers)
    lower_bound = np.percentile(df_segmentacion[variable], porcentaje_bottom)
    upper_bound = np.percentile(df_segmentacion[variable], 100 - porcentaje_top)

    # Filtramos los datos para aplicar K-means
    filtered_df = df_segmentacion[(df_segmentacion[variable] > lower_bound) & (
                                df_segmentacion[variable] < upper_bound)].copy()

    outliers_low = df_segmentacion[df_segmentacion[variable] <= lower_bound].copy()
    outliers_high = df_segmentacion[df_segmentacion[variable] >= upper_bound].copy()

    # Aplicamos K-means a los datos filtrados
    kmeans = KMeans(n_clusters=3, random_state=42)
    filtered_df[nombre_cluster] = kmeans.fit_predict(filtered_df[[variable]])

    # Ordenamos los clusters de menor a mayor según la media geometrica
    cluster_centers = filtered_df.groupby(nombre_cluster)[variable].mean().sort_values().index
    cluster_labels = ['low', 'medium', 'high']
    cluster_mapping = dict(zip(cluster_centers, cluster_labels))

    # Reasignamos los nombres de clusters a los valores originales
    filtered_df[nombre_cluster] = filtered_df[nombre_cluster].map(cluster_mapping)

    # Asignamos los clusters a los valores extremos
    outliers_low[nombre_cluster] = 'low'
    outliers_high[nombre_cluster] = 'high'

    # Concatenamos todos los datos
    df_clusters = pd.concat([filtered_df, outliers_low, outliers_high])
    df_clusters.sort_values(by=[variable])

    return df_clusters


SQL_QUERIES = QueryDict({

    # Region: Explicación de query

    # RESUMEN:
    #----------
    # Tabla que contiene cuanta venta/gasto tiene cada producto (conside
    # rando la suma de todos los clientes que suparan los filtros) y que
    # porcentaje corresponde esta venta dentro de su categoria.

    # TABLAS INTERMEDIAS:
    #--------------------
    # distinct_products: para filtrar error/duplicidad por proveedores
    # filtered_customers: para filtrar clientes de ciertos segmentos de
    #                     sofisticacion
    # category_counts: para filtrar aquellos compras de clientes que tienen
    #                  un mínimo de compras en la categoria (no clientes
    #                  esporádicos)

    # customer_category_avg: Tiene el gasto promedio por categoria de cada
    #                        cliente

    # category_avg: Tiene el gasto promedio de cada categoria

    # customer_outliers: Tiene a los clientes outliers (en gasto) de
    #                    cada categoria.

    # data_customer_filtrada: Deja solo las transacciones de los clientes
    #                         que no son outliers.

    # product_totals: Usando la data filtrada obtiene cuanto se gasto en
    #                 cada producto de cada categoria (suma de todos los
    #                 clientes seleccionados).

    # category_totals: Usando la data filtrada obtiene cuanto se gasto en
    #                  cada cada categoria (suma de todos los clientes
    #                  seleccionados).

    # porcentaje_totals: Obtiene a que porcentaje de su categoria equivale
    #                    el gasto de cada producto

    #Endregion

'query_principal':
"""
-- Eliminar duplicidades de la dim products

WITH distinct_products AS (

  SELECT DISTINCT
    EAN,
    CAT_DSC AS CATEGORY_DESCRIPTION,
    GRUPO_DSC as SUB_CATEGORY_DESCRIPTION,
    NM AS PRODUCT_DESCRIPTION,
    SKU_PRODUCT AS PRODUCT_ID,
    NEG_DSC

  FROM `cl-bigdata-analytics.ML_LAB.VW_DIM_PRODUCT`

),

-- Clientes sensibles al precio en cada categoria
filtered_customers AS (
        SELECT
        CUSTOMER_KEY as CUSTOMER_KEY,
        category_description
        FROM (SELECT
                  CSS.*,
                  H.customer_key as customer_key
              FROM `cl-bigdata-analytics.SANDBOX_BMOLINAB.${tabla_sofisticacion}` CSS
              INNER JOIN `cl-cda-unidata-prod.DS_PROD_CLIENTES_IC.VW_CDA_CST_DEID` H
                  ON CSS.customer_id = CAST(H.pda_customer_key AS BIGNUMERIC)
                WHERE CSS.formato = '${formato}'
                AND CSS.clasificacion_categoria in ('PRICE SENSITIVE')
                AND CSS.rango = '${rango}')
                ),

-- Minimo de compras para considerar a un cliente en una categoria
category_counts AS (

  SELECT
    A.CUSTOMER_KEY,
    P.CATEGORY_DESCRIPTION,
    COUNT(*) AS CATEGORY_COUNT
  FROM `cl-bigdata-analytics-prod.ML_LAB.VW_SALES_ITEM` A
  INNER JOIN distinct_products P
    ON A.EAN = P.EAN
  INNER JOIN `cl-bigdata-analytics.ML_LAB.VW_DIM_STORE` DS
    ON A.STORE_ID = DS.STORE_ID
  WHERE
    A.TRANSACTION_DATE >= DATE('${fecha_inicial_ano}')
    AND A.TRANSACTION_DATE < DATE_ADD(DATE('${fecha_inicial_ano}'), INTERVAL  ${cant_meses} MONTH)
    AND DS.STORE_BANNER = '${formato}'
  GROUP BY A.CUSTOMER_KEY, P.CATEGORY_DESCRIPTION
  HAVING COUNT(*) >= ${minimo_items_categoria}

),

-- Consulta principal
data_customer as (
SELECT
  A.CUSTOMER_KEY AS CUSTOMER_ID,
  A.STORE_ID,
  A.MARKET_BASKET_KEY,
  P.PRODUCT_DESCRIPTION,
  P.PRODUCT_ID,
  P.CATEGORY_DESCRIPTION,
  P.SUB_CATEGORY_DESCRIPTION,
  A.QUANTITY,
  A.VALUE,
  A.TRANSACTION_DATE AS P_DATE

FROM `cl-bigdata-analytics-prod.ML_LAB.VW_SALES_ITEM` A
INNER JOIN distinct_products P
  ON A.EAN = P.EAN
INNER JOIN `cl-bigdata-analytics.ML_LAB.VW_DIM_STORE` D
  ON A.STORE_ID = D.STORE_ID
INNER JOIN category_counts CC
  ON A.CUSTOMER_KEY = CC.CUSTOMER_KEY
  AND P.CATEGORY_DESCRIPTION = CC.CATEGORY_DESCRIPTION
INNER JOIN filtered_customers FC
  ON A.CUSTOMER_KEY = FC.CUSTOMER_KEY
  AND P.CATEGORY_DESCRIPTION = FC.category_description
WHERE
  A.TRANSACTION_DATE >= DATE('${fecha_inicial_ano}')
  AND A.TRANSACTION_DATE < DATE_ADD(DATE('${fecha_inicial_ano}'), INTERVAL  ${cant_meses} MONTH)
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
                WHERE CANAL_VENTA IN ('PEDIDOS YA','CORNER SHOP','RAPPI','RAPPI TURBO')
            )
  AND D.STORE_BANNER = '${formato}'
  AND A.CUSTOMER_KEY <> MD5('CST^CL^-1')
  ),

  -- Gasto diario de cada usuario por categoria
  customer_category_avg_day AS (
        SELECT
            CUSTOMER_ID,
            P_DATE,
            CATEGORY_DESCRIPTION,
            SUM(VALUE) AS SUM_CUSTOMER_DAY_VALUE
        FROM data_customer
        GROUP BY CUSTOMER_ID, P_DATE, CATEGORY_DESCRIPTION
        ),

  -- Gasto promedio de cada usuario en la categoria

  customer_category_avg AS (
        SELECT
            CUSTOMER_ID,
            CATEGORY_DESCRIPTION,
            AVG(SUM_CUSTOMER_DAY_VALUE) AS AVG_CUSTOMER_VALUE
        FROM customer_category_avg_day
        GROUP BY CUSTOMER_ID, CATEGORY_DESCRIPTION
    ),

    -- Gasto promedio por categoria
    category_avg AS (
        SELECT
            CATEGORY_DESCRIPTION,
            AVG(AVG_CUSTOMER_VALUE) AS AVG_CATEGORY_VALUE
        FROM customer_category_avg
        GROUP BY CATEGORY_DESCRIPTION
    ),

    -- Eliminar outliers
    customer_outliers AS (
        SELECT
            a.CUSTOMER_ID,
            a.CATEGORY_DESCRIPTION
        FROM customer_category_avg a
        INNER JOIN category_avg b
        ON a.CATEGORY_DESCRIPTION = b.CATEGORY_DESCRIPTION
        WHERE a.AVG_CUSTOMER_VALUE > ${factor_outliers} * b.AVG_CATEGORY_VALUE

    ),
  -- Principal sin outiers
    data_customer_filtrada AS (
        SELECT dc.*
        FROM data_customer dc
        LEFT JOIN customer_outliers co
            ON dc.CUSTOMER_ID = co.CUSTOMER_ID
            AND dc.CATEGORY_DESCRIPTION = co.CATEGORY_DESCRIPTION
        WHERE co.CUSTOMER_ID IS NULL
    ),

    -- Aporte de cada producto a la categoria
    product_totals AS (
        SELECT
            product_id,
            product_description,
            category_description,
            SUM(value) AS product_total
        FROM data_customer_filtrada

        GROUP BY product_id, product_description, category_description

    ),


    -- Aporte de cada categoria en formato
    category_totals AS (

        SELECT
            category_description,
            SUM(value) AS category_total
        FROM data_customer_filtrada
        GROUP BY category_description

    ),


  -- Aporte de cada categoria en formato
    porcentaje_totals AS (

        SELECT
            pt.product_id,
            pt.product_description,
            pt.category_description,
            pt.product_total,
            ct.category_total,
            (pt.product_total / ct.category_total * 100) AS porcentaje
        FROM product_totals pt
        INNER JOIN category_totals ct ON pt.category_description = ct.category_description

    )


  -- Query final
    SELECT
        category_description,
        product_id,
        product_description,
        product_total as gasto_producto,
        ROUND(porcentaje, 6) AS porcentaje -- Redondeamos el porcentaje a dos decimales
    FROM porcentaje_totals
    ORDER BY category_description, product_description;
""",

#--------------------------------------------------------------------------

# Region: Explicación de query

# RESUMEN:
#----------
# Query para obtener familias

#Endregion
'query_genfix':
"""
Select sku_padre,
        desc_padre,
        material,
        desc_material
from cl-bigdata-analytics.ML_LAB.TBL_PRICING_GENFIX
""",


#--------------------------------------------------------------------------


# Region: Explicación de query

# RESUMEN:
#----------
# Tabla que entrega el gasto total por producto (material)

# TABLAS INTERMEDIAS:
#--------------------
# distinct_products: para filtrar error/duplicidad por proveedores
# data_customer: todas las transacciones con los filtros solicitados


'query_gasto':
"""
-- Eliminar duplicidades de la dim products

WITH distinct_products AS (

  SELECT DISTINCT
    EAN,
    CAT_DSC AS CATEGORY_DESCRIPTION,
    GRUPO_DSC as SUB_CATEGORY_DESCRIPTION,
    NM AS PRODUCT_DESCRIPTION,
    SKU_PRODUCT AS PRODUCT_ID,
    NEG_DSC

  FROM `cl-bigdata-analytics.ML_LAB.VW_DIM_PRODUCT`

),

-- Consulta principal
data_customer as (
SELECT
  A.CUSTOMER_KEY AS CUSTOMER_ID,
  A.STORE_ID,
  A.MARKET_BASKET_KEY,
  P.PRODUCT_DESCRIPTION,
  P.PRODUCT_ID,
  P.CATEGORY_DESCRIPTION,
  P.SUB_CATEGORY_DESCRIPTION,
  A.QUANTITY,
  A.VALUE,
  A.TRANSACTION_DATE AS P_DATE

FROM `cl-bigdata-analytics-prod.ML_LAB.VW_SALES_ITEM` A
INNER JOIN distinct_products P
  ON A.EAN = P.EAN
INNER JOIN `cl-bigdata-analytics.ML_LAB.VW_DIM_STORE` D
  ON A.STORE_ID = D.STORE_ID
WHERE
  A.TRANSACTION_DATE >= DATE('${fecha_inicial_ano}')
  AND A.TRANSACTION_DATE < DATE_ADD(DATE('${fecha_inicial_ano}'), INTERVAL ${cant_meses} MONTH)
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
                WHERE CANAL_VENTA IN ('PEDIDOS YA','CORNER SHOP','RAPPI','RAPPI TURBO')
            )
  AND D.STORE_BANNER = '${formato}'
  )

  -- Query final
    Select
        Cast(product_id as INTEGER) as material,
        sum(value) as gasto_material
    from data_customer
    group by product_id

"""
})

# REGION: Inputs del proceso
#----------------------------------------------------------------------

# Formato a calcular
formato = 'Alvi'

# Mes a cargar
monthid = 202506

#----------------------------------------------------------------------
# ENDREGION

# REGION: Configuracion inicial
#----------------------------------------------------------------------

#---
# Principales
#---

# Usuario
usuario = 'product_sensibility'

# Cliente GCP
gbq_client_cda = Client(project='cl-cda-unidata-dev')

# Sesion aws
boto3_session = Session(**getSecret(
    project='cl-bigdata-analytics',
    secret_name='bdaa_aws_credentials'  # noqa: S106
))

# Configuracion nube
env_name = 'dev'

env_db_name = 'dev' if  env_name == 'test' else env_name
db_perm = f'{env_db_name}_perm'
db_temp = f'{env_db_name}_temp'
athena_workgroup = f'smu-datalake-{env_name}-workgroup'

# Minimo de compras por categoria para considerar las compras de un cliente
# en esa categora
minimo_items_categoria = 3

# Cantidad de meses a considerar
cant_meses = 12

# Factor para considerar a un cliente outlier (cantidad de veces
# superior que deben ser sus compras para entrar en outlier)
factor_outliers = 10

#----------------------------------------------------------------------
# ENDREGION


# REGION: Parametros iniciales
#----------------------------------------------------------------------

# Convertir a fecha
fecha = pd.to_datetime(str(monthid), format='%Y%m')

# Formatear como YYYY-MM
año_mes_final = fecha.strftime('%Y-%m')

# Calcular la fecha inicial 12 meses antes
año_mes_inicial = calcular_año_mes_inicial(año_mes_final)

print(f'Fecha final (AÑO-MES): {año_mes_final}')
print(f'Fecha inicial (AÑO-MES): {año_mes_inicial}')

# Para guardar el nombre de forma más correcta se reemplaza el "-"
año_mes_inicial_aux = año_mes_inicial.replace('-', '')
año_mes_final_aux = año_mes_final.replace('-', '')

# Se crea string del rango
rango = f'{año_mes_inicial_aux}-{año_mes_final_aux}'

# Día 1 del primer mes
fecha_inicial_ano = año_mes_inicial+'-01'


# Formato en MAYUSCULAS
formato_mayusculas = formato.upper()

print(' ')
print('--------------------')
print(f'Se inicia el proceso para {formato_mayusculas}')
print('--------------------')
print(f'Año y mes inicial: {año_mes_inicial}')
print(f'Año y mes final: {año_mes_final}')

#----------------------------------------------------------------------
# ENDREGION


# REGION: Query de tabla temporal
#----------------------------------------------------------------------

print('Se genera tabla temporal')

#----------------------------------------------------------------------
#ENDREGION



# REGION: Query de tabla con el detalle de los PRICE SENSITIVE
#----------------------------------------------------------------------

# Taabla con info sofisticacion
tabla_sofisticacion = 'customer_segmentation_sophistication'

# Se genera query definitiva
query_principal = SQL_QUERIES['query_principal'].substitute(
        fecha_inicial_ano=fecha_inicial_ano,
        formato=formato,
        rango = rango,
        minimo_items_categoria = minimo_items_categoria,
        cant_meses = cant_meses,
        factor_outliers = factor_outliers,
        tabla_sofisticacion = tabla_sofisticacion
)

print('Inicia la consulta de principal ...')

df_detalle = readBigQuery(
    query=query_principal,
    user=usuario,
    gbq_client=gbq_client_cda)

# Agregar columnas adicionales
df_detalle['formato'] = formato

# Rango para tener referencia
df_detalle['rango'] = rango

# Se crea material (product_id sin ceros iniciales)
df_detalle['material'] = df_detalle['product_id'
                                ].astype(str).str.lstrip('0').astype(int)

print('Termina la consulta de principal...')

#----------------------------------------------------------------------
# ENDREGION

# REGION: Tabla familias
#----------------------------------------------------------------------

query_genfix = SQL_QUERIES['query_genfix'].substitute()
df_genfix = readBigQuery(
    query=query_genfix,
    user=usuario,
    gbq_client=gbq_client_cda)

print('Se lee el genfix desde PDA')


# Eliminar espacios finales y convertir los nombres a minúsculas
df_genfix.columns = df_genfix.columns.str.strip().str.lower()

# Eliminar columna de descripcion de material, ya que esa se obtendra
# del merge con df_detalles.
df_genfix = df_genfix[['material','sku_padre']]

# Se crea una copia del original
df_genfix_original = df_genfix.copy()

# Se obtiene el tamaño original
tamaño_original = df_genfix.shape[0]

# Eliminar duplicados basándose en las columnas 'material',
# conservando el primero
df_genfix = df_genfix.drop_duplicates(subset=['material'],
                                    keep='first')

# Se obtiene el tamaño final
tamaño_final = df_genfix.shape[0]

if tamaño_original != tamaño_final:
    print(f'Se eliminaron {tamaño_original-tamaño_final} elementos duplicados')


# Se asegura que sku_padre sea int
df_genfix['sku_padre'] = df_genfix['sku_padre'].astype(int)


#----------------------------------------------------------------------
# ENDREGION

# REGION: Union df_detalle y df_genfix
#----------------------------------------------------------------------

# Realizas la fusión de los DataFrames
# Donde df_detalle_total quedara con las columnas:
#  sku_padre y sku_padre_description
df_detalle2 = df_detalle[['category_description',
                                'product_id',
                                'product_description',
                                'gasto_producto',
                                'porcentaje',
                                'material']].merge(
                    df_genfix,
                    on='material',
                    how='left')


# Crear 'genfix' que es 'si' si 'sku padre' no es NaN y 'no' si es NaN
df_detalle2['genfix'] = np.where(df_detalle2['sku_padre'].notna(),
                                'si', 'no')

# Rellenar 'sku padre' con 'material' donde 'sku padre' es NaN
# Que son auqellos que no estan en genfix
df_detalle2['sku_padre'] = df_detalle2['sku_padre'].fillna(
    df_detalle2['material'])
df_detalle2['sku_padre'] = df_detalle2['sku_padre'].astype(int)


# Crear un set de valores únicos en la columna 'sku padre' excluyendo
# aquellos donde 'sku padre' es igual a 'material'
sku_padre_set = set(df_detalle2.loc[
                        df_detalle2['sku_padre'] != df_detalle2['material'],
                        'sku_padre'].dropna())

# Crear 'con_familia' basada en si 'material' está en set 'sku padre'
df_detalle2['con_familia'] = df_detalle2['sku_padre'].apply(
                                lambda x: 'si' if x in sku_padre_set else 'no')

#----------------------------------------------------------------------
# ENDREGION


# REGION: Se agrega el porcentaje de la categoria y la media armonica
#----------------------------------------------------------------------

df_detalle2['gasto_producto'] = df_detalle2['gasto_producto'].astype('int64')

# Paso 1: Calculamos el gasto total por segmento
gasto_total = df_detalle2['gasto_producto'].sum()

# Paso 2: Calculamos el gasto por categoría dentro de cada segmento
gasto_categoria = df_detalle2.groupby(['category_description'])[
                                            'gasto_producto'].transform('sum')
df_detalle2['gasto_categoria'] = gasto_categoria

# Paso 3: Calculamos el porcentaje del gasto de cada categoría respecto
# al total del segmento
df_detalle2['porcentaje_categoria'] = (df_detalle2['gasto_categoria'
                                                ] / gasto_total) * 100

#----------------------------------------------------------------------
# ENDREGION


# REGION: Se agrega la media armonica (individual y de la familia)
#----------------------------------------------------------------------

# Primero se ordena el dataframe
df_detalle2 = df_detalle2[['genfix',
                        'con_familia',
                        'category_description',
                        'material',
                        'product_description',
                        'sku_padre',
                        'gasto_producto',
                        'gasto_categoria',
                        'porcentaje',
                        'porcentaje_categoria'
                        ]]

# Media armonica de porcentaje producto y categoria
df_detalle2['media_geometrica'] = np.sqrt(
    df_detalle2['porcentaje'] * df_detalle2['porcentaje_categoria'])

# Crear el objeto MinMaxScaler
scaler = MinMaxScaler()

# Aplicar el scaler a la columna 'media_geometrica'
df_detalle2['media_geometrica'] = scaler.fit_transform(
                                        df_detalle2[['media_geometrica']])

# Como considerar el porcentaje de las familias:
# Puede ser como la suma de todos los miembros: "Son el mismo producto"
# Puede ser el maximo: "El miembro de mayor venta es el que representa"
opcion = 'max'  # 'suma' o 'max'

# Funciones permitidas para agregar
funciones = {
    'suma': 'sum',
    'max': 'max',
}

# Filtro
condicion = (df_detalle2['genfix'] == 'si') & (df_detalle2['con_familia'] == 'si')

# Agregación sin apply (evita FutureWarning y es más rápido)
df_familia = (
    df_detalle2.loc[condicion]
    .groupby('sku_padre', as_index=False)['media_geometrica']
    .agg(media_geometrica_familia=funciones[opcion])
)

# Unimos los resultados al dataframe original
df_detalle2 = df_detalle2.merge(df_familia, on='sku_padre', how='left')

# Para los que no cumplen la condición, copiamos el valor de 'porcentaje'
df_detalle2['media_geometrica_familia'] = df_detalle2.apply(
    lambda row: row['media_geometrica'] if pd.isna(row['media_geometrica_familia']
                                            ) else row['media_geometrica_familia'],
    axis=1
)

print('Se agrega media armonica')

#----------------------------------------------------------------------
# ENDREGION



# REGION: Se realiza la segmentacion
#----------------------------------------------------------------------

# En Unimarc hay muchos productos "relleno" con compras casi 0, los cuales
# se eliminan del analisis (directo a low/BKG), esto no ocurre en los otros
# formatos
porcentaje_bottom = 50 if formato == 'Unimarc' else 0

nombre_cluster = 'cluster'
df_clusters = getClusters(df_detalle2,
                        'media_geometrica_familia',
                        porcentaje_top = 0,
                        porcentaje_bottom =  porcentaje_bottom,
                        nombre_cluster= nombre_cluster)

print('Se crean los clúster KVI')

#----------------------------------------------------------------------
# ENDREGION



# REGION: Unir clusters al dataframe princiapl
#----------------------------------------------------------------------

df_detalle3 = df_detalle2.merge(
                    df_clusters[['material', nombre_cluster]],
                    on = 'material', how = 'left')


#----------------------------------------------------------------------
# ENDREGION

# REGION: Se agrega columna gasto_material
# -------------------------------------------------------------------------

# Se genera query definitiva
query_gasto_total = SQL_QUERIES['query_gasto'].substitute(
    fecha_inicial_ano=fecha_inicial_ano,
    formato=formato,
    cant_meses = cant_meses

)

print('Inicia la consulta de gasto total ...')

# Resultado de la query
df_gasto_total = readBigQuery(
    query=query_gasto_total,
    user=usuario,
    gbq_client=gbq_client_cda)

df_detalle4 = df_detalle3.merge(df_gasto_total, on='material',how='left')


#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# ENDREGION


# REGION: *OPCIONAL* Forzar Las 80 categorias
#----------------------------------------------------------------------


# Paso 1: Agrupar por categoría y sumar gastos de productos
category_sums = df_detalle4.groupby('category_description'
                                    )['gasto_material'].sum()

# Paso 2: Seleccionar las top 80 categorías con más gasto
top_categories = category_sums.nlargest(80).index

# # Filtrar el DataFrame original para incluir solo esas 80 categoría
filtered_df = df_detalle4[df_detalle4['category_description'].isin(
    top_categories)]

# Paso 3 y 4: Seleccionar productos por categoría según el rango específico
result_list = []
for i, category in enumerate(top_categories, start=1):
    category_data = filtered_df[filtered_df['category_description'] == category]
    category_data = category_data.sort_values('media_geometrica_familia',
                                            ascending=False)
    if i <= 10:  # noqa: SIM114
        top_products = category_data.head(3)
    elif i <= 20:  # noqa: SIM114
        top_products = category_data.head(3)
    elif i <= 30:  # noqa: SIM114
        top_products = category_data.head(3)
    elif i <= 40:
        top_products = category_data.head(3)
    elif i <= 50:  # noqa: SIM114
        top_products = category_data.head(2)
    elif i <= 60:  # noqa: SIM114
        top_products = category_data.head(2)
    elif i <= 70:  # noqa: SIM114
        top_products = category_data.head(2)
    elif i <= 80:
        top_products = category_data.head(2)

    result_list.append(top_products[['material',
                                    'product_description',
                                    'sku_padre']])

# Concatenar todos los DataFrames recolectados en result_list
df_impuestos = pd.concat(result_list, ignore_index=True)

# Paso 5: Obtener la lista de familias únicos de df_impuestos
materiales_impuestos = df_impuestos['sku_padre'].unique()

# Paso 6: Filtrar df_detalle3 para los materiales en df_impuestos
filtro = df_detalle4['sku_padre'].isin(materiales_impuestos)


# Paso 7: Actualizar los valores a 'high'
df_detalle4.loc[filtro, 'cluster'] = 'high'


print('Se hace el merge con gasto de productos y se imponen ciertas categorias')
#----------------------------------------------------------------------
# ENDREGION


# REGION: SE GENERA DF_FINAL
# ---------------------------------------------------------------------
# Mapeo de cluster a KVI
mapa_kvi = {'low': 'BKG', 'medium': 'KCI', 'high': 'KVI'}


df_final = df_detalle4.copy()


rango=f'{año_mes_inicial_aux}-{año_mes_final_aux}'
df_final = df_final.assign(
    formato = formato,
    rango=rango,
    categoria=df_final['category_description'],
    descripcion_material=df_final['product_description'],
    material_padre=df_final['sku_padre'],
    familia=df_final['con_familia'],
    indice_sensibilidad=df_final['media_geometrica'].round(5),
    indice_sensibilidad_familia=df_final['media_geometrica_familia'].round(5),
    KVI=df_final['cluster'].map(mapa_kvi)
)[['formato',
    'rango',
    'categoria',
    'material',
    'descripcion_material',
    'material_padre',
    'genfix',
    'familia',
    'indice_sensibilidad',
    'indice_sensibilidad_familia',
    'KVI'
]]

print('Se crea dataframe final')

#----------------------------------------------------------------------
# ENDREGION


# REGION: SUBIR A GCP LA TABLA
# ---------------------------------------------------------------------

# Definir el WHERE
where_clause = f"formato = '{formato}' and rango = '{rango}'"
base_dir = os.path.dirname(os.path.abspath(__file__))
table_ddl_json_path = os.path.normpath(
    os.path.join(base_dir, '..', 'gbq_objects', 'ingest_product_sensibility.json')
)

createTableFromJSON(
        table_ddl_json_path = table_ddl_json_path,
        project='cl-bigdata-analytics',
        gbq_client=gbq_client_cda,
        if_exists = 'ignore'
    )

# Se elimina los datos para cierto formato y rango (si existen)
deleteFromTable(table_ref='cl-bigdata-analytics.ML_LAB.PRODUCT_SENSIBILITY',
                where_clause=where_clause,
                gbq_client=gbq_client_cda)




# Se carga en BQ con los datos recalculados
uploadFrame(
    df_final,
    table_ddl_json_path=table_ddl_json_path,
    project='cl-bigdata-analytics',
    gbq_client=gbq_client_cda,
    if_exists='append'
)

print('Se sube la tabla a GCP')

#----------------------------------------------------------------------
# ENDREGION



