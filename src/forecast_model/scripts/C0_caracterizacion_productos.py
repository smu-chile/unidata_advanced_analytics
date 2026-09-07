from __future__ import annotations

import os
import sys
import logging
import argparse
from logging import config

# Pip
from google.cloud.bigquery import Client


directorio_actual = os.path.abspath(os.curdir)

while directorio_actual != os.path.sep:
    try:
        sys.path.append(directorio_actual)
        from credentials import credentials  # noqa: F401
        break  # Si la importación es exitosa, sale del bucle
    except ModuleNotFoundError:
        sys.path.pop()  # Remueve el directorio que no contenía el módulo
        directorio_actual = os.path.dirname(directorio_actual)  # Retrocede


import common.gcp_extended.secretsmanager as secretmanager  # noqa: E402
from common.constants import LOGGING_CONFIG  # noqa: E402
from common.databases.queries import QueryDict  # noqa: E402
from common.gcp_extended.bigquery import (  # noqa: E402
    uploadFrame,  # noqa: F401
    readBigQuery,
    deleteFromTable,  # noqa: F401
    createTableAsSelect,  # noqa: F401
)


#######################################################
##########---------- 0. Auxiliares ----------##########
#######################################################


##########           0.1 Queries

# Query para obtener la data procesada de forecast para todos los
# productos disponibles.

QUERY_FORECAST= QueryDict({
    'query_data_procesada':
    """

    SELECT * EXCEPT(
        PRIMER_DIA_MES,
        ULTIMO_DIA_MES,
        P_WEEK,
        P_MONTH,
        VARIACION_TOP1_SUSTITUTO,
        VARIACION_TOP3_SUSTITUTOS
        )

    FROM `${path_table}`
    Where store_banner = 'Unimarc'
    """})



SQL_QUERIES_ALL_PROMOS = QueryDict({
    'query_promos_forecasting':
"""
WITH productos_objetivo AS (
    -- 1. Identificar los productos que tienen historial en la tabla forecast.
    SELECT DISTINCT
        ean
    FROM `${path_table}`
    Where store_banner = 'Unimarc'
),

historial_promocional AS (
    -- 2. Recuperar todo el historial promocional de los EAN objetivo.
    -- En esta etapa no se vuelve a filtrar por string_promos.
    SELECT DISTINCT
        promo.n_promocion,
        promo.nombre_promocion,
        promo.descripcion_evento_promocional,
        promo.descripcion_mecanica,
        promo.desc_categoria,
        promo.material,
        promo.desc_material,
        promo.un_medida_venta,
        promo.ean,

        SAFE_CAST(
            promo.precio_modal AS FLOAT64
        ) AS precio_modal,

        SAFE_CAST(
            promo.precio_modal_total AS FLOAT64
        ) AS precio_modal_total,

        SAFE_CAST(
            promo.precio_promocional AS FLOAT64
        ) AS precio_promocional,

        SAFE_CAST(
            promo.precio_total_promocional AS FLOAT64
        ) AS precio_total_promocional,

        promo.ahorro,
        promo.ahorro_total,
        promo.desc_promocion,
        promo.cantidad_n,
        promo.cantidad_m,

        DATE(
            promo.fecha_inicio_de_promocion
        ) AS fecha_inicio_de_promocion,

        DATE(
            promo.fecha_fin_de_promocion
        ) AS fecha_fin_de_promocion,

        promo.porcentaje_cobertura,
        promo.porcentaje_descuento,
        promo.organizacion_ventas,
        promo.canal_distribucion

    FROM `cl-bigdata-analytics-preprod.CDA_VISTAS.VW_FACT_WORKFLOW` AS promo

    INNER JOIN productos_objetivo AS producto
        ON promo.ean = producto.ean

    WHERE promo.organizacion_ventas = '1000'
      AND promo.canal_distribucion = '10'
      AND promo.registro_valido = 'X'

      -- Incluye promociones iniciadas antes de 2024
      -- que continúan vigentes durante 2024.
      AND DATE(
          promo.fecha_fin_de_promocion
      ) >= DATE '2024-01-01'

      -- Excluir períodos promocionales inconsistentes.
      AND DATE(
          promo.fecha_fin_de_promocion
      ) >= DATE(
          promo.fecha_inicio_de_promocion
      )
),

promociones_diarias AS (
    -- 3. Expandir cada promoción a una fila por día de vigencia.
    SELECT
        promo.*,
        p_date
    FROM historial_promocional AS promo

    CROSS JOIN UNNEST(
        GENERATE_DATE_ARRAY(
            GREATEST(
                promo.fecha_inicio_de_promocion,
                DATE '2024-01-01'
            ),
            promo.fecha_fin_de_promocion
        )
    ) AS p_date
),

promociones_con_precio_valido AS (
    -- 4. Conservar solo precios promocionales válidos:
    -- no nulos, finitos y estrictamente mayores que cero.
    SELECT *
    FROM promociones_diarias
    WHERE precio_promocional IS NOT NULL
      AND NOT IS_NAN(precio_promocional)
      AND NOT IS_INF(precio_promocional)
      AND precio_promocional > 0
),

precio_promocional_diario AS (
    -- 5. Ordenar las promociones activas de cada EAN y día
    -- desde el menor precio promocional válido.
    SELECT
        *,

        ROW_NUMBER() OVER (
            PARTITION BY
                ean,
                p_date
            ORDER BY
                precio_promocional ASC,
                n_promocion ASC,
                material ASC,
                nombre_promocion ASC
        ) AS orden_precio_diario,

        COUNT(*) OVER (
            PARTITION BY
                ean,
                p_date
        ) AS cantidad_promociones_activas

    FROM promociones_con_precio_valido
)

-- 6. Conservar una única promoción por EAN y día.
SELECT
    * EXCEPT (
        orden_precio_diario
    ),

    1 AS FLAG_PROMO,

    precio_promocional
        AS precio_promocional_minimo,

    desc_promocion
        AS desc_promocion_minimo

FROM precio_promocional_diario

WHERE orden_precio_diario = 1

ORDER BY
    ean,
    p_date
"""
})


SQL_QUERIES_ALL_PROMOS_COMBINACION = QueryDict({
    'query_promos_forecasting_combinacion':
"""
WITH productos_objetivo AS (
    -- 1. Identificar los productos que tienen historial en la tabla forecast.
    SELECT DISTINCT
        ean
    FROM `${path_table}`
    Where store_banner = 'Unimarc'
),

historial_promocional AS (
    -- 2. Recuperar todo el historial promocional de los EAN objetivo.
    -- En esta etapa no se vuelve a filtrar por string_promos.
    SELECT DISTINCT
        promo.n_promocion,
        promo.nombre_promocion,
        promo.descripcion_evento_promocional,
        promo.descripcion_mecanica,
        promo.desc_categoria,
        promo.material,
        promo.desc_material,
        promo.un_medida_venta,
        promo.ean,

        SAFE_CAST(
            promo.precio_modal AS FLOAT64
        ) AS precio_modal,

        SAFE_CAST(
            promo.precio_modal_total AS FLOAT64
        ) AS precio_modal_total,

        SAFE_CAST(
            promo.precio_promocional AS FLOAT64
        ) AS precio_promocional,

        SAFE_CAST(
            promo.precio_total_promocional AS FLOAT64
        ) AS precio_total_promocional,

        promo.ahorro,
        promo.ahorro_total,
        promo.desc_promocion,
        promo.cantidad_n,
        promo.cantidad_m,

        DATE(
            promo.fecha_inicio_de_promocion
        ) AS fecha_inicio_de_promocion,

        DATE(
            promo.fecha_fin_de_promocion
        ) AS fecha_fin_de_promocion,

        promo.porcentaje_cobertura,
        promo.porcentaje_descuento,
        promo.organizacion_ventas,
        promo.canal_distribucion

    FROM `cl-bigdata-analytics-preprod.CDA_VISTAS.VW_FACT_WORKFLOW` AS promo

    INNER JOIN productos_objetivo AS producto
        ON promo.ean = producto.ean

    WHERE promo.organizacion_ventas = '1000'
      AND promo.canal_distribucion = '10'
      AND promo.registro_valido = 'X'

      -- Incluye promociones iniciadas antes de 2024
      -- que continúan vigentes durante 2024.
      AND DATE(
          promo.fecha_fin_de_promocion
      ) >= DATE '2024-01-01'

      -- Excluir períodos promocionales inconsistentes.
      AND DATE(
          promo.fecha_fin_de_promocion
      ) >= DATE(
          promo.fecha_inicio_de_promocion
      )
),

promociones_diarias AS (
    -- 3. Expandir cada promoción a una fila por día de vigencia.
    SELECT
        promo.*,
        p_date
    FROM historial_promocional AS promo

    CROSS JOIN UNNEST(
        GENERATE_DATE_ARRAY(
            GREATEST(
                promo.fecha_inicio_de_promocion,
                DATE '2024-01-01'
            ),
            promo.fecha_fin_de_promocion
        )
    ) AS p_date
),

promociones_con_precio_valido AS (
    -- 4. Conservar solo precios promocionales válidos:
    -- no nulos, finitos y estrictamente mayores que cero.
    SELECT *
    FROM promociones_diarias
    WHERE precio_promocional IS NOT NULL
      AND NOT IS_NAN(precio_promocional)
      AND NOT IS_INF(precio_promocional)
      AND precio_promocional > 0
),

promociones_ordenadas AS (
    -- 5. Ordenar las promociones activas de cada EAN y día
    -- desde el menor precio promocional válido.
    SELECT
        *,

        ROW_NUMBER() OVER (
            PARTITION BY
                ean,
                p_date
            ORDER BY
                precio_promocional ASC,
                n_promocion ASC,
                material ASC,
                nombre_promocion ASC
        ) AS orden_precio_diario,

        COUNT(*) OVER (
            PARTITION BY
                ean,
                p_date
        ) AS cantidad_promociones_activas,

        LEAD(precio_promocional) OVER (
            PARTITION BY
                ean,
                p_date
            ORDER BY
                precio_promocional ASC,
                n_promocion ASC,
                material ASC,
                nombre_promocion ASC
        ) AS segundo_precio_promocional

    FROM promociones_con_precio_valido
),

precio_diario_calculado AS (
    -- 6. Calcular un único precio promocional por EAN y día.
    SELECT
        * EXCEPT (
            orden_precio_diario,
            segundo_precio_promocional
        ),

        1 AS FLAG_PROMO,

        CASE
            -- Caso general: utilizar directamente el menor
            -- precio promocional válido.
            WHEN desc_promocion != 'COMBINACION NX$$'
                 OR desc_promocion IS NULL
                THEN precio_promocional

            -- Tratamiento especial con más de una promoción activa:
            -- promedio de los dos precios promocionales más bajos.
            WHEN cantidad_promociones_activas > 1
                THEN (
                    precio_promocional
                    + segundo_precio_promocional
                ) / 2.0

            -- Tratamiento especial con una única promoción activa:
            -- promedio del precio promocional y el precio modal.
            WHEN precio_modal IS NOT NULL
                 AND NOT IS_NAN(precio_modal)
                 AND NOT IS_INF(precio_modal)
                 AND precio_modal > 0
                THEN (
                    precio_promocional
                    + precio_modal
                ) / 2.0

            -- No se puede calcular el precio cuando se necesita
            -- el precio modal y este no es válido.
            ELSE NULL
        END AS precio_promocional_minimo,

        desc_promocion
            AS desc_promocion_minimo,

        CASE
            WHEN desc_promocion != 'COMBINACION NX$$'
                 OR desc_promocion IS NULL
                THEN 'PRECIO_PROMOCIONAL_MINIMO_DIRECTO'

            WHEN cantidad_promociones_activas > 1
                THEN 'PROMEDIO_DOS_PRECIOS_PROMOCIONALES_MAS_BAJOS'

            WHEN precio_modal IS NOT NULL
                 AND NOT IS_NAN(precio_modal)
                 AND NOT IS_INF(precio_modal)
                 AND precio_modal > 0
                THEN 'PROMEDIO_PRECIO_PROMOCIONAL_Y_PRECIO_MODAL'

            ELSE 'SIN_PRECIO_DIARIO_VALIDO'
        END AS regla_calculo_precio_promocional

    FROM promociones_ordenadas

    -- La primera fila representa la promoción con el menor precio.
    WHERE orden_precio_diario = 1
)

-- 7. Conservar solamente precios diarios calculados válidos.
SELECT *
FROM precio_diario_calculado

WHERE precio_promocional_minimo IS NOT NULL
  AND NOT IS_NAN(precio_promocional_minimo)
  AND NOT IS_INF(precio_promocional_minimo)
  AND precio_promocional_minimo > 0

ORDER BY
    ean,
    p_date
"""
})


##########          0.2 Config

config.dictConfig(LOGGING_CONFIG)
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

usuario = 'pricing'


def main():

    #------- Inputs ---------#
    args = vars(parser.parse_args())
    execution_date: str = args['execution_date']
    proyecto: str = args['project_id']  # noqa: F841
    store_banner:str = args['store_banner']

    file_site = '/sites/BigDatayAdvancedAnalytics/Documentos compartidos/'
    file_site += 'Pricing/Forecast Promociones'
    secret_name = 'bdaa_sharepoint_credentials'  # noqa: S105#HC
    sp_cred = secretmanager.getSecret(secret_name, project=proyecto)  # noqa: F841

    esquema = 'TMP'
    tabla = 'TMP_REGRESSION_PROCESSED_DATA_FORECAST'
    path_table = f'{proyecto}.{esquema}.{tabla}'

    # Nota: local queda definido, en producción se inyecta desde Airflow
    gbq_client = Client()
    logging.info(f'execution_date: {execution_date}')
    logging.info(f'proyecto: {proyecto}')
    logging.info(f'store_banner: {store_banner}')


    logging.info('##### [P1] Datasets queries #####')

    query_forecast = QUERY_FORECAST['query_data_procesada'].substitute(
        path_table=path_table)
    df_hist_venta = readBigQuery(
                    query=query_forecast,
                    user=usuario,
                    gbq_client=gbq_client)

    logging.info('[1.1] Dimensiones df hist_venta: %s', df_hist_venta.shape)
    logging.info(f'[1.1] Memoria utilizada: {df_hist_venta.memory_usage(deep=True).sum() / 1024**2:.2f} MB')  # noqa: E501



    query_promos_basic = SQL_QUERIES_ALL_PROMOS['query_promos_forecasting'].substitute(
            path_table=path_table)
    df_promos_basic = readBigQuery(
        query=query_promos_basic,
        user=usuario,
        gbq_client=gbq_client)

    logging.info('[1.2] Dimensiones df promos basic: %s', df_promos_basic.shape)
    logging.info(f'[1.2] Memoria utilizada: {df_promos_basic.memory_usage(deep=True).sum() / 1024**2:.2f} MB')  # noqa: E501


    query_promos_tratada = SQL_QUERIES_ALL_PROMOS_COMBINACION['query_promos_forecasting_combinacion'].substitute(  # noqa: E501
            path_table=path_table)

    df_promos_tratada = readBigQuery(
        query=query_promos_tratada,
        user=usuario,
        gbq_client=gbq_client)

    logging.info('[1.3] Dimensiones df promos tratada: %s', df_promos_tratada.shape)
    logging.info(f'[1.3] Memoria utilizada: {df_promos_tratada.memory_usage(deep=True).sum() / 1024**2:.2f} MB')  # noqa: E501

if __name__ == '__main__':

    main()
