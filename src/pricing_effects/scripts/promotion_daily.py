# Default
from __future__ import annotations

import logging
import argparse
from logging import config

# Pip
import pendulum
from google.cloud.bigquery import Client, DatasetReference

# Own
from common.constants import LOGGING_CONFIG
from common.databases.queries import QueryDict


# -------------------------------------------------------------------------
#  Config
# -------------------------------------------------------------------------
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
# Nota: este script NO recibe --store_banner -- construye la tabla para
# TODOS los banners en una sola corrida (es un CREATE OR REPLACE TABLE).


# -------------------------------------------------------------------------
#  SQL Queries
# -------------------------------------------------------------------------
SQL_QUERIES = QueryDict({  # Region: Explicacion de query

    # Construye/reemplaza TMP_PROMOTION_DAILY completa, para los 6
    # banners (fisicos + las 2 variantes de e-commerce), incluyendo la
    # mecanica ESCALAS con eleccion empirica de tramo de CANTIDAD_N
    # (cascada SKU -> banner).
    'query_promotion_daily':
    """
    CREATE OR REPLACE TABLE `${proyecto}.${dataset}.TMP_PROMOTION_DAILY` AS
    (
    WITH

    -- =====================================================================
    -- 1) PROMOCIONES BASE (WORKFLOW) -- multi-banner, Escalas incluida
    -- =====================================================================
    promociones_base AS (
        SELECT DISTINCT
            W.MATERIAL AS material,
            W.DESC_MATERIAL AS desc_material,
            CASE
                WHEN W.ORGANIZACION_VENTAS = '1000' AND W.CANAL_DISTRIBUCION = '70' THEN 'Ecommerce Unimarc'
                WHEN W.ORGANIZACION_VENTAS = '7500' AND W.CANAL_DISTRIBUCION = '70' THEN 'Ecommerce Alvi'
                WHEN W.ORGANIZACION_VENTAS = '1000' AND W.CANAL_DISTRIBUCION = '10' THEN 'Unimarc'
                WHEN W.ORGANIZACION_VENTAS = '3000' AND W.CANAL_DISTRIBUCION = '10' THEN 'Super 10'
                WHEN W.ORGANIZACION_VENTAS = '3500' AND W.CANAL_DISTRIBUCION = '10' THEN 'Mayorista 10'
                WHEN W.ORGANIZACION_VENTAS = '7500' AND W.CANAL_DISTRIBUCION = '10' THEN 'Alvi'
                ELSE 'NO APLICA'
            END AS store_banner,
            W.DESC_PROMOCION AS desc_promocion,
            W.CANTIDAD_N AS cantidad_n,
            W.N_PROMOCION AS n_promocion,
            W.FECHA_INICIO_DE_PROMOCION AS fecha_inicio_de_promocion,
            W.FECHA_FIN_DE_PROMOCION AS fecha_fin_de_promocion,
            W.PRECIO_MODAL AS precio_modal,
            W.PRECIO_PROMOCIONAL AS precio_promocional,
            ROUND(SAFE_DIVIDE(W.PRECIO_MODAL - W.PRECIO_PROMOCIONAL, W.PRECIO_MODAL) * 100, 2) AS descuento_efectivo,
            CASE
                WHEN W.DESC_PROMOCION IN ('PRECIO FIJO', '% DE DESCUENTO', 'COMBINACION NX$$', 'ESCALAS') THEN
                    CASE
                        WHEN SAFE_DIVIDE(W.PRECIO_MODAL - W.PRECIO_PROMOCIONAL, W.PRECIO_MODAL) * 100 < 10 THEN '0_10'
                        WHEN SAFE_DIVIDE(W.PRECIO_MODAL - W.PRECIO_PROMOCIONAL, W.PRECIO_MODAL) * 100 < 15 THEN '10_15'
                        WHEN SAFE_DIVIDE(W.PRECIO_MODAL - W.PRECIO_PROMOCIONAL, W.PRECIO_MODAL) * 100 < 20 THEN '15_20'
                        WHEN SAFE_DIVIDE(W.PRECIO_MODAL - W.PRECIO_PROMOCIONAL, W.PRECIO_MODAL) * 100 < 25 THEN '20_25'
                        WHEN SAFE_DIVIDE(W.PRECIO_MODAL - W.PRECIO_PROMOCIONAL, W.PRECIO_MODAL) * 100 < 30 THEN '25_30'
                        WHEN SAFE_DIVIDE(W.PRECIO_MODAL - W.PRECIO_PROMOCIONAL, W.PRECIO_MODAL) * 100 < 40 THEN '30_40'
                        ELSE '40_mas'
                    END
            END AS atributo_promocion

        FROM `cl-cda-prod.DS_CDA_VW_SMU.DW_VW_FACT_WORKFLOW` W

        WHERE
            REGEXP_CONTAINS(
                UPPER(W.DESCRIPCION_EVENTO_PROMOCIONAL),
                r'CATALOGO|CICLO|VENTA ESPECIAL|APOTEOSICO|OFERTA QUINCENAL|MASIVAS'
            )
            AND W.DESC_PROMOCION IN ('PRECIO FIJO', '% DE DESCUENTO', 'COMBINACION NX$$', 'ESCALAS')
            AND SAFE_DIVIDE(W.PRECIO_MODAL - W.PRECIO_PROMOCIONAL, W.PRECIO_MODAL) * 100 <= 65
            AND CANAL_DISTRIBUCION IN ('10', '70')
            AND FECHA_FIN_DE_PROMOCION >= '${fecha_inicial}'
            AND FECHA_INICIO_DE_PROMOCION <= '${fecha_final}'
    ),

    -- =====================================================================
    -- 2) UNIDADES POR TRANSACCION REAL (para decidir el tramo de Escalas)
    -- =====================================================================
    unidades_por_transaccion AS (
        SELECT
            CAST(LTRIM(DPH.SKU_PRODUCT, '0') AS INT64) AS material,
            CASE
                WHEN DSH.ORG_IP_ID = '01' AND EC.MARKET_BASKET_KEY IS NULL THEN 'Unimarc'
                WHEN DSH.ORG_IP_ID = '08' AND EC.MARKET_BASKET_KEY IS NULL THEN 'Alvi'
                WHEN DSH.ORG_IP_ID = '01' AND EC.MARKET_BASKET_KEY IS NOT NULL THEN 'Ecommerce Unimarc'
                WHEN DSH.ORG_IP_ID = '08' AND EC.MARKET_BASKET_KEY IS NOT NULL THEN 'Ecommerce Alvi'
                WHEN DSH.ORG_IP_ID = '09' THEN 'Super 10'
                WHEN DSH.ORG_IP_ID = '02' THEN 'Mayorista 10'
                ELSE 'NO APLICA'
            END AS store_banner,
            FIT.TXN_KEY AS txn_key,
            CAST(SUM(
                CASE WHEN FIT.NBR_PD_ITM = 0 THEN (COALESCE(CAST(FIT.WGHT_ITM AS FLOAT64), 1) / 1000)
                    ELSE (COALESCE(CAST(FIT.NBR_PD_ITM AS FLOAT64), 1) * COALESCE(CAST(DP.CONT_CONV_UMB AS FLOAT64), 1))
                END
            ) AS BIGINT) AS unidades_esta_transaccion

        FROM (
            SELECT ITM_TXN_AMT, TAX_AMOUNT, TXN_KEY, CUSTOMER_KEY, CUSTOMER_HEX, NBR_PD_ITM,
                WGHT_ITM, STORE_KEY, PRODUCT_KEY_1, DATE_KEY, MARKET_BASKET_KEY,
                ITM_TXN_FCN_TP_DSC, FNC_DOC_TP_HEX
            FROM `cl-cda-prod.DS_CDA_VW_SMU.DW_VW_FACT_ITM_TXN`
            WHERE DATE_TRUNC(ITM_TXN_TMS, MONTH) >= '${fecha_inicial}'
        ) FIT

        JOIN `cl-cda-prod.DS_CDA_VW_SMU.DW_VW_DIM_STORE_HIERARCHY` DSH ON DSH.STORE_KEY = FIT.STORE_KEY
        JOIN `cl-cda-prod.DS_CDA_VW_SMU.DW_VW_DIM_PRODUCT_HIERARCHY` DPH ON DPH.PRODUCT_KEY = FIT.PRODUCT_KEY_1
        JOIN `cl-cda-prod.DS_CDA_VW_SMU.DW_VW_DIM_PRODUCT` DP ON DP.PRODUCT_KEY = FIT.PRODUCT_KEY_1
        LEFT JOIN `cl-cda-prod.DS_CDA_VW_SMU.DW_VW_FACT_MARKET_BASKET_E_COMMERCE` EC
            ON EC.MARKET_BASKET_KEY = FIT.MARKET_BASKET_KEY AND EC.CANAL_VENTA = 'E-COMMERCE'

        WHERE
            FIT.ITM_TXN_FCN_TP_DSC = 'V'
            AND FIT.FNC_DOC_TP_HEX IN (
                '5756ebdc189492f0ad8e05e633217018', '3ad6ff06d7bc49ae6f05b15354c3af0a',
                'a2f3a5cc2e8b1292bc6629beac500720', '4a209440364b13aa8cd293a37cee6ee1',
                '2fae0e1971b412541215bec30dcedf01', 'cfe71cea05fb5fa5cb5b5f2a72d616af',
                'e784c5e99b4e72f9e4d85a3f244246a9', 'b7ff659d1213e5fe6a36d081943123a2'
            )
            AND DPH.NEG_ID NOT IN ('14', '15')
            AND DSH.STORE_ID NOT IN ('0622')

        GROUP BY 1, 2, 3
    ),

    -- =====================================================================
    -- 3) TRAMOS DE CANTIDAD_N CONFIGURADOS PARA CADA SKU EN ESCALAS
    -- =====================================================================
    tramos_escalas_por_material AS (
        SELECT DISTINCT material, store_banner, cantidad_n
        FROM promociones_base
        WHERE desc_promocion = 'ESCALAS' AND cantidad_n IS NOT NULL AND cantidad_n > 0
    ),

    -- =====================================================================
    -- 4) CLASIFICAR CADA TRANSACCION REAL EN EL TRAMO MAS CERCANO (por abajo)
    -- =====================================================================
    transacciones_clasificadas AS (
        SELECT
            U.material,
            U.store_banner,
            U.txn_key,
            (
                SELECT MAX(T.cantidad_n)
                FROM tramos_escalas_por_material T
                WHERE T.material = U.material AND T.store_banner = U.store_banner
                    AND T.cantidad_n <= U.unidades_esta_transaccion
            ) AS tramo_comprado
        FROM unidades_por_transaccion U
        WHERE EXISTS (
            SELECT 1 FROM tramos_escalas_por_material T
            WHERE T.material = U.material AND T.store_banner = U.store_banner
        )
    ),

    -- =====================================================================
    -- 5) TRAMO REPRESENTATIVO -- por SKU (evidencia propia)
    -- =====================================================================
    frecuencia_tramo_sku AS (
        SELECT material, store_banner, tramo_comprado, COUNT(*) AS n_transacciones
        FROM transacciones_clasificadas
        WHERE tramo_comprado IS NOT NULL
        GROUP BY 1, 2, 3
    ),

    tramo_representativo_sku AS (
        SELECT material, store_banner, tramo_comprado AS cantidad_n_representativa
        FROM frecuencia_tramo_sku
        QUALIFY ROW_NUMBER() OVER (PARTITION BY material, store_banner ORDER BY n_transacciones DESC) = 1
    ),

    -- =====================================================================
    -- 6) TRAMO REPRESENTATIVO -- a nivel BANNER (respaldo, cascada)
    -- =====================================================================
    frecuencia_tramo_banner AS (
        SELECT store_banner, tramo_comprado, COUNT(*) AS n_transacciones
        FROM transacciones_clasificadas
        WHERE tramo_comprado IS NOT NULL
        GROUP BY 1, 2
    ),

    tramo_representativo_banner AS (
        SELECT store_banner, tramo_comprado AS cantidad_n_representativa
        FROM frecuencia_tramo_banner
        QUALIFY ROW_NUMBER() OVER (PARTITION BY store_banner ORDER BY n_transacciones DESC) = 1
    ),

    -- =====================================================================
    -- 7) TRAMO FINAL ELEGIDO POR SKU (propio si existe, si no hereda del banner)
    -- =====================================================================
    tramo_final_por_sku AS (
        SELECT
            P.material,
            P.store_banner,
            COALESCE(S.cantidad_n_representativa, B.cantidad_n_representativa) AS cantidad_n_elegida
        FROM (SELECT DISTINCT material, store_banner FROM promociones_base WHERE desc_promocion = 'ESCALAS') P
        LEFT JOIN tramo_representativo_sku S ON S.material = P.material AND S.store_banner = P.store_banner
        LEFT JOIN tramo_representativo_banner B ON B.store_banner = P.store_banner
    ),

    -- =====================================================================
    -- 8) PROMOCIONES FINALES -- Escalas usa el tramo elegido; el resto, igual que antes
    -- =====================================================================
    promociones_no_escalas AS (
        SELECT * FROM promociones_base WHERE desc_promocion != 'ESCALAS'
    ),

    promociones_escalas_final AS (
        SELECT PB.*
        FROM promociones_base PB
        JOIN tramo_final_por_sku TF ON TF.material = PB.material AND TF.store_banner = PB.store_banner
        WHERE PB.desc_promocion = 'ESCALAS' AND PB.cantidad_n = TF.cantidad_n_elegida
    ),

    promociones AS (
        SELECT * FROM promociones_no_escalas
        UNION ALL
        SELECT * FROM promociones_escalas_final
    ),

    -- =====================================================================
    -- 9) EXPANSION A NIVEL DIA
    -- =====================================================================
    promotion_daily AS (
        SELECT
            material, desc_material, store_banner, desc_promocion, atributo_promocion,
            descuento_efectivo, precio_promocional, p_date,
            DATE_DIFF(fecha_fin_de_promocion, fecha_inicio_de_promocion, DAY) + 1 AS duracion,
            DATE_DIFF(p_date, fecha_inicio_de_promocion, DAY) + 1 AS dia_promocion,
            SAFE_DIVIDE(
                DATE_DIFF(p_date, fecha_inicio_de_promocion, DAY) + 1,
                DATE_DIFF(fecha_fin_de_promocion, fecha_inicio_de_promocion, DAY) + 1
            ) AS progreso_promocion
        FROM promociones,
        UNNEST(GENERATE_DATE_ARRAY(fecha_inicio_de_promocion, fecha_fin_de_promocion)) p_date
    ),

    promotion_daily_unique AS (
        SELECT *
        FROM promotion_daily
        QUALIFY ROW_NUMBER() OVER(
            PARTITION BY store_banner, material, p_date
            ORDER BY descuento_efectivo DESC, precio_promocional ASC
        ) = 1
    ),

    promotion_features AS (
        SELECT
            A.material, A.desc_material, A.store_banner, A.desc_promocion, A.p_date,
            A.atributo_promocion, A.duracion, A.dia_promocion, A.progreso_promocion,
            COUNT(B.p_date) AS frecuencia_promocional_90d
        FROM promotion_daily_unique A
        LEFT JOIN promotion_daily_unique B
            ON A.material = B.material AND A.store_banner = B.store_banner
            AND B.p_date BETWEEN DATE_SUB(A.p_date, INTERVAL 89 DAY) AND A.p_date
        GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9
    )

    -- =====================================================================
    -- RESULTADO FINAL
    -- =====================================================================
    SELECT
        material, desc_material, store_banner, desc_promocion, p_date,
        atributo_promocion, duracion, dia_promocion, progreso_promocion, frecuencia_promocional_90d
    FROM promotion_features
    WHERE
        p_date BETWEEN '${fecha_inicial}' AND '${fecha_final}'
        AND store_banner IN UNNEST(${banners_lista})
    ORDER BY store_banner, material, p_date
    )
    """,  # noqa: E501
})


def main() -> None:  # noqa: D103

    # Parse input variables
    args = vars(parser.parse_args())
    execution_date: str = args['execution_date']
    proyecto: str = args['project_id']
    logging.info(f'execution_date: {execution_date}')
    logging.info(f'proyecto: {proyecto}')

    # Set gbq client for all subsequent queries
    gbq_client = Client()

    # REGION: Parametros iniciales
    # -------------------------------------------------------------------
    dataset = 'PRECIO_PROMOCIONES'
    cant_meses = 29  # misma ventana que TMP_REGRESSION_PROCESSED_DATA_ELASTICITY

    banners_a_incluir = [
        'Unimarc', 'Super 10', 'Mayorista 10', 'Alvi',
        'Ecommerce Unimarc', 'Ecommerce Alvi',
    ]

    # ENDREGION

    # REGION: Ventana de fechas -- SIEMPRE relativa a execution_date,
    #  NUNCA fija
    # -------------------------------------------------------------------
    fecha_ejecucion = pendulum.parse(execution_date)
    fecha_final = fecha_ejecucion.start_of('month').subtract(days=1)
    fecha_inicial = fecha_final.subtract(months=cant_meses).add(months=1).start_of('month')

    fecha_inicial_str = fecha_inicial.format('YYYY-MM-DD')
    fecha_final_str = fecha_final.format('YYYY-MM-DD')

    logging.info(f'Ventana de fechas: {fecha_inicial_str} a {fecha_final_str}')
    # ENDREGION

    # REGION: Asegurar que el dataset de destino exista
    # -------------------------------------------------------------------
    dataset_ref = DatasetReference(proyecto, dataset)
    gbq_client.create_dataset(dataset_ref, exists_ok=True)
    logging.info(f'Dataset verificado/creado: {proyecto}.{dataset}')
    # ENDREGION

    # REGION: Construir y ejecutar la query (CREATE OR REPLACE TABLE)
    # -------------------------------------------------------------------
    banners_sql = '[' + ', '.join(f"'{b}'" for b in banners_a_incluir) + ']'

    query_promotion_daily = SQL_QUERIES['query_promotion_daily'].substitute(
        proyecto=proyecto,
        dataset=dataset,
        fecha_inicial=fecha_inicial_str,
        fecha_final=fecha_final_str,
        banners_lista=banners_sql,
    )

    logging.info('Inicia la construccion de TMP_PROMOTION_DAILY ...')
    gbq_client.query(query_promotion_daily).result()
    logging.info(f'Tabla creada/reemplazada: {proyecto}.{dataset}.TMP_PROMOTION_DAILY')
    # ENDREGION


if __name__ == '__main__':
    main()
