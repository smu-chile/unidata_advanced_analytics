from __future__ import annotations  # noqa: I001

import logging
import argparse
from logging import config

import pendulum
from google.cloud.bigquery import Client

from common.constants import LOGGING_CONFIG
from common.databases.queries import QueryDict
from common.gcp_extended.bigquery import readBigQuery, uploadFrame

config.dictConfig(LOGGING_CONFIG)

parser = argparse.ArgumentParser()
parser.add_argument('--project_id', type=str, help='GCP project')
parser.add_argument('--execution_date', type=str, help='DAG execution date')

SQL_QUERIES = QueryDict({
'query_tiendas_activas':
"""
SELECT DISTINCT
    CASE
        WHEN DSH.ORG_IP_ID = '01' THEN 'Unimarc'
        WHEN DSH.ORG_IP_ID = '08' THEN 'Alvi'
        WHEN DSH.ORG_IP_ID = '09' THEN 'Super 10'
        WHEN DSH.ORG_IP_ID = '02' THEN 'Mayorista 10'
        ELSE 'NO APLICA'
    END AS store_banner,
    CASE
        WHEN DSH.STE_ID = '01' THEN 'I Tarapacá'
        WHEN DSH.STE_ID = '02' THEN 'II Antofagasta'
        WHEN DSH.STE_ID = '03' THEN 'III Atacama'
        WHEN DSH.STE_ID = '04' THEN 'IV Región de Coquimbo'
        WHEN DSH.STE_ID = '05' THEN 'V Región de Valparaíso'
        WHEN DSH.STE_ID = '06' THEN 'VI Región de OHiggins'
        WHEN DSH.STE_ID = '07' THEN 'VII Región del Maule'
        WHEN DSH.STE_ID = '08' THEN 'VIII Región del Bío-Bío'
        WHEN DSH.STE_ID = '09' THEN 'IX Región de la Araucanía'
        WHEN DSH.STE_ID = '10' THEN 'X Región de Los Lagos'
        WHEN DSH.STE_ID = '11' THEN 'XI Región de Aysén'
        WHEN DSH.STE_ID = '12' THEN 'XII Magallanes'
        WHEN DSH.STE_ID = '13' THEN 'XIII Región Metropolitana'
        WHEN DSH.STE_ID = '14' THEN 'XIV Región de Los Ríos'
        WHEN DSH.STE_ID = '15' THEN 'XV Arica y Parinacota'
        WHEN DSH.STE_ID = '16' THEN 'XVI Región de Ñuble'
    END AS region,
    CASE
        WHEN DSH.STE_ID IN ('01', '15', '02') THEN 'Norte Grande'
        WHEN DSH.STE_ID IN ('03', '04') THEN 'Norte Chico'
        WHEN DSH.STE_ID IN ('05', '06', '07') THEN 'Central'
        WHEN DSH.STE_ID = '13' THEN 'Metropolitana'
        WHEN DSH.STE_ID IN ('09', '08', '14', '10', '16') THEN 'Sur'
        WHEN DSH.STE_ID IN ('11', '12') THEN 'Austral'
    END AS zona,
    -- Sector solo aplica dentro de la Region Metropolitana (STE_ID=13)
    -- -- fuera de ahi, siempre NULL (se agrupa por region o zona).
    CASE
        WHEN DSH.STE_ID != '13' THEN NULL
        WHEN DSH.COUNTY_DESC LIKE '%SANTIAGO%' THEN 'Centro'
        WHEN DSH.COUNTY_DESC LIKE '%PROVIDENCIA%'
            OR DSH.COUNTY_DESC LIKE '%ÑUÑOA%'
            OR DSH.COUNTY_DESC LIKE '%LA REINA%'
            OR DSH.COUNTY_DESC LIKE '%VITACURA%'
            OR DSH.COUNTY_DESC LIKE '%LO BARNECHEA%'
            OR DSH.COUNTY_DESC LIKE '%LAS CONDES%'
            OR DSH.COUNTY_DESC LIKE '%MACUL%' THEN 'Oriente'
        WHEN DSH.COUNTY_DESC LIKE '%QUINTA NORMAL%'
            OR DSH.COUNTY_DESC LIKE '%LO PRADO%'
            OR DSH.COUNTY_DESC LIKE '%PUDAHUEL%'
            OR DSH.COUNTY_DESC LIKE '%CERRO NAVIA%'
            OR DSH.COUNTY_DESC LIKE '%RENCA%'
            OR DSH.COUNTY_DESC LIKE '%QUILICURA%'
            OR DSH.COUNTY_DESC LIKE '%CONCHALI%'
            OR DSH.COUNTY_DESC LIKE '%HUECHURABA%'
            OR DSH.COUNTY_DESC LIKE '%INDEPENDENCIA%'
            OR DSH.COUNTY_DESC LIKE '%RECOLETA%' THEN 'Norte'
        WHEN DSH.COUNTY_DESC LIKE '%EL BOSQUE%'
            OR DSH.COUNTY_DESC LIKE '%SAN BERNARDO%'
            OR DSH.COUNTY_DESC LIKE '%LA PINTANA%'
            OR DSH.COUNTY_DESC LIKE '%SAN RAMON%'
            OR DSH.COUNTY_DESC LIKE '%SAN MIGUEL%'
            OR DSH.COUNTY_DESC LIKE '%LA CISTERNA%' THEN 'Sur'
        WHEN DSH.COUNTY_DESC LIKE '%SAN JOAQUIN%'
            OR DSH.COUNTY_DESC LIKE '%LA GRANJA%'
            OR DSH.COUNTY_DESC LIKE '%PEÑALOLEN%'
            OR DSH.COUNTY_DESC LIKE '%LA FLORIDA%'
            OR DSH.COUNTY_DESC LIKE '%PUENTE ALTO%' THEN 'Sur Oriente'
        WHEN DSH.COUNTY_DESC LIKE '%MAIPU%'
            OR DSH.COUNTY_DESC LIKE '%PEDRO AGUIRRE CERDA%'
            OR DSH.COUNTY_DESC LIKE '%LO ESPEJO%'
            OR DSH.COUNTY_DESC LIKE '%ESTACION CENTRAL%'
            OR DSH.COUNTY_DESC LIKE '%CERRILLOS%' THEN 'Poniente'
    END AS sector,
    LTRIM(DSH.STORE_ID,'0') AS store_id
FROM (
    SELECT PRODUCT_KEY_1, STORE_KEY, DATE_KEY, ITM_TXN_FCN_TP_DSC,
        FNC_DOC_TP_HEX, CUSTOMER_KEY
    FROM `cl-cda-prod.DS_CDA_VW_SMU.DW_VW_FACT_ITM_TXN`
    WHERE ITM_TXN_TMS >= '${fecha_inicial}'
) FIT
JOIN `cl-cda-prod.DS_CDA_VW_SMU.DW_VW_DIM_PRODUCT_HIERARCHY` DPH
    ON DPH.PRODUCT_KEY = FIT.PRODUCT_KEY_1
JOIN `cl-cda-prod.DS_CDA_VW_SMU.DW_VW_DIM_DATE` DD
    ON DD.DATE_KEY = FIT.DATE_KEY
JOIN `cl-cda-prod.DS_CDA_VW_SMU.DW_VW_DIM_STORE_HIERARCHY` DSH
    ON DSH.STORE_KEY = FIT.STORE_KEY
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
    AND FIT.CUSTOMER_KEY <> MD5('CST^CL^-1')
    AND DD.CALENDAR_YEAR * 100 + DD.CALENDAR_MONTH_NUMBER
        BETWEEN ${p_month_inicial} AND ${p_month_final}
""",
})


def main() -> None:  # noqa: D103
    args = vars(parser.parse_args())
    execution_date: str = args['execution_date']
    proyecto: str = args['project_id']

    logging.info(f'execution_date: {execution_date}')

    # Misma ventana que processed_regression_data_region.py, para que
    # ambos scripts esten de acuerdo en que significa "el periodo".
    cant_meses = 29
    fecha_ejecucion = pendulum.parse(execution_date)
    fecha_final = fecha_ejecucion.start_of('month').subtract(days=1)
    fecha_inicial = fecha_final.subtract(months=cant_meses).add(months=1).start_of('month')

    logging.info(f'Fecha inicial: {fecha_inicial}')
    logging.info(f'Fecha final: {fecha_final}')

    gbq_client = Client()
    usuario = 'pricing'

    query = SQL_QUERIES['query_tiendas_activas'].substitute(
        fecha_inicial=fecha_inicial.to_date_string(),
        p_month_inicial=fecha_inicial.year * 100 + fecha_inicial.month,
        p_month_final=fecha_final.year * 100 + fecha_final.month,
    )

    df_tiendas_activas = readBigQuery(query=query, user=usuario, gbq_client=gbq_client)
    df_tiendas_activas = df_tiendas_activas[df_tiendas_activas['store_banner'] != 'NO APLICA']
    df_tiendas_activas = df_tiendas_activas[df_tiendas_activas['region'].notna()]
    # 'sector' queda NULL fuera de la Metropolitana -- es esperado

    logging.info(f'Tiendas activas resueltas: {len(df_tiendas_activas):,}')
    logging.info(
        df_tiendas_activas.groupby(['store_banner', 'region'])['store_id']
        .count().to_string()
    )
    logging.info('Desglose de Metropolitana por sector:')
    logging.info(
        df_tiendas_activas[df_tiendas_activas['region'] == 'XIII Región Metropolitana']
        .groupby(['store_banner', 'sector'])['store_id'].count().to_string()
    )

    tabla_destino = f'{proyecto}.TMP.TMP_TIENDAS_ACTIVAS_POR_REGION'

    # Mismo resguardo que en processed_regression_data_region.py --
    # uploadFrame asigna nombres de columna por posicion, no por
    # nombre. Se fuerza el orden explicito para evitar el mismo bug.
    columnas_schema_orden = ['store_banner', 'region', 'zona', 'sector', 'store_id']
    mapa_columnas_actual = {c.lower(): c for c in df_tiendas_activas.columns}
    df_tiendas_activas = df_tiendas_activas[
        [mapa_columnas_actual[c] for c in columnas_schema_orden]
    ]

    uploadFrame(
        df_tiendas_activas,
        table_ddl_json_path='gbq_objects/ingest_tiendas_activas_por_region.json',
        project=proyecto,
        gbq_client=gbq_client,
        if_exists='replace',  # siempre refleja SOLO la corrida actual, no acumula
    )
    logging.info(f'Tabla {tabla_destino} actualizada.')


if __name__ == '__main__':
    main()
