"""Resuelve la tabla store_id -> zona para Unimarc.

A diferencia de resolver_tiendas_activas_region.py, no resuelve
"tiendas activas" contra transacciones -- SEGMENTACION_ZONAS_BM_PRICING
ya viene curada, solo se cruza contra la dimensional de tiendas.
"""
from __future__ import annotations  # noqa: I001

import logging
import argparse
from logging import config

from google.cloud.bigquery import Client

from common.constants import LOGGING_CONFIG
from common.databases.queries import QueryDict
from common.gcp_extended.bigquery import readBigQuery, uploadFrame

config.dictConfig(LOGGING_CONFIG)

parser = argparse.ArgumentParser()
parser.add_argument('--project_id', type=str, help='GCP project')
parser.add_argument('--execution_date', type=str, help='DAG execution date')

# A diferencia de resolver_tiendas_activas_region.py, esta version NO
# necesita resolver "tiendas activas" contra transacciones reales --
# SEGMENTACION_ZONAS_BM_PRICING ya viene curada por el equipo que la
# entrega (confirmado). Solo hace falta el cruce con la dimensional de
# tiendas para obtener el STORE_ID en el mismo formato que usa el
# resto del pipeline (sin ceros a la izquierda), y quedarse solo con
# Unimarc (ORG_IP_ID='01') -- esta segmentacion es exclusiva de ese
# banner.
SQL_QUERIES = QueryDict({
'query_tiendas_zona':
"""
SELECT DISTINCT
    'Unimarc' AS store_banner,
    TIENDAS_PRICING.ZONA AS zona,
    LTRIM(DSH.STORE_ID, '0') AS store_id
FROM `${proyecto}.PRECIO_PROMOCIONES.SEGMENTACION_ZONAS_BM_PRICING` TIENDAS_PRICING
LEFT JOIN `cl-cda-prod.DS_CDA_VW_SMU.DW_VW_DIM_STORE_HIERARCHY` DSH
    ON LTRIM(DSH.STORE_ID, '0') = CAST(TIENDAS_PRICING.STORE_ID AS STRING)
WHERE DSH.ORG_IP_ID IN ('01')
ORDER BY store_id ASC
""",
})


def main() -> None:  # noqa: D103
    args = vars(parser.parse_args())
    execution_date: str = args['execution_date']
    proyecto: str = args['project_id']

    logging.info(f'execution_date: {execution_date}')

    gbq_client = Client()
    usuario = 'pricing'

    query = SQL_QUERIES['query_tiendas_zona'].substitute(proyecto=proyecto)
    df_tiendas_zona = readBigQuery(query=query, user=usuario, gbq_client=gbq_client)

    # Filtrar filas donde el cruce con la dimensional no encontro
    # coincidencia (DSH.STORE_ID nulo por el LEFT JOIN) -- no deberian
    # existir si la segmentacion viene curada, pero es una salvaguarda
    # barata antes de subir la tabla.
    n_antes = len(df_tiendas_zona)
    df_tiendas_zona = df_tiendas_zona[df_tiendas_zona['store_id'].notna()]
    n_sin_cruce = n_antes - len(df_tiendas_zona)
    if n_sin_cruce > 0:
        logging.warning(
            f'{n_sin_cruce} tiendas de SEGMENTACION_ZONAS_BM_PRICING no '
            'cruzaron con la dimensional (DSH.STORE_ID nulo) -- se excluyen.'
        )

    logging.info(f'Tiendas resueltas por zona: {len(df_tiendas_zona):,}')
    logging.info(
        df_tiendas_zona.groupby('zona')['store_id'].count().to_string()
    )

    tabla_destino = f'{proyecto}.PRECIO_PROMOCIONES.TMP_TIENDAS_ACTIVAS_POR_ZONA'

    # Mismo resguardo que resolver_tiendas_activas_region.py --
    # uploadFrame asigna nombres de columna por posicion, no por
    # nombre.
    columnas_schema_orden = ['store_banner', 'zona', 'store_id']
    mapa_columnas_actual = {c.lower(): c for c in df_tiendas_zona.columns}
    df_tiendas_zona = df_tiendas_zona[
        [mapa_columnas_actual[c] for c in columnas_schema_orden]
    ]

    uploadFrame(
        df_tiendas_zona,
        table_ddl_json_path='gbq_objects/ingest_tiendas_activas_por_zona.json',
        project=proyecto,
        gbq_client=gbq_client,
        if_exists='replace',  # siempre refleja SOLO la corrida actual, no acumula
    )
    logging.info(f'Tabla {tabla_destino} actualizada.')


if __name__ == '__main__':
    main()
