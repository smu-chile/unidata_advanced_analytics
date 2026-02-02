# Default
from __future__ import annotations

import os

# Pip
import logging
import argparse
from logging import config

import pandas as pd
import pendulum  # noqa: F401
from google.cloud import bigquery  # noqa: F401
from google.cloud.bigquery import Client

# Own
import common.gcp_extended.bigquery as gbq_extended  # noqa: F401
from common.constants import LOGGING_CONFIG
from common.databases.queries import QueryDict
from common.gcp_extended.bigquery import uploadFrame, readBigQuery


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

# -------------------------------------------------------------------------
#  SQL Queries
# -------------------------------------------------------------------------

SQL_QUERIES = QueryDict({
    'promotions_to_evaluate':
    """
    SELECT FORMATO,N_PROMOCION_PPAL,N_PROMOCION
    FROM `${gcp_project}.${schema}.TEMP_DIM_PROMOTIONS_TO_EVALUATE`
    WHERE UPPER(FORMATO) = '${upper_store_banner}'
    """
})

WORKFLOW_QUERIES = QueryDict({
    'Unimarc':
    """
    SELECT DISTINCT
    N_PROMOCION,
    NOMBRE_PROMOCION,
    DESCRIPCION_MECANICA,
    CASE
        WHEN
            DESCRIPCION_EVENTO_PROMOCIONAL = 'UNI VENTA ESPECIAL'
            AND LOWER(NOMBRE_PROMOCION) LIKE '%fresquito%'
        THEN 'FRESQUITOS'
        WHEN DESCRIPCION_EVENTO_PROMOCIONAL = 'UNI APOTEOSICO' THEN 'APOTEOSICO'
        WHEN DESCRIPCION_EVENTO_PROMOCIONAL = 'UNI CATALOGO' THEN 'CATALOGO'
        ELSE 'UNI VENTA ESPECIAL'
    END AS TIPO_PROMOCION,
    DESCRIPCION_EVENTO_PROMOCIONAL,
    CAST(FECHA_INICIO_DE_PROMOCION AS STRING) AS FECHA_INICIO_DE_PROMOCION,
    CAST(FECHA_FIN_DE_PROMOCION AS STRING) AS FECHA_FIN_DE_PROMOCION
    FROM `${gcp_project}.${schema}.VW_FACT_WORKFLOW`
    WHERE n_promocion IN (${promotions_ids})
    """,

    'Mayorista':
    """
    SELECT DISTINCT
    N_PROMOCION,
    NOMBRE_PROMOCION,
    DESCRIPCION_MECANICA,
    CASE
        WHEN DESCRIPCION_EVENTO_PROMOCIONAL = 'M10 CICLO' THEN 'M10 CICLO'
        WHEN DESCRIPCION_EVENTO_PROMOCIONAL = 'M10 APOTEOSICO' THEN 'APOTEOSICO M10'
        WHEN DESCRIPCION_EVENTO_PROMOCIONAL = 'M10 10 DE M10' THEN 'M10 10 DE M10'
        ELSE 'M10 VENTA ESPECIAL'
    END AS TIPO_PROMOCION,
    DESCRIPCION_EVENTO_PROMOCIONAL,
    CAST(FECHA_INICIO_DE_PROMOCION AS STRING) AS FECHA_INICIO_DE_PROMOCION,
    CAST(FECHA_FIN_DE_PROMOCION AS STRING) AS FECHA_FIN_DE_PROMOCION
    FROM `${gcp_project}.${schema}.VW_FACT_WORKFLOW`
    WHERE n_promocion IN (${promotions_ids})
    """
})

# -------------------------------------------------------------------------
# Main function
# -------------------------------------------------------------------------

def main():
    usuario = 'halo_efect'  # noqa: F841
    # parse input variables
    args = vars(parser.parse_args())
    execution_date: str = args['execution_date']
    proyecto: str = args['project_id']
    store_banner: str = args['store_banner']
    logging.info(f'execution_date: {execution_date}')
    logging.info(f'store_banner: {store_banner}')

    gbq_client = Client()

    upper_store_banner = {
    'Alvi': 'ALVI',
    'Unimarc': 'UNIMARC',
    'Super 10': 'S10',
    'Mayorista': 'M10'
    }[store_banner]

    logging.info(f'upper_store_banner: {upper_store_banner}')

    promotions_to_evaluate = readBigQuery(SQL_QUERIES['promotions_to_evaluate'].substitute(
    gcp_project = proyecto,
    schema = 'TMP',
    upper_store_banner = upper_store_banner
    ),
    user = usuario,
    gbq_client = gbq_client
    )

    workflow_data = readBigQuery(WORKFLOW_QUERIES[store_banner].substitute(
    gcp_project = proyecto,
    schema = 'CDA_VISTAS',
    promotions_ids = ','.join(promotions_to_evaluate['N_PROMOCION'].astype('str').to_list())
    ),
    user = 'abravom',
    gbq_client = gbq_client
    )

    if list(
    set(promotions_to_evaluate['N_PROMOCION'].to_list()).difference(
        set(workflow_data['N_PROMOCION'].to_list())
    )
    ):
        missing_promotions = list(
        set(promotions_to_evaluate['N_PROMOCION'].to_list()).difference(
            set(workflow_data['N_PROMOCION'].to_list())
            )
        )
        logging.info(f'The following promotions are missing: {list(missing_promotions)}')

        promotions_to_evaluate = promotions_to_evaluate[
        ~promotions_to_evaluate['N_PROMOCION'].isin(missing_promotions)
        ]

        promotions_to_evaluate = promotions_to_evaluate.iloc[:, ::-1].replace(
        dict.fromkeys(missing_promotions, pd.NA)
        ).fillna(
            method='ffill', axis=1
        ).iloc[:, ::-1]

    master_promotion_table = promotions_to_evaluate.merge(
    workflow_data[[
        'N_PROMOCION',
        'NOMBRE_PROMOCION',
        'FECHA_INICIO_DE_PROMOCION',
        'FECHA_FIN_DE_PROMOCION',
        'TIPO_PROMOCION',
        'DESCRIPCION_EVENTO_PROMOCIONAL',
        'DESCRIPCION_MECANICA'
    ]].rename(columns={'N_PROMOCION': 'N_PROMOCION_PPAL'}),
    on='N_PROMOCION_PPAL',
    how='inner',
    ).rename(columns={
        'NOMBRE_PROMOCION': 'NOMBRE_PROMOCION_PPAL',
        'FECHA_INICIO_DE_PROMOCION': 'FECHA_INICIO_DE_PROMOCION_PPAL',
        'FECHA_FIN_DE_PROMOCION': 'FECHA_FIN_DE_PROMOCION_PPAL'
    # Add child promotion data
    }).merge(
        workflow_data[[
            'N_PROMOCION',
            'NOMBRE_PROMOCION',
            'FECHA_INICIO_DE_PROMOCION',
            'FECHA_FIN_DE_PROMOCION'
        ]],
        on='N_PROMOCION',
        how='inner',
    )
    master_promotion_table['MES_PROMOCION'] = (
        master_promotion_table['FECHA_FIN_DE_PROMOCION_PPAL'].str[:-2]
        + '01'
    )

    columns = ['N_PROMOCION',
        'TIPO_PROMOCION',
        'NOMBRE_PROMOCION_PPAL',
        'FECHA_INICIO_DE_PROMOCION_PPAL',
        'FECHA_FIN_DE_PROMOCION_PPAL',
        'MES_PROMOCION',
        'DESCRIPCION_MECANICA',
        'DESCRIPCION_EVENTO_PROMOCIONAL',
        'NOMBRE_PROMOCION',
        'FECHA_INICIO_DE_PROMOCION',
        'FECHA_FIN_DE_PROMOCION']

    if store_banner == 'Unimarc':
        uploadFrame(
        master_promotion_table[columns],
        table_ddl_json_path=os.path.join('gbq_objects','master_promotion_unimarc.json'),
        project=proyecto,
        gbq_client=gbq_client,
        if_exists='replace'
        )
    elif store_banner == 'Mayorista':
        uploadFrame(
        master_promotion_table[columns],
        table_ddl_json_path=os.path.join('gbq_objects','master_promotion_mayorista.json'),
        project=proyecto,
        gbq_client=gbq_client,
        if_exists='replace'
        )

if __name__ == '__main__':
    main()
