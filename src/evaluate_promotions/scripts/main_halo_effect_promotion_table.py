# Default
import os
import logging
import argparse
from logging import config

# pip
import pandas as pd
from google.cloud.bigquery import Client

# Own
import common.gcp_extended.bigquery as gbq_extended
from common.constants import LOGGING_CONFIG
from common.databases.queries import QueryDict
from common.gcp_extended.secretsmanager import getSecret
from common.office365_extended.sharepoint import SharePointFile


# -------------------------------------------------------------------------
#  Config
# -------------------------------------------------------------------------
# Logging config
config.dictConfig(LOGGING_CONFIG)
# Parser config
parser = argparse.ArgumentParser()
parser.add_argument(
    '--project_name', type=str,
    help='Name of the Advanced Analytics project executed'
)
parser.add_argument(
    '--project_id', type=str,
    help='GCP project in which the script will be executed'
)
parser.add_argument(
    '--execution_date', type=str,
    help='DAG execution date'
)


# -------------------------------------------------------------------------
#  SQL Queries
# -------------------------------------------------------------------------
WORKFLOW_QUERIES = QueryDict({
    'workflow_data':
    """
    SELECT DISTINCT
        'Unimarc' AS store_banner,
        n_promocion,
        nombre_promocion,
        descripcion_mecanica,
        CASE
            WHEN
                descripcion_evento_promocional = 'UNI VENTA ESPECIAL'
                AND REGEXP_CONTAINS(nombre_promocion, '(?i)fresquito')
            THEN 'FRESQUITOS'
            WHEN descripcion_evento_promocional = 'UNI APOTEOSICO' THEN 'APOTEOSICO'
            WHEN descripcion_evento_promocional = 'UNI CATALOGO' THEN 'CATALOGO'
            ELSE 'UNI VENTA ESPECIAL'
        END AS tipo_promocion,
        descripcion_evento_promocional,
        CAST(fecha_inicio_de_promocion AS STRING) AS fecha_inicio_de_promocion,
        CAST(fecha_fin_de_promocion AS STRING) AS fecha_fin_de_promocion
    FROM `${gcp_project}.CDA_VISTAS.VW_FACT_WORKFLOW`
    WHERE
        n_promocion IN (${promotion_ids})
        AND ORGANIZACION_VENTAS = '1000' -- Unimarc

    UNION ALL

    SELECT DISTINCT
        'Mayorista' AS store_banner,
        n_promocion,
        nombre_promocion,
        descripcion_mecanica,
        CASE
            WHEN descripcion_evento_promocional = 'M10 CICLO' THEN 'M10 CICLO'
            WHEN descripcion_evento_promocional = 'M10 APOTEOSICO' THEN 'APOTEOSICO M10'
            WHEN descripcion_evento_promocional = 'M10 10 DE M10' THEN 'M10 10 DE M10'
            ELSE 'M10 VENTA ESPECIAL'
        END AS tipo_promocion,
        descripcion_evento_promocional,
        CAST(fecha_inicio_de_promocion AS STRING) AS fecha_inicio_de_promocion,
        CAST(fecha_fin_de_promocion AS STRING) AS fecha_fin_de_promocion
    FROM `${gcp_project}.CDA_VISTAS.VW_FACT_WORKFLOW`
    WHERE
        n_promocion IN (${promotion_ids})
        AND ORGANIZACION_VENTAS = '3000' -- Mayorista
    """,
})


# -------------------------------------------------------------------------
# Main function
# -------------------------------------------------------------------------
def main() -> None:  # noqa: D103
    # Parse input variables
    args = vars(parser.parse_args())
    user: str = args['project_name'] + '_halo'
    gcp_project: str = args['project_id']
    execution_date: str = args['execution_date']

    # Static
    gbq_client = Client()

    logging.info(f'execution_date: {execution_date}')

    promotions_to_evaluate = SharePointFile(**{
        **getSecret(
            'bdaa_sharepoint_credentials',
            gcp_project,
        ),
        'server_relative_path': (
            '/sites/'
            'BigDatayAdvancedAnalytics/'
            'Documentos compartidos/'
            'Pricing/'
            'Evaluacion Promociones/'
            'promotions_to_evaluate.xlsx'
        )
    }).toFrame().ffill(axis=0)
    promotions_to_evaluate.iloc[:, 0] = promotions_to_evaluate.iloc[:, 0].str.lower()

    workflow_data = gbq_extended.readBigQuery(
        query=WORKFLOW_QUERIES['workflow_data'].substitute(
            gcp_project=gcp_project,
            promotion_ids=', '.join(
                promotions_to_evaluate['n_promocion'].astype('str').to_list()
            ),
        ),
        user=user,
        gbq_client=gbq_client,
    )

    logging.info('Building promo linkage')
    if missing_promotions := list(
        set(promotions_to_evaluate['n_promocion'].to_list()).difference(
            set(workflow_data['n_promocion'].to_list())
        )
    ):
        logging.warning(f'The following promotions are missing: {list(missing_promotions)}')

        # Remove missing promotions from childs
        promotions_to_evaluate = promotions_to_evaluate[
            ~promotions_to_evaluate['n_promocion'].isin(missing_promotions)
        ]

        # Ussing child as parent if necessary
        promotions_to_evaluate = promotions_to_evaluate.iloc[:, ::-1].replace(
            dict.fromkeys(missing_promotions, pd.NA)
        ).ffill(
            axis=1
        ).iloc[:, ::-1]

    # Add parent promotion data
    logging.info('Adding promotion data to linkage')
    master_promotion_table = promotions_to_evaluate.merge(
        workflow_data[[
            'store_banner',
            'n_promocion',
            'nombre_promocion',
            'fecha_inicio_de_promocion',
            'fecha_fin_de_promocion',
            'tipo_promocion',
            'descripcion_evento_promocional',
            'descripcion_mecanica'
        ]].rename(columns={
            'n_promocion': 'n_promocion_ppal',
        }),
        on='n_promocion_ppal',
        how='inner',
    ).rename(columns={
        'nombre_promocion': 'nombre_promocion_ppal',
        'fecha_inicio_de_promocion': 'fecha_inicio_de_promocion_ppal',
        'fecha_fin_de_promocion': 'fecha_fin_de_promocion_ppal',
    # Add child promotion data
    }).merge(
        workflow_data[[
            'n_promocion',
            'nombre_promocion',
            'fecha_inicio_de_promocion',
            'fecha_fin_de_promocion'
        ]],
        on='n_promocion',
        how='inner',
    )

    # Add promotion month as first day of the month the promotion ends
    master_promotion_table['mes_promocion'] = (
        master_promotion_table['fecha_fin_de_promocion_ppal'].str[:-2]
        + '01'
    )

    logging.info('Uploading table to GBQ')
    gbq_extended.uploadFrame(
        df=master_promotion_table[[
            'store_banner',
            'n_promocion',
            'tipo_promocion',
            'nombre_promocion_ppal',
            'fecha_inicio_de_promocion_ppal',
            'fecha_fin_de_promocion_ppal',
            'mes_promocion',
            'descripcion_mecanica',
            'descripcion_evento_promocional',
            'nombre_promocion',
            'fecha_inicio_de_promocion',
            'fecha_fin_de_promocion'
        ]],
        table_ddl_json_path=os.path.join('gbq_objects', 'halo_promotions_to_evaluate.json'),
        project=gcp_project,
        if_exists='replace',
        gbq_client=gbq_client,
    )

    logging.info('Process ended!')


if __name__ == '__main__':
    main()
