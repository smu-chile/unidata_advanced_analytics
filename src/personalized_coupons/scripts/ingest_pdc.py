# Default
import os
import sys
import logging
import argparse
from logging import config

# pip
import pandas as pd
import pendulum
from requests.exceptions import HTTPError
from google.cloud.bigquery import Client

# Own
import common.gcp_extended.bigquery as gbq_extended
from common.constants import LOGGING_CONFIG
from common.databases.queries import QueryDict
from common.utils.data_transform import normalizeText
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
    '--project_name', type=str, required=True,
    help='Name fo the Advanced Analytics project executed'
)
parser.add_argument(
    '--gcp_project', type=str, required=True,
    help='Name of the GCP project billed. Used to differenciate dev from prod'
)
parser.add_argument(
    '--execution_date', type=str, required=True,
    help='DAG execution date'
)


# -------------------------------------------------------------------------
#  SQL Queries
# -------------------------------------------------------------------------
SQL_QUERIES = QueryDict({
    'last_internal_cycle':
    """
    SELECT max(ciclo_interno) AS last_internal_cycle
    FROM ${gcp_project}.FIDELIZACION.PLAN_DE_CAMPANAS_CUPONES_PERSONALIZADOS
    WHERE year = ${execution_year}-1
    """
})


# -------------------------------------------------------------------------
#                        Main Function
# -------------------------------------------------------------------------
def main() -> None:
    args = vars(parser.parse_args())
    # Environment parameters
    user: str = args['project_name']
    gcp_project: str = args['gcp_project']
    execution_date: pendulum.Date = pendulum.date(
        *list(map(int, args['execution_date'].split('-')))
    )

    # Static parameters
    gbq_client = Client()
    # TODO(ecastrot): Harcoded variables should be removed
    base_year = 2026
    base_period = 13

    logging.info(f'execution_date: {execution_date}')

    logging.info('Get the file from SharePoint')
    try:
        pdc_alvi_raw = SharePointFile(
            **getSecret(
                'bdaa_sharepoint_credentials',
                project=gcp_project,
            ),
            server_relative_path=(
                '/sites/'
                'BigDatayAdvancedAnalytics/'
                'Documentos compartidos/'
                'Ingesta/'
                'Ciclos Alvi/'
                f'PROCESADO-pdc_alvi_{execution_date.isoformat()}.xlsx'
            )
        ).toFrame(
            header=None
        )
    except HTTPError as e:
        if 'Not Found for url:' in str(e):
            msg = 'There was a problem reading the file from SharePoint. Skipping process'
            logging.exception(msg)
            # Skipper error code
            sys.exit(10)
        else:
            raise


    # Get the last internal cycle uploaded in the table
    logging.info('Getting max internal cycle for the last year')
    last_internal_cycle = gbq_extended.readBigQuery(
        SQL_QUERIES['last_internal_cycle'].substitute(
            gcp_project=gcp_project,
            execution_year=execution_date.year,
        ),
        user=user,
        gbq_client=gbq_client,
    ).to_numpy()[0,0]

    # Get only the part of the frame with the personalized offers
    pdc_alvi = pdc_alvi_raw.iloc[
        (pdc_alvi_raw[pdc_alvi_raw[0] == 'PERSONALIZADAS'].index[0] + 1):, :
    ].reset_index(
        drop=True
    )

    # Set frame header
    pdc_alvi = pdc_alvi.rename(
        columns=pdc_alvi.iloc[0]
    ).drop(
        pdc_alvi.index[0]
    ).reset_index(
        drop=True
    )

    # Add PERIODO. This mark increase by one once per year
    pdc_alvi['PERIODO'] = execution_date.year - base_year + base_period
    # Add CICLO INTERNO. This mark changes once per year
    pdc_alvi['CICLO INTERNO'] = last_internal_cycle + pdc_alvi.index + 1
    # Add year
    pdc_alvi['year'] = execution_date.year

    # Standarize column names
    logging.info(f"Original columns: {' | '.join(pdc_alvi.columns.to_list())}")
    pdc_alvi.columns = [
        normalizeText(
            col_name, lower=True, replace_spaces='_', strip_accents=True
        )
        for col_name in pdc_alvi.columns
    ]

    # Get only nunmber from cycle app
    pdc_alvi['ciclo_app'] = pdc_alvi['ciclo_app'].str.split(' ').str[-1]

    # Format date columns
    pdc_alvi['inicio'] = pd.to_datetime(
        pdc_alvi['inicio'], errors='coerce'
    ).replace({
        pd.NaT: None
    })
    pdc_alvi['fin'] = pd.to_datetime(
        pdc_alvi['fin'], errors='coerce'
    ).replace({
        pd.NaT: None
    })
    # Format integer columns
    pdc_alvi = pdc_alvi.astype({
        'periodo': int,
        'ciclo_app': int,
        'ciclo_interno': int,
    # Rename date columns
    }).drop_duplicates().reset_index(
        drop=True
    )[[
        'periodo', 'ciclo_app', 'inicio', 'fin', 'ciclo_interno', 'year'
    ]].rename(columns={
        'inicio': 'inicio_ciclo',
        'fin': 'fin_ciclo',
    })

    # TODO(ecastrot): Hardcoded store_banner here. Must be changed when
    # generalized
    pdc_alvi['store_banner'] = 'Alvi'

    logging.info('Remove last run if exists')
    # TODO(ecastrot): Hardcoded store_banner here. Must be changed when
    # generalized
    gbq_extended.deleteFromTable(
        table_ref=os.path.join('gbq_objects', 'plan_de_campanas_cupones_personalizados.json'),
        project=gcp_project,
        where_clause=f"""
            year = {execution_date.year}
            AND store_banner = 'Alvi'
        """,
        gbq_client=gbq_client,
    )

    logging.info('Upload the PDC for this run')
    gbq_extended.uploadFrame(
        df=pdc_alvi[[
            'year', 'store_banner', 'periodo', 'ciclo_app',
            'inicio_ciclo', 'fin_ciclo', 'ciclo_interno'
        ]],
        table_ddl_json_path=os.path.join(
            'gbq_objects', 'plan_de_campanas_cupones_personalizados.json'
        ),
        project=gcp_project,
        gbq_client=gbq_client,
        if_exists='append',
    )


if __name__ == '__main__':
    main()
