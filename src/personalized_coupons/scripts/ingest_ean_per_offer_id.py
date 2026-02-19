# Default
import os
import sys
import logging
import argparse
from logging import config

# pip
import pendulum
from requests.exceptions import HTTPError
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
    'cycle_info_from_pdc':
    """
    SELECT
        ciclo_app,
        CAST(inicio_ciclo AS STRING) AS inicio_ciclo,
        CAST(fin_ciclo AS STRING) AS fin_ciclo,
        ciclo_interno
    FROM ${gcp_project}.FIDELIZACION.PLAN_DE_CAMPANAS_CUPONES_PERSONALIZADOS
    WHERE inicio_ciclo = DATE('${cycle_start_date}')
    LIMIT 1
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

    logging.info(f'execution_date: {execution_date}')

    try:
        ean_df = SharePointFile(
            **getSecret(
                'bdaa_sharepoint_credentials',
                project=gcp_project,
            ),
            server_relative_path=(
                '/sites/'
                'BigDatayAdvancedAnalytics/'
                'Documentos compartidos/'
                'Power Automate/'
                'Correos Cupones Alvi/'
                f'PROCESADO-planilla_alvi_ean_{execution_date.isoformat()}.xlsx'
            )
        ).toFrame()

        offerid_df = SharePointFile(
            **getSecret(
                'bdaa_sharepoint_credentials',
                project=gcp_project,
            ),
            server_relative_path=(
                '/sites/'
                'BigDatayAdvancedAnalytics/'
                'Documentos compartidos/'
                'Power Automate/'
                'Correos Cupones Alvi/'
                f'PROCESADO-planilla_alvi_ofe_{execution_date.isoformat()}.xlsx'
            )
        ).toFrame()
    except HTTPError as e:
        if 'Not Found for url:' in str(e):
            msg = 'At least one of the two required files was not found. Skipping process'
            logging.exception(msg)
            # Skipper error code
            sys.exit(10)
        else:
            raise

    logging.info('Loading frames')
    offerid_df.columns = offerid_df.columns.str.lower()
    ean_df.columns = ean_df.columns.str.lower()

    logging.info('Formatting offer id frame')
    offerid_df = offerid_df[[
        'id_workflow', 'n_promocion', 'offer_id', 'fecha_inicio_ciclo','fecha_fin_ciclo'
    ]]

    logging.info('Formatting ean frame')
    aggregated_df = offerid_df.merge(
        ean_df,
        on='offer_id',
        how='left'
    )
    # Format columns as date str
    aggregated_df['fecha_fin_ciclo'] = aggregated_df['fecha_fin_ciclo'].dt.date.astype(str)
    aggregated_df['fecha_inicio_ciclo'] = aggregated_df['fecha_inicio_ciclo'].dt.date.astype(str)

    logging.info('Getting cycle info from pdc table')
    cycle_info = gbq_extended.readBigQuery(
        query=SQL_QUERIES['cycle_info_from_pdc'].substitute(
            gcp_project=gcp_project,
            cycle_start_date=aggregated_df['fecha_inicio_ciclo'].unique()[0],
        ),
        user=user,
        gbq_client=gbq_client,
    )

    aggregated_df = aggregated_df.merge(
        cycle_info,
        how='left',
        left_on='fecha_inicio_ciclo',
        right_on='inicio_ciclo',
    )
    cycles = aggregated_df['ciclo_app'].dropna().unique().tolist()
    cycle = cycles[0] if len(cycles) == 1 else f'{cycles[0]}-{cycles[1]}'
    aggregated_df['nombre_promocion'] = (
        'APP CICLO '
        + str(cycle)
        + ' ALVI '
        + aggregated_df['tipo_oferta']
    ).str.upper()
    aggregated_df['description'] = (
        aggregated_df['category'].str.strip()
        + ' '
        + aggregated_df['brand_desc'].str.strip()
    ).str.replace(
        r'\s+', ' ', regex=True
    )
    aggregated_df = aggregated_df[[
        'nombre_promocion', 'ciclo_app','ciclo_interno', 'offer_id',
        'description', 'tipo_oferta','material', 'ean','id_workflow',
        'n_promocion', 'fecha_inicio_ciclo', 'fecha_fin_ciclo'
    ]].dropna(subset=[
        'fecha_inicio_ciclo', 'fecha_fin_ciclo', 'offer_id'
    ]).dropna(
        subset=['material', 'ean'],
        how='all'
    )
    aggregated_df['tipo_oferta'] = aggregated_df['tipo_oferta'].str.upper()
    aggregated_df.insert(0, 'store_banner', 'Alvi')

    # Drop past run from table
    logging.info('Removing past run from table')
    logging.info(
        f"The last run cycle starts on: {aggregated_df['fecha_inicio_ciclo'].unique()[0]}"
    )
    logging.info(f"The last run cycle ends on: {aggregated_df['fecha_fin_ciclo'].unique()[0]}")
    gbq_extended.deleteFromTable(
        table_ref=os.path.join('gbq_objects', 'ean_por_offer_id_cupones_personalizados.json'),
        project=gcp_project,
        where_clause=f"""
            fecha_inicio_ciclo = DATE('{aggregated_df['fecha_inicio_ciclo'].unique()[0]}')
            AND fecha_fin_ciclo = DATE('{aggregated_df['fecha_fin_ciclo'].unique()[0]}')
        """,
        gbq_client=gbq_client,
    )

    logging.info('Uploading data')
    gbq_extended.uploadFrame(
        df=aggregated_df,
        table_ddl_json_path=os.path.join(
            'gbq_objects', 'ean_por_offer_id_cupones_personalizados.json'
        ),
        project=gcp_project,
        gbq_client=gbq_client,
        if_exists='append',
    )


if __name__ == '__main__':
    main()
