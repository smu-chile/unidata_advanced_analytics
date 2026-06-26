"""Load orden_comuna_localidad from PostgreSQL to BigQuery."""

# -------------------------------------------------------------------------
# Imports
# -------------------------------------------------------------------------
import os
import logging
import argparse
from logging import config

import pandas as pd
import pendulum
from google.cloud.bigquery import Client

from common.constants import LOGGING_CONFIG
from common.databases.queries import QueryDict
from common.databases.postgresql import readPostgresQuery
from common.gcp_extended.bigquery import uploadFrame
from common.gcp_extended.secretsmanager import getSecret


# -------------------------------------------------------------------------
# Logging config
# -------------------------------------------------------------------------
config.dictConfig(LOGGING_CONFIG)

# -------------------------------------------------------------------------
# Parser config
# -------------------------------------------------------------------------
parser = argparse.ArgumentParser()

parser.add_argument(
    '--project_id',
    type=str,
    required=True,
    help='GCP project id'
)

parser.add_argument(
    '--execution_date',
    type=str,
    required=True,
    help='Execution date YYYY-MM-DD'
)

# -------------------------------------------------------------------------
# SQL Queries
# -------------------------------------------------------------------------
SQL_QUERIES = QueryDict({
    'extract_all': """
    SELECT
        id_orden,
        id_localidad,
        comuna_de_la_orden,
        localidad
    FROM ecommdata.orden_comuna_localidad
    ORDER BY id_orden ASC
    """
})


# -------------------------------------------------------------------------
# Functions
# -------------------------------------------------------------------------
def get_existing_ids_from_bigquery(gbq_client: Client, project_id: str) -> set:
    """Obtiene todos los id_orden existentes en BigQuery.
    Solo trae los IDs.
    """  # noqa: D205
    table_id = f'{project_id}.ECOMMERCE.ORDEN_COMUNA_LOCALIDAD'

    query = f"""
    SELECT DISTINCT id_orden
    FROM `{table_id}`
    """  # noqa: S608

    try:
        result = gbq_client.query(query).to_dataframe()
        existing_ids = set(result['id_orden'].tolist()) if not result.empty else set()
        logging.info(f'IDs existentes en BigQuery: {len(existing_ids):,}')
        return existing_ids
    except Exception as e:  # noqa: BLE001
        logging.warning(f'Tabla no encontrada o primera carga: {e}')
        return set()


def filter_new_records(data: pd.DataFrame, existing_ids: set) -> pd.DataFrame:
    """Filtra en Python los registros que no existen en BigQuery.
    Esta es la parte clave - usamos pandas que es muy rápido.
    """  # noqa: D205
    if not existing_ids:
        logging.info('Primera carga - Todos los registros son nuevos')
        return data

    # Filtrar usando pandas (mucho más rápido que SQL con NOT IN)
    nuevos_ids = data[~data['id_orden'].isin(existing_ids)]

    logging.info(f'Registros totales en PostgreSQL: {len(data):,}')
    logging.info(f'Registros nuevos a cargar: {len(nuevos_ids):,}')
    logging.info(f'Registros ya existentes: {len(data) - len(nuevos_ids):,}')

    return nuevos_ids


def get_postgres_total_count(pg_credentials: dict) -> int:
    """Obtiene el total de registros en PostgreSQL (para monitoreo)."""
    query = 'SELECT COUNT(*) as total FROM ecommdata.orden_comuna_localidad'
    result = readPostgresQuery(query=query, credentials_dict=pg_credentials)
    return result['total'].iloc[0] if not result.empty else 0


# -------------------------------------------------------------------------
# Main
# -------------------------------------------------------------------------
def main() -> None:
    """Main process."""  # noqa: D401
    # ---------------------------------------------------------------------
    # Parse arguments
    # ---------------------------------------------------------------------
    args = vars(parser.parse_args())

    execution_date: pendulum.Date = pendulum.date(
        *list(map(int, args['execution_date'].split('-'))))

    gcp_project_id: str = args['project_id']

    logging.info(f'Execution date: {execution_date}')

    # ---------------------------------------------------------------------
    # BigQuery client
    # ---------------------------------------------------------------------
    gbq_client = Client()

    # ---------------------------------------------------------------------
    # Get PostgreSQL credentials
    # ---------------------------------------------------------------------
    logging.info('Obteniendo credenciales de PostgreSQL...')
    pg_credentials = getSecret(
        secret_name='ecommerce_postgres_credentials',  # noqa: S106
        project=gcp_project_id,
    )

    # ---------------------------------------------------------------------
    # Get total count from PostgreSQL (monitoreo)
    # ---------------------------------------------------------------------
    total_pg = get_postgres_total_count(pg_credentials)
    logging.info(f'Total registros en PostgreSQL: {total_pg:,}')

    # ---------------------------------------------------------------------
    # Get existing IDs from BigQuery (solo IDs, datos ligeros)
    # ---------------------------------------------------------------------
    logging.info('Obteniendo IDs existentes desde BigQuery...')
    existing_ids = get_existing_ids_from_bigquery(gbq_client, gcp_project_id)

    # ---------------------------------------------------------------------
    # Extract ALL data from PostgreSQL (una sola consulta)
    # ---------------------------------------------------------------------
    logging.info('Extrayendo TODOS los datos desde PostgreSQL...')
    data = readPostgresQuery(
        query=SQL_QUERIES['extract_all'].substitute(),
        credentials_dict=pg_credentials
    )

    logging.info(f'Datos extraídos de PostgreSQL: {data.shape[0]:,} registros')

    # ---------------------------------------------------------------------
    # Filter new records in Python (rápido y eficiente)
    # ---------------------------------------------------------------------
    logging.info('Filtrando registros nuevos en Python...')
    new_data = filter_new_records(data, existing_ids)

    # ---------------------------------------------------------------------
    # Verificar si hay datos nuevos
    # ---------------------------------------------------------------------
    if new_data.empty:
        logging.info('✅ No hay registros nuevos para cargar')
        print('\n' + '='*60)  # noqa: T201
        print('📊 CARGA INCREMENTAL COMPLETADA')  # noqa: T201
        print('='*60)  # noqa: T201
        print('✅ No hay registros nuevos para procesar')  # noqa: T201
        print(f'📊 Total en PostgreSQL: {total_pg:,}')  # noqa: T201
        print(f'📊 Total en BigQuery: {len(existing_ids):,}')  # noqa: T201
        print('='*60 + '\n')  # noqa: T201
        return

    # ---------------------------------------------------------------------
    # Validaciones de calidad
    # ---------------------------------------------------------------------
    # Verificar que no haya duplicados en los nuevos datos
    if new_data['id_orden'].duplicated().any():
        duplicates = new_data[new_data['id_orden'].duplicated()]
        logging.warning(f'⚠️ Se encontraron {len(duplicates)} IDs duplicados en los nuevos datos')
        logging.warning('Eliminando duplicados, manteniendo el primero...')
        new_data = new_data.drop_duplicates(subset=['id_orden'], keep='first')

    # Verificar que no haya IDs nulos
    if new_data['id_orden'].isna().any():
        null_count = new_data['id_orden'].isna().sum()
        logging.error(f'❌ Error: {null_count} registros con id_orden nulo')
        logging.error('Eliminando registros con id_orden nulo...')
        new_data = new_data.dropna(subset=['id_orden'])

    # ---------------------------------------------------------------------
    # Mostrar resumen de datos nuevos
    # ---------------------------------------------------------------------
    nuevos_ids = set(new_data['id_orden'].tolist())

    logging.info(f'✅ Registros nuevos a cargar: {len(new_data):,}')
    logging.info(f'Rango de IDs nuevos: {min(nuevos_ids)} - {max(nuevos_ids)}')
    logging.info(f'Muestra de IDs nuevos (primeros 10): {list(nuevos_ids)[:10]}')

    # Verificar que realmente son nuevos (doble validación)
    duplicados = nuevos_ids.intersection(existing_ids)
    if duplicados:
        logging.error(f'❌ CRÍTICO: {len(duplicados)} IDs duplicados encontrados')
        logging.error(f'IDs duplicados: {list(duplicados)[:10]}')
        # Remover duplicados por seguridad
        new_data = new_data[~new_data['id_orden'].isin(duplicados)]
        logging.info(f'IDs duplicados removidos. Quedan {len(new_data):,} registros')

    # ---------------------------------------------------------------------
    # Upload to BigQuery (APPEND)
    # ---------------------------------------------------------------------
    logging.info('Cargando datos nuevos a BigQuery (modo APPEND)...')

    json_path = os.path.join('gbq_objects', 'orden_comuna_localidad.json')
    if not os.path.exists(json_path):
        logging.error(f'❌ Archivo JSON no encontrado: {json_path}')
        return

    uploadFrame(
        new_data,
        table_ddl_json_path=json_path,
        project=gcp_project_id,
        gbq_client=gbq_client,
        if_exists='append',
    )

    # ---------------------------------------------------------------------
    # Resumen final
    # ---------------------------------------------------------------------
    total_bq_final = len(existing_ids) + len(new_data)

    logging.info('✅ Proceso completado exitosamente')

    print('\n' + '='*70)  # noqa: T201
    print('📊 CARGA INCREMENTAL COMPLETADA (Filtro en Python)')  # noqa: T201
    print('='*70)  # noqa: T201
    print(f'📁 Tabla origen: ecommdata.orden_comuna_localidad')  # noqa: F541, T201
    print(f'📁 Tabla destino: ECOMMERCE.ORDEN_COMUNA_LOCALIDAD')  # noqa: F541, T201
    print(f'📊 Total en PostgreSQL: {total_pg:,}')  # noqa: T201
    print(f'📊 IDs existentes en BQ: {len(existing_ids):,}')  # noqa: T201
    print(f'📈 IDs nuevos cargados: {len(new_data):,}')  # noqa: T201
    print(f'📊 Total IDs en BQ (estimado): {total_bq_final:,}')  # noqa: T201
    if len(new_data) > 0:
        print(f'🔢 Rango IDs nuevos: {min(nuevos_ids)} - {max(nuevos_ids)}')  # noqa: T201
    print('='*70 + '\n')  # noqa: T201

# -------------------------------------------------------------------------
# Entrypoint
# -------------------------------------------------------------------------
if __name__ == '__main__':
    main()
