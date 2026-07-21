"""Load table (id,fecha,comuna) from PostgreSQL to BigQuery.
(incremental fecha).
"""  # noqa: D205
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
# SQL Queries (ajusta el nombre de la tabla y columnas según tu caso)
# -------------------------------------------------------------------------
SQL_QUERIES = QueryDict({
    'extract_all': """
    SELECT
        id,
        fecha_creacion,
        comuna_de_la_orden
    FROM power_bi.ordenes_comuna_corregida_alvi
    ORDER BY fecha_creacion ASC, id ASC
    """
})


# -------------------------------------------------------------------------
# Functions
# -------------------------------------------------------------------------
def get_max_date_from_bigquery(gbq_client: Client, project_id: str) -> str | None:
    """Obtiene la fecha máxima (tipo DATE) de la tabla en BigQuery.
    Retorna None si la tabla no existe o está vacía.
    """  # noqa: D205
    table_id = f'{project_id}.ECOMMERCE.ORDENES_COMUNA_CORREGIDA_ALVI'

    query = f"""
    SELECT MAX(fecha_creacion) as max_fecha
    FROM `{table_id}`
    """  # noqa: S608

    try:
        result = gbq_client.query(query).to_dataframe()
        if not result.empty and result['max_fecha'].iloc[0] is not None:
            max_date = result['max_fecha'].iloc[0]
            logging.info(f'Fecha máxima en BigQuery: {max_date}')
            return max_date
        else:  # noqa: RET505
            logging.info('Tabla vacía o sin registros. Se realizará primera carga completa.')
            return None
    except Exception as e:  # noqa: BLE001
        logging.warning(f'Tabla no encontrada o error al obtener fecha máxima: {e}. Se realizará primera carga completa.')  # noqa: E501
        return None


def delete_records_by_date(gbq_client: Client, project_id: str, date_to_delete) -> int:  # noqa: ANN001
    """Elimina de BigQuery todos los registros con fecha = date_to_delete.
    Retorna el número de filas eliminadas.
    """  # noqa: D205
    table_id = f'{project_id}.ECOMMERCE.ORDENES_COMUNA_CORREGIDA_ALVI'
    # Convertir date a string YYYY-MM-DD si es datetime
    if hasattr(date_to_delete, 'strftime'):
        date_str = date_to_delete.strftime('%Y-%m-%d')
    else:
        date_str = str(date_to_delete)

    delete_query = f"""
    DELETE FROM `{table_id}`
    WHERE fecha_creacion = DATE('{date_str}')
    """  # noqa: S608

    logging.info(f'Eliminando registros con fecha = {date_str}...')
    try:
        job = gbq_client.query(delete_query)
        job.result()
        logging.info('Eliminación completada.')
        return 0
    except Exception as e:
        logging.error(f'Error al eliminar registros: {e}')  # noqa: TRY400
        raise


def filter_new_records_by_date(data: pd.DataFrame, max_date) -> pd.DataFrame:  # noqa: ANN001
    """Filtra los registros cuya fecha sea >= max_date (si max_date no es None).
    Si max_date es None, retorna todos los registros (primera carga).
    """  # noqa: D205, W505
    if max_date is None:
        logging.info('Primera carga: se tomarán TODOS los registros.')
        return data

    # Asegurar que fecha sea tipo date
    if not pd.api.types.is_datetime64_any_dtype(data['fecha_creacion']):
        data['fecha_creacion'] = pd.to_datetime(data['fecha_creacion']).dt.date

    # Filtrar
    filtered = data[data['fecha_creacion'] >= max_date]
    logging.info(f'Registros totales en PostgreSQL: {len(data):,}')
    logging.info(f'Registros con fecha >= {max_date}: {len(filtered):,}')
    return filtered


def get_postgres_total_count(pg_credentials: dict) -> int:
    """Obtiene el total de registros en PostgreSQL (para monitoreo)."""
    query = 'SELECT COUNT(*) as total FROM power_bi.ordenes_comuna_corregida_alvi'
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
    # Get max date from BigQuery
    # ---------------------------------------------------------------------
    logging.info('Obteniendo fecha máxima desde BigQuery...')
    max_date = get_max_date_from_bigquery(gbq_client, gcp_project_id)

    # ---------------------------------------------------------------------
    # Extract ALL data from PostgreSQL
    # ---------------------------------------------------------------------
    logging.info('Extrayendo TODOS los datos desde PostgreSQL...')
    data = readPostgresQuery(
        query=SQL_QUERIES['extract_all'].substitute(),
        credentials_dict=pg_credentials
    )

    logging.info(f'Datos extraídos de PostgreSQL: {data.shape[0]:,} registros')

    # ---------------------------------------------------------------------
    # Filter records by date (fecha >= max_date)
    # ---------------------------------------------------------------------
    logging.info('Filtrando registros según fecha máxima...')
    new_data = filter_new_records_by_date(data, max_date)

    # ---------------------------------------------------------------------
    # Verificar si hay datos nuevos
    # ---------------------------------------------------------------------
    if new_data.empty:
        logging.info('No hay registros nuevos para cargar (fecha >= max_date)')
        logging.info('=' * 60)
        logging.info('CARGA INCREMENTAL COMPLETADA')
        logging.info('=' * 60)
        logging.info('No hay registros nuevos para procesar')
        logging.info(f'Total en PostgreSQL: {total_pg:,}')
        logging.info(f"Fec.Max en BQ: {max_date if max_date else 'N/A'}")
        logging.info('=' * 60)
        return

    # ---------------------------------------------------------------------
    # Si hay fecha máxima, eliminar los registros con esa fecha en BigQuery
    # (para evitar duplicados y actualizar posibles cambios)
    # ---------------------------------------------------------------------
    if max_date is not None:
        logging.info(f'Eliminando registros con fecha = {max_date} en BigQuery...')
        delete_records_by_date(gbq_client, gcp_project_id, max_date)

    # ---------------------------------------------------------------------
    # Validaciones de calidad
    # ---------------------------------------------------------------------
    # Verificar que no haya duplicados de (fecha, id) por si acaso
    # Si la clave compuesta es (fecha, id), puedes validar:
    if new_data.duplicated(subset=['fecha_creacion', 'id']).any():
        dup_count = new_data.duplicated(subset=['fecha_creacion', 'id']).sum()
        logging.warning(f'⚠️ Se encontraron {dup_count} registros duplicados (fecha_creacion, id). Eliminando duplicados...')  # noqa: E501
        new_data = new_data.drop_duplicates(subset=['fecha_creacion', 'id'], keep='first')

    # Verificar que no haya fechas nulas
    if new_data['fecha_creacion'].isna().any():
        null_count = new_data['fecha_creacion'].isna().sum()
        logging.error(f'❌ Error: {null_count} registros con fecha nula. Eliminando...')
        new_data = new_data.dropna(subset=['fecha_creacion'])

    # ---------------------------------------------------------------------
    # Mostrar resumen de datos a cargar
    # ---------------------------------------------------------------------
    min_date = new_data['fecha_creacion'].min()
    max_date_new = new_data['fecha_creacion'].max()
    logging.info(f'✅ Registros nuevos a cargar: {len(new_data):,}')
    logging.info(f'Rango de fechas a cargar: {min_date} - {max_date_new}')

    # ---------------------------------------------------------------------
    # Upload to BigQuery (APPEND)
    # ---------------------------------------------------------------------
    logging.info('Cargando datos nuevos a BigQuery (modo APPEND)...')

    # Asegurar que el archivo JSON de esquema existe
    json_path = os.path.join('gbq_objects', 'ordenes_comuna_corregida_alvi.json')
    if not os.path.exists(json_path):
        logging.error(f'❌ Archivo JSON no encontrado: {json_path}')
        logging.error('Crea el archivo con el esquema de la tabla destino.')
        return

    uploadFrame(
        new_data,
        table_ddl_json_path=json_path,
        project=gcp_project_id,
        gbq_client=gbq_client,
        if_exists='append',
    )
    # ---------------------------------------------------------------------
    # Resumen final (usando solo logging)
    # ---------------------------------------------------------------------
    logging.info('✅ Proceso completado exitosamente')

    # Variables auxiliares
    fecha_max_bq = max_date if max_date else 'N/A'
    umbral_fecha = max_date if max_date else 'TODOS'

    # Construcción del resumen
    summary_lines = [
        '=' * 70,
        'CARGA INCREMENTAL COMPLETADA (por fecha_creacion)',
        '=' * 70,
        'Tbl ori: power_bi.'
        'ordenes_comuna_corregida_alvi',
        'Tbl des: ECOMMERCE.'
        'ORDENES_COMUNA_CORREGIDA_ALVI',
        f'Total en PostgreSQL: {total_pg:,}',
        f'Fec.Max previa en BQ: {fecha_max_bq}',
        f'Reg.Cargados (fecha>={umbral_fecha}): {len(new_data):,}',
    ]

    if not new_data.empty:
        min_date = new_data['fecha_creacion'].min()
        max_date_new = new_data['fecha_creacion'].max()
        summary_lines.append(
            f'Rango de fechas cargadas: {min_date} - {max_date_new}'
        )

    summary_lines.append('=' * 70)

    logging.info('\n'.join(summary_lines))

# -------------------------------------------------------------------------
# Entrypoint
# -------------------------------------------------------------------------
if __name__ == '__main__':
    main()
