import logging  # noqa: D100

from google.cloud import bigquery


PROJECT_ID = 'cl-bigdata-analytics-prod'
SOURCE_TABLE = (
    'cl-cda-unidata-dev.DS_DESA_LOCAL_JTORRESCE.CRM_DATA_SF_PUSH_EVENT')
TARGET_TABLE = (
    'cl-bigdata-analytics-prod.CRM.CRM_DATA_SF_PUSH_EVENT')
DATE_FIELD = 'FECHA_CARGA'


def get_max_date(client):  # noqa: ANN001, ANN201, D103

    query = f"""
        SELECT COALESCE(MAX({DATE_FIELD}), DATE('1900-01-01')) AS max_date
        FROM `{TARGET_TABLE}`
    """  # noqa: S608

    result = client.query(query).result()

    for row in result:
        return row.max_date
    return None


def get_pending_count(client, max_date):  # noqa: ANN001, ANN201, D103

    query = f"""
        SELECT COUNT(*) AS total
        FROM `{SOURCE_TABLE}`
        WHERE {DATE_FIELD} > DATE('{max_date}')
    """  # noqa: S608

    result = client.query(query).result()

    for row in result:
        return row.total

    return 0


def insert_new_records(client, max_date):  # noqa: ANN001, ANN201, D103

    query = f"""
        INSERT INTO `{TARGET_TABLE}`
        SELECT *
        FROM `{SOURCE_TABLE}`
        WHERE {DATE_FIELD} > DATE('{max_date}')
    """  # noqa: S608

    job = client.query(query)

    job.result()


def main():  # noqa: ANN201, D103

    logging.basicConfig(level=logging.INFO)
    client = bigquery.Client(project=PROJECT_ID)
    max_date = get_max_date(client)
    logging.info(f'Fecha máxima encontrada en destino: {max_date}')
    pending_rows = get_pending_count(client, max_date)
    logging.info(f'Registros pendientes por cargar: {pending_rows}')

    if pending_rows == 0:
        logging.info('No existen registros nuevos para insertar.')
        return

    insert_new_records(client, max_date)

    logging.info(f'Proceso finalizado. Registros insertados: {pending_rows}')


if __name__ == '__main__':
    main()
