import csv  # noqa: D100
import json
import time
import logging
import argparse
import datetime

import pandas as pd
import paramiko
from google.cloud import bigquery
from google.api_core.exceptions import NotFound

from common.gcp_extended import secretsmanager


# ---------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# ---------------------------------------------------------------------
# Configuración PROD
# ---------------------------------------------------------------------
PROJECT_ID = 'cl-bigdata-analytics-prod'
DATASET = 'CRM'

# Tabla STG de prueba
STG_TABLE = 'CRM_DATA_SF_PUSH_EVENT_STG'
STG_TABLE_ID = f'{PROJECT_ID}.{DATASET}.{STG_TABLE}'
FINAL_TABLE = 'CRM_DATA_SF_PUSH_EVENT'
FINAL_TABLE_ID = f'{PROJECT_ID}.{DATASET}.{FINAL_TABLE}'

# ---------------------------------------------------------------------
# SFTP
# ---------------------------------------------------------------------
REMOTE_PATH = '/Import'
REMOTE_FILE = 'MobilePushDetailExtractReport.csv'

# ---------------------------------------------------------------------
# Salesforce
# ---------------------------------------------------------------------
EID = 546001831

# ---------------------------------------------------------------------
# Formatos
# ---------------------------------------------------------------------
FORMATOS = {
    'unimarc': {
        'business_unit': 'Unimarc',
        'client_id': 546002518
    },
    'alvi': {
        'business_unit': 'Alvi',
        'client_id': 546002699
    }
}

def validar_duplicados_stg(  # noqa: D103
    client: bigquery.Client, stg_table: str) -> None:
    query = f'''
        SELECT
            BUSINESS_UNIT,
            REQUEST_ID,
            DEVICE_ID,
            SUBSCRIBERKEY,
            PUSH_JOB_ID,
            FECHA_CARGA,
            COUNT(*) AS CANTIDAD
        FROM `{stg_table}`
        GROUP BY
            BUSINESS_UNIT,
            REQUEST_ID,
            DEVICE_ID,
            SUBSCRIBERKEY,
            PUSH_JOB_ID,
            FECHA_CARGA
        HAVING COUNT(*) > 1
        ORDER BY CANTIDAD DESC
        LIMIT 100
    '''  # noqa: Q001, S608

    query_job = client.query(query)
    rows = list(query_job.result())

    if rows:
        print('ERROR: Se encontraron registros duplicados en STG.')  # noqa: T201
        for row in rows:
            print(  # noqa: T201
                f'BUSINESS_UNIT={row['BUSINESS_UNIT']},'
                f'REQUEST_ID={row['REQUEST_ID']},'
                f'DEVICE_ID={row['DEVICE_ID']},'
                f'SUBSCRIBERKEY={row['SUBSCRIBERKEY']},'
                f'PUSH_JOB_ID={row['PUSH_JOB_ID']},'
                f'FECHA_CARGA={row['FECHA_CARGA']},'
                f'CANTIDAD={row['CANTIDAD']}')

        raise ValueError(
            'La tabla STG contiene duplicados para la llave definida. '  # noqa: EM101
            'Se detiene el proceso para evitar una carga incorrecta.')

    print('OK: No existen duplicados de llave en STG.')  # noqa: T201


def ejecutar_merge_bq(  # noqa: D103
    client: bigquery.Client, stg_table: str, final_table: str) -> None:

    query = f'''
        MERGE `{final_table}` AS T
        USING `{stg_table}` AS S
        ON
            T.BUSINESS_UNIT = S.BUSINESS_UNIT
            AND T.REQUEST_ID = S.REQUEST_ID
            AND T.DEVICE_ID = S.DEVICE_ID
            AND T.SUBSCRIBERKEY = S.SUBSCRIBERKEY
            AND T.PUSH_JOB_ID = S.PUSH_JOB_ID
            AND T.FECHA_CARGA = S.FECHA_CARGA
        WHEN NOT MATCHED THEN
            INSERT (EVENT_DATE,
                CLIENT_ID,
                EID,
                APP_NAME,
                MESSAGE_NAME,
                MESSAGE_ID,
                TEMPLATE,
                FORMAT_TYPE,
                GEOFENCENAME,
                PAGE_NAME,
                CAMPAIGNS,
                DEVICE_ID,
                SUBSCRIBERKEY,
                DATETIME_SEND,
                MESSAGE_CONTENT,
                MESSAGE_OPENED,
                OPEN_DATE,
                TIME_IN_APP,
                PLATFORM,
                PLATFORM_VERSION,
                STATUS,
                PUSH_JOB_ID,
                SYSTEM_TOKEN,
                INBOX_DOWNLOAD,
                INBOX_OPEN,
                IOS_MEDIA_URL,
                ANDROID_MEDIA_URL,
                MEDIA_ALT,
                REQUEST_ID,
                BUSINESS_UNIT,
                FECHA_CARGA)
            VALUES (
                S.EVENT_DATE,
                S.CLIENT_ID,
                S.EID,
                S.APP_NAME,
                S.MESSAGE_NAME,
                SAFE_CAST(S.MESSAGE_ID AS INT64),
                S.TEMPLATE,
                S.FORMAT_TYPE,
                S.GEOFENCENAME,
                S.PAGE_NAME,
                S.CAMPAIGNS,
                S.DEVICE_ID,
                S.SUBSCRIBERKEY,
                S.DATETIME_SEND,
                S.MESSAGE_CONTENT,
                S.MESSAGE_OPENED,
                S.OPEN_DATE,
                S.TIME_IN_APP,
                S.PLATFORM,
                S.PLATFORM_VERSION,
                S.STATUS,
                S.PUSH_JOB_ID,
                S.SYSTEM_TOKEN,
                S.INBOX_DOWNLOAD,
                S.INBOX_OPEN,
                S.IOS_MEDIA_URL,
                S.ANDROID_MEDIA_URL,
                S.MEDIA_ALT,
                S.REQUEST_ID,
                S.BUSINESS_UNIT,
                S.FECHA_CARGA
            )
    '''  # noqa: Q001, S608

    print('Ejecutando MERGE STG → tabla final...')  # noqa: T201

    query_job = client.query(query)
    query_job.result()

    print('OK: MERGE ejecutado correctamente.')  # noqa: T201

def validar_carga_final(  # noqa: D103
    client: bigquery.Client, stg_table: str, final_table: str) -> None:

    query_stg = f'''
        SELECT COUNT(*) AS CANTIDAD FROM `{stg_table}`'''  # noqa: Q001, S608

    query_final = f'''
        SELECT COUNT(*) AS CANTIDAD FROM `{final_table}`'''  # noqa: Q001, S608

    stg_count = list(client.query(query_stg).result())[0]['CANTIDAD']  # noqa: RUF015
    final_count = list(client.query(query_final).result())[0]['CANTIDAD']  # noqa: RUF015

    print(f'Registros en STG: {stg_count}')  # noqa: T201
    print(f'Registros en tabla final: {final_count}')  # noqa: T201

    print('OK: Validación posterior al MERGE finalizada.')  # noqa: T201

# ---------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------
def main():  # noqa: ANN201, D103

    parser = argparse.ArgumentParser()
    parser.add_argument(
        '--schema_file',
        required=True,
        help='Ruta al archivo JSON con el schema de la tabla STG.')

    args = parser.parse_args()
    schema_file = args.schema_file
    logging.info('Schema file: %s', schema_file)

    with open(schema_file, 'r', encoding='utf-8') as f:  # noqa: UP015
        schema_json = json.load(f)

    schema = [
        bigquery.SchemaField(
            field['name'],
            field['field_type'],
            mode=field.get('mode', 'NULLABLE')
        )
        for field in schema_json['columns']
    ]

    today = datetime.date.today()  # noqa: DTZ011

    logging.info('=' * 80)
    logging.info('INICIO PROCESO SFTP -> BIGQUERY STG')
    logging.info('=' * 80)

    # ---------------------------------------------------------------
    # Credenciales
    # ---------------------------------------------------------------
    logging.info('Obteniendo credenciales SFTP...')

    sftp_secret = secretsmanager.getSecret(
        'salesforce_sftp_credentials',
        project='cl-bigdata-analytics-prod')

    # ---------------------------------------------------------------
    # BigQuery
    # ---------------------------------------------------------------
    bq_client = bigquery.Client(project=PROJECT_ID)

    # ---------------------------------------------------------
    # Crear tabla STG si no existe
    # ---------------------------------------------------------
    table_ref = f'{STG_TABLE_ID}'

    try:
        bq_client.get_table(table_ref)
        logging.info('La tabla STG ya existe: %s', STG_TABLE)

    except NotFound:
        logging.info('La tabla STG no existe. Creándola desde el JSON...')

        schema = [
            bigquery.SchemaField(
                column['name'],
                column['field_type'],
                mode=column.get('mode', 'NULLABLE')
            )
            for column in schema_json['columns']]

        table = bigquery.Table(table_ref, schema=schema)
        bq_client.create_table(table)

        logging.info('Tabla STG creada correctamente en BQ: %s', STG_TABLE)

    # ---------------------------------------------------------------
    # Limpiar tabla STG antes de iniciar la carga
    # ---------------------------------------------------------------
    bq_client.query(f'TRUNCATE TABLE `{STG_TABLE_ID}`').result()
    logging.info('Tabla STG truncada correctamente.')

    # ---------------------------------------------------------------
    # Procesar formatos
    # ---------------------------------------------------------------
    for formato, cfg in FORMATOS.items():

        logging.info('=' * 80)
        logging.info('Formato : %s', formato.upper())

        host = sftp_secret['host']
        port = int(sftp_secret['port'])
        user = sftp_secret[f'user_{formato}']
        password = sftp_secret[f'pass_{formato}']

        remote_file = f'{REMOTE_PATH}/{REMOTE_FILE}'

        transport = None
        sftp = None

        try:

            # -------------------------------------------------------
            # Conectar SFTP
            # -------------------------------------------------------
            transport = paramiko.Transport((host, port))
            transport.connect(username=user, password=password)
            sftp = paramiko.SFTPClient.from_transport(transport)
            logging.info('Conexión SFTP exitosa.')

            # -------------------------------------------------------
            # Validar existencia archivo
            # -------------------------------------------------------
            try:
                sftp.stat(remote_file)

            except FileNotFoundError:

                logging.warning(
                    'Archivo %s no existe para %s. Se continúa.',
                    remote_file, formato)

                continue

            logging.info('Archivo encontrado: %s', remote_file)

            # -------------------------------------------------------
            # Leer CSV directamente desde SFTP
            # -------------------------------------------------------
            logging.info('Leyendo CSV completo desde SFTP...')

            inicio_lectura = time.time()
            registros = []
            with sftp.open(remote_file, 'r', bufsize=1024 * 1024) as f:
                reader = csv.DictReader(f)

                for row in reader:
                    registros.append(row)  # noqa: PERF402

            fin_lectura = time.time()

            logging.info(
                'Lectura SFTP finalizada. '
                'Registros: %s. '
                'Tiempo: %.2f segundos',
                len(registros), fin_lectura - inicio_lectura)

            # -------------------------------------------------------
            # Validar archivo vacío
            # -------------------------------------------------------
            if not registros:

                logging.info(
                    "El archivo '%s' no contiene ningún registro. "
                    "Se omite el formato.", formato)
                continue

            # -------------------------------------------------------
            # Crear DataFrame
            # -------------------------------------------------------
            df_push = pd.DataFrame(registros)
            logging.info('DataFrame creado. Registros: %s', len(df_push))

            # -------------------------------------------------------
            # Normalizar columnas
            # -------------------------------------------------------
            df_push.columns = [
                str(col)
                .replace('\ufeff', '')
                .strip()
                for col in df_push.columns]

            # -------------------------------------------------------
            # Eliminar columna no utilizada
            # -------------------------------------------------------
            if 'ServiceResponse' in df_push.columns:
                df_push.drop(columns=['ServiceResponse'], inplace=True)  # noqa: PD002

            # -------------------------------------------------------
            # Renombrar columnas
            # -------------------------------------------------------
            df_push.rename(
                columns={
                    'AppName': 'APP_NAME',
                    'MessageName': 'MESSAGE_NAME',
                    'MessageID': 'MESSAGE_ID',
                    'Campaigns': 'CAMPAIGNS',
                    'DeviceId': 'DEVICE_ID',
                    'DateTimeSend': 'DATETIME_SEND',
                    'MessageContent': 'MESSAGE_CONTENT',
                    'MessageOpened': 'MESSAGE_OPENED',
                    'OpenDate': 'OPEN_DATE',
                    'TimeInApp': 'TIME_IN_APP',
                    'Platform': 'PLATFORM',
                    'PlatformVersion': 'PLATFORM_VERSION',
                    'Status': 'STATUS',
                    'GeofenceName': 'GEOFENCENAME',
                    'Template': 'TEMPLATE',
                    'Format': 'FORMAT_TYPE',
                    'PageName': 'PAGE_NAME',
                    'PushJobId': 'PUSH_JOB_ID',
                    'SystemToken': 'SYSTEM_TOKEN',
                    'InboxMessageDownloaded': 'INBOX_DOWNLOAD',
                    'InboxMessageOpened': 'INBOX_OPEN',
                    'IosMediaUrl': 'IOS_MEDIA_URL',
                    'AndroidMediaUrl': 'ANDROID_MEDIA_URL',
                    'MediaAlt': 'MEDIA_ALT',
                    'ContactKey': 'SUBSCRIBERKEY',
                    'RequestId': 'REQUEST_ID'
                }, inplace=True)  # noqa: PD002

            # -------------------------------------------------------
            # Agregar columnas adicionales
            # -------------------------------------------------------
            df_push['BUSINESS_UNIT'] = cfg['business_unit']
            df_push['EVENT_DATE'] = today
            df_push['CLIENT_ID'] = cfg['client_id']
            df_push['EID'] = EID
            df_push['FECHA_CARGA'] = today

            # -------------------------------------------------------
            # Conversión de fechas
            # -------------------------------------------------------
            df_push['DATETIME_SEND'] = pd.to_datetime(
                df_push['DATETIME_SEND'],
                format='%m/%d/%Y %I:%M:%S %p', errors='coerce')

            df_push['OPEN_DATE'] = pd.to_datetime(
                df_push['OPEN_DATE'],
                format='%m/%d/%Y %I:%M:%S %p', errors='coerce')

            # -------------------------------------------------------
            # Validar columnas obligatorias
            # -------------------------------------------------------
            columnas_obligatorias = [
                'APP_NAME',
                'DEVICE_ID',
                'DATETIME_SEND',
                'REQUEST_ID'
            ]

            faltantes = [
                columna
                for columna in columnas_obligatorias
                if columna not in df_push.columns
            ]

            if faltantes:
                logging.error(
                    'Faltan columnas obligatorias en %s: %s',
                    formato, faltantes)

                logging.info('Columnas disponibles: %s', list(df_push.columns))
                continue

            # -------------------------------------------------------
            # Cerrar SFTP
            # -------------------------------------------------------
            sftp.close()
            sftp = None

            transport.close()
            transport = None

            # -------------------------------------------------------
            # Cargar a STG
            # -------------------------------------------------------
            logging.info('Cargando %s registros a BigQuery STG...', len(df_push))
            inicio_carga = time.time()

            job_config = bigquery.LoadJobConfig(
                write_disposition='WRITE_APPEND', schema=schema)

            job = bq_client.load_table_from_dataframe(
                df_push, STG_TABLE_ID, job_config=job_config)

            job.result()
            fin_carga = time.time()
            logging.info(
                'Carga STG finalizada. '
                'Registros cargados: %s. '
                'Tiempo: %.2f segundos',
                len(df_push), fin_carga - inicio_carga)

        except Exception:
            logging.exception('Error procesando %s', formato)

        finally:
            # -------------------------------------------------------
            # Cerrar conexiones si todavía están abiertas
            # -------------------------------------------------------
            if sftp is not None:
                try:
                    sftp.close()
                except Exception:  # noqa: BLE001
                    logging.warning(
                        'No fue posible cerrar SFTP correctamente.')

            if transport is not None:
                try:
                    transport.close()
                except Exception:  # noqa: BLE001
                    logging.warning(
                        'No fue posible cerrar Transport correctamente.')

            logging.info('Proceso finalizado para %s', formato)

    logging.info('=' * 80)
    logging.info('PROCESO SFTP -> STG FINALIZADO')
    logging.info('=' * 80)

    # ---------------------------------------------------------
    # VALIDACIÓN DE LLAVES EN STG
    # ---------------------------------------------------------
    validar_duplicados_stg(client=bq_client, stg_table=table_ref)

    # ---------------------------------------------------------
    # MERGE STG → TABLA FINAL
    # ---------------------------------------------------------
    ejecutar_merge_bq(client=bq_client,
        stg_table=table_ref, final_table=FINAL_TABLE_ID)

    # ---------------------------------------------------------
    # VALIDACIÓN FINAL
    # ---------------------------------------------------------
    validar_carga_final(client=bq_client,
        stg_table=table_ref, final_table=FINAL_TABLE_ID)

# ---------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------
if __name__ == '__main__':
    main()
