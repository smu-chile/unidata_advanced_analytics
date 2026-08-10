import os  # noqa: D100
import csv
import json
import time
import logging
import datetime

import pandas as pd
import paramiko
from google.cloud import bigquery

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

# ---------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------
def main():  # noqa: ANN201, D103

    schema_file = os.path.join(
    os.path.dirname(os.path.dirname(__file__)),
    'gbq_objects', 'CRM_DATA_SF_PUSH_EVENT_STG.json')

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

    # ---------------------------------------------------------------
    # Limpiar tabla STG una sola vez al inicio
    # ---------------------------------------------------------------
    logging.info('Limpiando tabla STG antes de iniciar la carga...')
    truncate_sql = f"""TRUNCATE TABLE `{STG_TABLE_ID}`"""
    bq_client.query(truncate_sql).result()
    logging.info('Tabla STG limpiada correctamente.')

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
            logging.info('Conectando SFTP...')

            transport = paramiko.Transport((host, port))
            transport.connect(username=user, password=password)
            sftp = paramiko.SFTPClient.from_transport(transport)
            logging.info('Conexión exitosa.')

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

            logging.info('Columnas originales: %s', list(df_push.columns))

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
            logging.info('Lectura finalizada. Cerrando conexión SFTP...')

            sftp.close()
            sftp = None

            transport.close()
            transport = None

            logging.info('Conexión SFTP cerrada correctamente.')

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


# ---------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------
if __name__ == '__main__':
    main()
