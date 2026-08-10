import os  # noqa: D100
import csv
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
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s')


# ---------------------------------------------------------------------
# Configuración
# ---------------------------------------------------------------------
PROJECT_ID = 'cl-bigdata-analytics-prod'
DATASET = 'CRM'
TABLE = 'CRM_DATA_SF_PUSH_EVENT'

TABLE_ID = f'{PROJECT_ID}.{DATASET}.{TABLE}'

REMOTE_PATH = '/Import'
REMOTE_FILE = 'MobilePushDetailExtractReport.csv'

EID = 546001831

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
    today = datetime.date.today()  # noqa: DTZ011
    logging.info('Obteniendo credenciales SFTP...')
    sftp_secret = secretsmanager.getSecret(
        'salesforce_sftp_credentials', project='cl-bigdata-analytics-prod')

    bq_client = bigquery.Client(project=PROJECT_ID)

    # ===============================================================
    # Procesar ambos formatos
    # ===============================================================
    for formato, cfg in FORMATOS.items():

        logging.info('=' * 80)
        logging.info(f'Formato : {formato.upper()}')

        host = sftp_secret['host']
        port = int(sftp_secret['port'])
        user = sftp_secret[f'user_{formato}']
        password = sftp_secret[f'pass_{formato}']

        remote_file = f'{REMOTE_PATH}/{REMOTE_FILE}'

        transport = None
        sftp = None
        local_file = None

        try:
            # 1. Conectar SFTP
            logging.info('Conectando SFTP...')
            transport = paramiko.Transport((host, port))
            transport.connect(username=user, password=password)
            sftp = paramiko.SFTPClient.from_transport(transport)
            logging.info('Conexión exitosa.')

            # 2. Validar existencia archivo
            try: sftp.stat(remote_file)

            except FileNotFoundError:
                logging.warning(
                    'Archivo %s no existe para %s. Se continúa.',
                    remote_file, formato)
                continue

            logging.info(f'Archivo encontrado: {remote_file}')

            # 3. Descargar archivo completo
            nombre_archivo = os.path.basename(remote_file)
            local_file = os.path.join(
                os.environ.get('TEMP', '.'), nombre_archivo)

            logging.info(f'Descargando archivo SFTP a local: {local_file}')

            inicio_descarga = time.time()

            sftp.get(remote_file, local_file)

            fin_descarga = time.time()

            logging.info(
                f'Descarga finalizada. '
                f'Tiempo: {fin_descarga - inicio_descarga:.2f} segundos')

            # 4. Leer CSV local
            logging.info('Leyendo CSV completo desde archivo local...')
            inicio_lectura = time.time()
            with open(local_file, 'r', encoding='utf-8-sig', newline='') as f:  # noqa: UP015
                reader = csv.DictReader(f)
                registros = list(reader)

            df_push = pd.DataFrame(registros)

            fin_lectura = time.time()
            logging.info(
                f'CSV leído completamente. '
                f'Registros: {len(df_push)}. '
                f'Tiempo: {fin_lectura - inicio_lectura:.2f} segundos')

            # 5. Archivo vacío
            if df_push.empty:
                logging.info(
                    f'El archivo {formato} no contiene ningún registro. '
                    f'Se omite el procesamiento.')
                continue

            # 6. Normalización de columnas
            df_push.columns = [
                str(col).replace('\ufeff', '').strip()
                for col in df_push.columns]

            # ------------------------------------------------------
            # Eliminar columna no existente en BigQuery
            # ------------------------------------------------------
            if 'ServiceResponse' in df_push.columns:
                df_push.drop(columns=['ServiceResponse'], inplace=True)  # noqa: PD002

            # ------------------------------------------------------
            # Renombrar columnas al esquema BigQuery
            # ------------------------------------------------------
            df_push.rename(columns={
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
                'RequestId': 'REQUEST_ID'}, inplace=True)  # noqa: PD002

            # ------------------------------------------------------
            # Agregar columnas requeridas
            # ------------------------------------------------------
            df_push['BUSINESS_UNIT'] = cfg['business_unit']
            df_push['EVENT_DATE'] = today
            df_push['CLIENT_ID'] = cfg['client_id']
            df_push['EID'] = EID
            df_push['FECHA_CARGA'] = today

            # ------------------------------------------------------
            # Conversión tipos de datos
            # ------------------------------------------------------
            df_push['DATETIME_SEND'] = pd.to_datetime(
                df_push['DATETIME_SEND'],
                format='%m/%d/%Y %I:%M:%S %p', errors='coerce')

            df_push['OPEN_DATE'] = pd.to_datetime(
                df_push['OPEN_DATE'],
                format='%m/%d/%Y %I:%M:%S %p', errors='coerce')

            df_push['MESSAGE_ID'] = pd.to_numeric(
                df_push['MESSAGE_ID'], errors='coerce').astype('Int64')

            df_push['CLIENT_ID'] = pd.to_numeric(
                df_push['CLIENT_ID'], errors='coerce').astype('Int64')

            df_push['EID'] = int(EID)

            # ------------------------------------------------------
            # Crear llave única
            # ------------------------------------------------------
            df_push['KEY'] = (df_push['BUSINESS_UNIT']
                + '|' + df_push['REQUEST_ID'] + '|' + df_push['DEVICE_ID'])

            logging.info('Registros leídos : %s', len(df_push))
            logging.info('Consultando llaves existentes en BigQuery...')

            sql = f"""
            SELECT CONCAT(
                BUSINESS_UNIT, '|', REQUEST_ID, '|', DEVICE_ID) AS KEY
            FROM `{PROJECT_ID}.{DATASET}.{TABLE}`
            WHERE BUSINESS_UNIT = '{cfg["business_unit"]}'
            AND DATETIME_SEND >= DATE_SUB(CURRENT_DATE(), INTERVAL 120 DAY)
            """  # noqa: S608

            existentes = bq_client.query(sql).to_dataframe(
    create_bqstorage_client=False)

            logging.info('Llaves existentes : %s', len(existentes))

            df_push = df_push[~df_push['KEY'].isin(existentes['KEY'])].copy()

            logging.info('Registros nuevos : %s', len(df_push))

            if df_push.empty:
                logging.info('No existen registros nuevos.')

                continue

            df_push.drop(columns=['KEY'], inplace=True)  # noqa: PD002

            # ------------------------------------------------------
            # Job Config
            # ------------------------------------------------------
            job_config = bigquery.LoadJobConfig(
                write_disposition='WRITE_APPEND')

            # ------------------------------------------------------
            # Cargar BigQuery
            # ------------------------------------------------------
            logging.info('Cargando registros a BigQuery...')

            job = bq_client.load_table_from_dataframe(
                df_push, TABLE_ID, job_config=job_config)

            job.result()

            logging.info(f'Registros cargados: {len(df_push):,}')

        except FileNotFoundError:
            logging.info(f'No existe archivo para {formato}.')

        except Exception:
            logging.exception(f'Error procesando %s', formato)  # noqa: F541

        finally:
            if sftp is not None:
                sftp.close()

            if transport is not None:
                transport.close()

            if os.path.exists(local_file):
                os.remove(local_file)
                logging.info(f'Archivo temporal eliminado: {local_file}')

            logging.info(f'Proceso finalizado para {formato}')

    logging.info('=' * 70)
    logging.info('PROCESO FINALIZADO CORRECTAMENTE')
    logging.info('=' * 70)


# ---------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------
if __name__ == '__main__':
    main()
