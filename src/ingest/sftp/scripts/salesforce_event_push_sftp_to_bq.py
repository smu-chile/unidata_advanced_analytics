# ---------------------------------------------------------------------
# Imports
# ---------------------------------------------------------------------
import csv
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
def main():
    today = datetime.date.today()  # noqa: DTZ011
    logging.info('Obteniendo credenciales SFTP...')
    sftp_secret = secretsmanager.getSecret(
        'salesforce_sftp_credentials',
        project='cl-bigdata-analytics-prod')

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

        try:
            logging.info('Conectando SFTP...')
            transport = paramiko.Transport((host, port))
            transport.connect(username=user, password=password)
            sftp = paramiko.SFTPClient.from_transport(transport)
            logging.info('Conexión exitosa.')

            # ------------------------------------------------------
            # Validar existencia archivo
            # ------------------------------------------------------
            try: sftp.stat(remote_file)

            except FileNotFoundError:
                logging.warning(
                    'Archivo %s no existe para %s. Se continúa.',
                    REMOTE_FILE, formato)
                continue
            logging.info('Archivo encontrado.')

            logging.info('Leyendo primeras 10 filas desde SFTP...')
            with sftp.open(remote_file, 'r') as f:

                reader = csv.DictReader(f)
                registros = []
                for row in reader:
                    registros.append(row)  # noqa: PERF402

            if not registros:
                logging.info(
                    "El archivo '%s' no contiene registros. Se omite la carga.",
                    formato)
                continue

            df = pd.DataFrame(registros)

            df.columns = [
                str(col).replace('\ufeff', '').strip()
                for col in df.columns]

            # ------------------------------------------------------
            # Eliminar columna no existente en BigQuery
            # ------------------------------------------------------
            if 'ServiceResponse' in df.columns:
                df.drop(columns=['ServiceResponse'], inplace=True)  # noqa: PD002

            # ------------------------------------------------------
            # Renombrar columnas al esquema BigQuery
            # ------------------------------------------------------
            df.rename(columns={
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
            df['BUSINESS_UNIT'] = cfg['business_unit']
            df['EVENT_DATE'] = today
            df['CLIENT_ID'] = cfg['client_id']
            df['EID'] = EID
            df['FECHA_CARGA'] = today

            # ------------------------------------------------------
            # Conversión tipos de datos
            # ------------------------------------------------------
            df['DATETIME_SEND'] = pd.to_datetime(
                df['DATETIME_SEND'],
                format='%m/%d/%Y %I:%M:%S %p', errors='coerce')

            df['OPEN_DATE'] = pd.to_datetime(
                df['OPEN_DATE'],
                format='%m/%d/%Y %I:%M:%S %p', errors='coerce')

            df['MESSAGE_ID'] = pd.to_numeric(
                df['MESSAGE_ID'], errors='coerce').astype('Int64')

            df['CLIENT_ID'] = pd.to_numeric(
                df['CLIENT_ID'], errors='coerce').astype('Int64')

            df['EID'] = int(EID)

            # ------------------------------------------------------
            # Crear llave única
            # ------------------------------------------------------
            df['KEY'] = (df['BUSINESS_UNIT']
                + '|' + df['REQUEST_ID'] + '|' + df['DEVICE_ID'])

            logging.info('Registros leídos : %s', len(df))
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

            df = df[~df['KEY'].isin(existentes['KEY'])].copy()

            logging.info('Registros nuevos : %s', len(df))

            if df.empty:
                logging.info('No existen registros nuevos.')

                continue

            df.drop(columns=['KEY'], inplace=True)  # noqa: PD002

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
                df, TABLE_ID, job_config=job_config)

            job.result()

            logging.info(f'Registros cargados: {len(df):,}')

        except FileNotFoundError:
            logging.info(f'No existe archivo para {formato}.')

        except Exception:
            logging.exception(f'Error procesando %s', formato)  # noqa: F541

        finally:
            if sftp is not None:
                sftp.close()

            if transport is not None:
                transport.close()

            logging.info(
                f'Proceso finalizado para {formato}'
            )

    logging.info('=' * 70)
    logging.info('PROCESO FINALIZADO CORRECTAMENTE')
    logging.info('=' * 70)


# ---------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------
if __name__ == '__main__':
    main()
