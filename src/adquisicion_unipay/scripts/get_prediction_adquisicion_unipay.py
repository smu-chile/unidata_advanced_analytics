# Default
from __future__ import annotations

import io
import os
import re
import logging
import argparse
from logging import config

# Pip
import numpy as np  # noqa: F401
import joblib
import pandas as pd
import pyarrow as pa  # noqa: F401
import xgboost as xgb  # noqa: F401
import pendulum
import pyarrow.parquet as pq  # noqa: F401

# Own
from google.cloud import (
    storage,
    bigquery,  # noqa: F401
)
from google.cloud.bigquery import Client

import common.gcp_extended.bigquery as gbq_extended  # noqa: F401
from common.constants import LOGGING_CONFIG
from common.gcp_extended.bigquery import uploadFrame, deleteFromTable


# -------------------------------------------------------------------------
#  Config
# -------------------------------------------------------------------------
# Logging config
config.dictConfig(LOGGING_CONFIG)
# Parser config
parser = argparse.ArgumentParser()
parser.add_argument(
    '--project_id', type=str,
    help='GCP project in which the script will be executed'
)
parser.add_argument(
    '--execution_date', type=str,
    help='DAG execution date'
)


# -------------------------------------------------------------------------
# Functions
# -------------------------------------------------------------------------

def obtener_ultima_version(bucket_name: str, prefix: str) -> str | None:
    client = storage.Client()
    iterator = client.list_blobs(bucket_or_name=bucket_name, prefix=prefix, delimiter='/')
    _ = list(iterator)

    pattern = re.compile(rf'^{re.escape(prefix)}([Vv]\s*(\d+))/$', re.ASCII)
    candidates = []

    for prefix in iterator.prefixes:
        m = pattern.match(prefix)
        if m:
            num = int(m.group(2))
            folder_name = m.group(1)
            candidates.append((num, folder_name))

    if not candidates:
        return None

    candidates.sort(key=lambda x: x[0])
    return candidates[-1][1]

def cargar_archivos_cloud_storage(bucket_name: str, blob_path: str):
    client = storage.Client()
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(blob_path)

    data = blob.download_as_bytes()
    buf = io.BytesIO(data)
    return joblib.load(buf)

# -------------------------------------------------------------------------
# Main function
# -------------------------------------------------------------------------

def main():
    usuario = 'dataset_preaprobados'  # noqa: F841
    # parse input variables
    args = vars(parser.parse_args())
    execution_date: str = args['execution_date']
    proyecto: str = args['project_id']  # noqa: F841
    logging.info(f'execution_date: {execution_date}')

    # Set gbq client for all subsequent queries
    gbq_client = Client()

    # Variables de tiempo utilizadas en las queries
    fecha = pendulum.parse(execution_date)
    periodo = fecha.add(months=1).strftime('%Y%m')

    # Variables Cloud Storage
    bucket_dataset = 'cl-bigdata-analytics-preprod-us-sandbox-datasets'
    prefix_dataset = 'UNIPAY/ADQUISICION_UNIPAY/DATASETS/'
    bucket_encoder_modelo = 'cl-bigdata-analytics-preprod-us-sandbox-models'
    prefix_modelo = 'UNIPAY/ADQUISICION/'

    # Version del modelo
    version = obtener_ultima_version(
    bucket_name=bucket_encoder_modelo,
    prefix=prefix_modelo
    )

    logging.info(' ')
    logging.info('--------------------')
    logging.info(f'Se inicia el proceso para el periodo: {periodo}')
    logging.info(f'utilizando la version del modelo: {version}')


    logging.info(' ')
    logging.info('--------------------')
    logging.info('Inicia el proceso de carga del dataset, encoders y modelo de cloud storage')
    logging.info('--------------------')

    path = 'gs://'+bucket_dataset+'/'+prefix_dataset
    path += 'DATASET_ADQUISICION_'+str(periodo)+'.parquet'
    dataset = pd.read_parquet(path, engine='pyarrow')

    logging.info(' ')
    logging.info('Se cargo el dataset')

    logging.info(' ')
    logging.info('Inicia el proceso de carga de encoders y el modelo')

    ce_ise = cargar_archivos_cloud_storage(
    bucket_name=bucket_encoder_modelo,
    blob_path='UNIPAY/ADQUISICION/'+str(version)+\
        '/CATBOOST_ENCODER_ISE.pkl'
    )

    ce_gr = cargar_archivos_cloud_storage(
    bucket_name=bucket_encoder_modelo,
    blob_path='UNIPAY/ADQUISICION/'+str(version)+\
        '/CATBOOST_ENCODER_GRUPO_RIESGO.pkl'
    )

    ce_ec = cargar_archivos_cloud_storage(
    bucket_name=bucket_encoder_modelo,
    blob_path='UNIPAY/ADQUISICION/'+str(version)+\
        '/CATBOOST_ENCODER_ESTADO_CIVIL.pkl'
    )

    ce_uni = cargar_archivos_cloud_storage(
    bucket_name=bucket_encoder_modelo,
    blob_path='UNIPAY/ADQUISICION/'+str(version)+\
        '/CATBOOST_ENCODER_SHABIT_UNIMARC.pkl'
    )

    ce_alvi = cargar_archivos_cloud_storage(
    bucket_name=bucket_encoder_modelo,
    blob_path='UNIPAY/ADQUISICION/'+str(version)+\
        '/CATBOOST_ENCODER_SHABIT_ALVI.pkl'
    )

    ce_mpp1 = cargar_archivos_cloud_storage(
    bucket_name=bucket_encoder_modelo,
    blob_path='UNIPAY/ADQUISICION/'+str(version)+\
        '/CATBOOST_ENCODER_MEDIO_PAGO_PREFERIDO_1M.pkl'
    )

    ce_mpp3 = cargar_archivos_cloud_storage(
    bucket_name=bucket_encoder_modelo,
    blob_path='UNIPAY/ADQUISICION/'+str(version)+\
        '/CATBOOST_ENCODER_MEDIO_PAGO_PREFERIDO_3M.pkl'
    )

    ce_mpp6 = cargar_archivos_cloud_storage(
    bucket_name=bucket_encoder_modelo,
    blob_path='UNIPAY/ADQUISICION/'+str(version)+\
        '/CATBOOST_ENCODER_MEDIO_PAGO_PREFERIDO_6M.pkl'
    )

    ce_mpu1 = cargar_archivos_cloud_storage(
    bucket_name=bucket_encoder_modelo,
    blob_path='UNIPAY/ADQUISICION/'+str(version)+\
        '/CATBOOST_ENCODER_MEDIOS_PAGO_USADOS_1M.pkl'
    )

    ce_mpu3 = cargar_archivos_cloud_storage(
    bucket_name=bucket_encoder_modelo,
    blob_path='UNIPAY/ADQUISICION/'+str(version)+\
        '/CATBOOST_ENCODER_MEDIOS_PAGO_USADOS_3M.pkl'
    )

    ce_mpu6 = cargar_archivos_cloud_storage(
    bucket_name=bucket_encoder_modelo,
    blob_path='UNIPAY/ADQUISICION/'+str(version)+\
        '/CATBOOST_ENCODER_MEDIOS_PAGO_USADOS_6M.pkl'
    )

    modelo = cargar_archivos_cloud_storage(
    bucket_name=bucket_encoder_modelo,
    blob_path='UNIPAY/ADQUISICION/'+str(version)+\
        '/XGBOOST_ADQUISICION.joblib'
    )

    logging.info(' ')
    logging.info('Se cargaron los encoders y el modelo')

    logging.info(' ')
    logging.info('--------------------')
    logging.info('Finaliza el proceso de carga del dataset, encoders y modelo de cloud storage')
    logging.info('--------------------')

    logging.info(' ')
    logging.info('--------------------')
    logging.info('Inicia el proceso de ajuste del dataset para realizar la prediccion')
    logging.info('--------------------')

    dataset['ISE'] = ce_ise.transform(dataset['ISE'])
    dataset['GRUPO_RIESGO'] = ce_gr.transform(dataset['GRUPO_RIESGO'])
    dataset['ESTADO_CIVIL'] = ce_ec.transform(dataset['ESTADO_CIVIL'])
    dataset['SHABIT_UNIMARC'] = ce_uni.transform(dataset['SHABIT_UNIMARC'])
    dataset['SHABIT_ALVI'] = ce_alvi.transform(dataset['SHABIT_ALVI'])
    dataset['MEDIO_PAGO_PREFERIDO_1M'] = ce_mpp1.transform(dataset['MEDIO_PAGO_PREFERIDO_1M'])
    dataset['MEDIO_PAGO_PREFERIDO_3M'] = ce_mpp3.transform(dataset['MEDIO_PAGO_PREFERIDO_3M'])
    dataset['MEDIO_PAGO_PREFERIDO_6M'] = ce_mpp6.transform(dataset['MEDIO_PAGO_PREFERIDO_6M'])
    dataset['MEDIOS_PAGO_USADOS_1M'] = ce_mpu1.transform(dataset['MEDIOS_PAGO_USADOS_1M'])
    dataset['MEDIOS_PAGO_USADOS_3M'] = ce_mpu3.transform(dataset['MEDIOS_PAGO_USADOS_3M'])
    dataset['MEDIOS_PAGO_USADOS_6M'] = ce_mpu6.transform(dataset['MEDIOS_PAGO_USADOS_6M'])

    columns = ['N_PREAPROBADO','CUPO',
        'GRUPO_RIESGO','AÑO_NACIMIENTO',
        'SHABIT_ALVI','MEDIOS_PAGO_USADOS_6M',
        'TOT_SALE_AMT_3M','SHABIT_UNIMARC',
        'PROM_DIST_PROMOS_3M','PROM_DIST_PROMOS_1M',
        'CANT_HIJOS','MEDIO_PAGO_PREFERIDO_1M',
        'N_PRODUCTOS_6M','CANT_BBRR_FAM',
        'PROM_DIST_PROMOS_6M','N_TARJETAS',
        'MEDIOS_PAGO_USADOS_3M','PROM_TOT_SALE_AMT_1M',
        'RENTA_HH','MEDIO_PAGO_PREFERIDO_6M',
        'N_VISITAS_3M','N_PRODUCTOS_1M',
        'PROM_TOT_SALE_AMT_3M','ESTADO_CIVIL',
        'PROM_N_PRODUCTOS_6M','MEDIO_PAGO_PREFERIDO_3M',
        'N_PRODUCTOS_3M','MEDIOS_PAGO_USADOS_1M',
        'ISE','PROM_N_PRODUCTOS_3M',
        'PROM_TOT_SALE_AMT_6M'
    ]

    pred_prob = modelo.predict_proba(dataset[columns])[:,1]

    tabla = pd.DataFrame(columns=['CUSTOMER_KEY','PRED_PROB'])
    tabla['CUSTOMER_KEY'] = dataset['CUSTOMER_KEY']
    tabla['PRED_PROB'] = pred_prob

    grupos = tabla.copy()
    bins, bin_edges = pd.qcut(grupos['PRED_PROB'], q=10, labels=False, retbins=True)
    grupos.loc[:, 'GRUPO'] = bins
    grupos.loc[:, 'LIMITE INF'] = [bin_edges[i] for i in bins]
    grupos.loc[:, 'LIMITE SUP'] = [bin_edges[i+1] for i in bins]

    tabla = tabla.merge(grupos[['CUSTOMER_KEY','GRUPO']],on='CUSTOMER_KEY',how='left')
    tabla['GRUPO'] = tabla['GRUPO'].fillna(-1)

    tabla['MONTHID'] = dataset['PERIODO']

    tabla['FECHA'] = (
        pd.to_datetime(tabla['MONTHID'].astype(str) + '01', format='%Y%m%d')
        .dt.strftime('%Y-%m-%d')
    )

    logging.info(' ')
    logging.info('--------------------')
    logging.info('Finaliza el proceso de ajuste del dataset para realizar la prediccion')
    logging.info('--------------------')

    logging.info(' ')
    logging.info(f'Se borra la partición actual de {periodo}')

    deleteFromTable(
    table_ref='cl-bigdata-analytics-preprod.UNIPAY.ADQUISICION_UNIPAY',
    where_clause=f"monthid = '{periodo}'",
    gbq_client=gbq_client,
    )

    logging.info(' ')
    logging.info('Inicia la carga de datos a la tabla de GCP')

    uploadFrame(
    tabla[['CUSTOMER_KEY','PRED_PROB','GRUPO','MONTHID','FECHA']],
    table_ddl_json_path=os.path.join('gbq_objects','adquisicion_unipay.json'),
    project=proyecto,
    gbq_client=gbq_client,
    if_exists='append'
    )

    logging.info(' ')
    logging.info('Finaliza la carga de datos a la tabla de GCP')

if __name__ == '__main__':
    main()
