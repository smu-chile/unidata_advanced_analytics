# Default
from __future__ import annotations

import os

# Pip
import re
import logging
import argparse
import posixpath
from logging import config

import pendulum  # noqa: F401
from google.cloud import bigquery  # noqa: F401
from google.cloud.bigquery import Client

# Own
import common.gcp_extended.bigquery as gbq_extended  # noqa: F401
import common.gcp_extended.secretsmanager as secretmanager
import common.office365_extended.sharepoint as sp
from common.constants import LOGGING_CONFIG
from common.gcp_extended.bigquery import uploadFrame


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
# Main function
# -------------------------------------------------------------------------

def main():
    usuario = 'halo_efect'  # noqa: F841
    # parse input variables
    args = vars(parser.parse_args())
    execution_date: str = args['execution_date']
    proyecto: str = args['project_id']
    logging.info(f'execution_date: {execution_date}')

    gbq_client = Client()

    secret_name = 'bdaa_sharepoint_credentials'  # noqa: S105

    file_site = '/sites/BigDatayAdvancedAnalytics/Documentos compartidos/'
    file_site += 'Unipay/PRUEBAS'

    #file_site = '/sites/BigDatayAdvancedAnalytics/Documentos compartidos/'  # noqa: ERA001, W505
    #file_site += 'Evaluacion Promociones'  # noqa: ERA001

    sp_cred = secretmanager.getSecret(secret_name, project=proyecto)

    sp_folder = sp.SharePointFolder(
        **sp_cred,
        server_relative_folder=file_site
    )

    archivos = sp_folder.fileList()

    patron = re.compile(r'promotions_to_evaluate\.xlsx$', re.IGNORECASE)

    archivo_valido = sorted([
            n for n in archivos
            if n.lower().endswith('.xlsx') and not n.startswith('~$') and patron.match(n)
    ])

    logging.info(f'Archivo con las promociones a evaluar: {archivo_valido[0]}')

    input_file_path = posixpath.join(file_site, archivo_valido[0])
    sharepoint_in = sp.SharePointFile(
        **sp_cred,
        server_relative_path=input_file_path
    )

    promociones = sharepoint_in.toFrame()
    promociones[['Formato', 'n_promocion_ppal']] = promociones[
        ['Formato', 'n_promocion_ppal']
    ].ffill()
    promociones['n_promocion_ppal'] = promociones['n_promocion_ppal'].astype('int64')
    promociones = promociones[promociones['Formato'].isin(['Unimarc', 'M10'])]

    uploadFrame(
    promociones[['Formato','n_promocion_ppal','n_promocion']],
    table_ddl_json_path=os.path.join('gbq_objects','get_promotions_from_sharepoint.json'),
    project=proyecto,
    gbq_client=gbq_client,
    if_exists='replace'
    )

if __name__ == '__main__':
    main()
