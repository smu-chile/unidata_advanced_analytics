# =========================================================
# IMPORTS
# =========================================================

import logging
import pandas as pd
from google.cloud.bigquery import Client

from common.databases.queries import QueryDict

from common.gcp_extended.bigquery import (
    readBigQuery,
    uploadFrame
)

# =========================================================
# LOGGING CONFIG
# =========================================================

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

# =========================================================
# BIGQUERY CLIENT
# =========================================================

gbq_client = Client(
    project='cl-cda-unidata-dev'
)

# =========================================================
# SQL QUERIES
# =========================================================

SQL_QUERIES = QueryDict({

    'product':

    """
    SELECT *
    FROM `cl-bigdata-analytics.CDA_VISTAS.VW_DIM_PRODUCT`
    """

})

# =========================================================
# MAIN FUNCTION
# =========================================================

def main():

    try:

        # =================================================
        # INICIO PIPELINE
        # =================================================

        logging.info("Iniciando pipeline CUSTOMER")

        # =================================================
        # LEER DATOS DESDE BIGQUERY
        # =================================================

        logging.info("Leyendo datos desde BigQuery")

        df_product = readBigQuery(
            SQL_QUERIES['product'].substitute(),
            user='ilopeze',
            gbq_client=gbq_client
        )

        # =================================================
        # VALIDACIONES
        # =================================================

        logging.info(
            f"Cantidad registros: {len(df_product)}"
        )

        logging.info(
            f"Cantidad columnas: {len(df_product.columns)}"
        )

        logging.info(
            f"Duplicados: {df_product.duplicated().sum()}"
        )

        logging.info(
            "Valores nulos por columna:"
        )

        logging.info(
            df_product.isnull().sum()
        )

        # =================================================
        # LIMPIEZA
        # =================================================

        logging.info("Aplicando limpieza básica")

        # nombres columnas lowercase
        df_product.columns = (
            df_product.columns
            .str.lower()
        )

        # eliminar duplicados
        df_product = (
            df_product
            .drop_duplicates()
        )

        # =================================================
        # PREVIEW DATOS
        # =================================================

        logging.info("Preview dataframe")

        logging.info(
            df_product.head()
        )

        # =================================================
        # CARGA BIGQUERY RAW
        # =================================================

        logging.info(
            "Cargando tabla RAW en BigQuery"
        )

        uploadFrame(
            df=df_product,
            table_ddl_json_path='ingest/rds/gbq_objects/dim_product_raw.json',
            project='cl-cda-unidata-dev',
            gbq_client=gbq_client,
            if_exists='replace'
        )

        # =================================================
        # FIN
        # =================================================

        logging.info(
            "Pipeline CUSTOMER finalizado OK"
        )

    except Exception as e:

        logging.error(
            f"Error pipeline CUSTOMER: {e}"
        )

        raise

# =========================================================
# MAIN
# =========================================================

if __name__ == "__main__": main()