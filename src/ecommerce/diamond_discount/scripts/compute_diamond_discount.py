# Default
import logging
import argparse
from logging import config

# pip
import awswrangler as wr
from boto3 import Session
from google.cloud.bigquery import Client

# Own
from common.constants import LOGGING_CONFIG
from common.databases.queries import QueryDict
from common.gcp_extended.bigquery import readBigQuery
from common.gcp_extended.secretsmanager import getSecret


# -------------------------------------------------------------------------
# Config
# -------------------------------------------------------------------------
# Logging config
config.dictConfig(LOGGING_CONFIG)
# Parser config
parser = argparse.ArgumentParser()
parser.add_argument(
    '--project_name', type=str, required=True,
    help='Name fo the Advanced Analytics project executed'
)
parser.add_argument(
    '--gcp_project', type=str, required=True,
    help='Name of the GCP project billed. Used to differenciate dev from prod'
)
parser.add_argument(
    '--execution_date', type=str, required=True,
    help='DAG execution date'
)


# -------------------------------------------------------------------------
# SQL Queries
# -------------------------------------------------------------------------
SQL_QUERIES = QueryDict({
    'discount_table':
    """
    SELECT
        HASH_STRING,
        DATE(ITM_TXN_TMS) AS TRANSACTION_DATE,
        MARKET_BASKET_KEY,
        STORE_ID,
        SUM(ITM_TXN_AMT) AS BASKET_VALUE,
        SUM(COALESCE(DIAMOND_ITM_TXN_DSC.DCN_AMT, 0)) AS BASKET_DISCOUNT

    FROM (
        SELECT *
        FROM `cl-cda-prod.DS_CDA_VW_SMU.DW_VW_FACT_ITM_TXN`
        WHERE DATE(ITM_TXN_TMS) = DATE('${execution_date}')
            AND ITM_TXN_AMT>0
            AND ITM_TXN_FCN_TP_DSC = 'V'
    ) ITM_TXN

    LEFT JOIN (
        SELECT
            ITEM_TRANSACTION_KEY,
            DATE_KEY,
            DCN_AMT

        FROM `cl-cda-prod.DS_CDA_VW_SMU.DW_VW_FACT_ITEM_TRANSACTION_DISCT` ITM_TXN_DSC

        INNER JOIN `cl-cda-prod.DS_CDA_VW_SMU.DW_VW_DIM_PROMOTIONAL_GROUP` DIM_PROMOTIONAL_GROUP
            ON ITM_TXN_DSC.PROM_CODE_KEY = DIM_PROMOTIONAL_GROUP.PROM_GROUP_KEY

        INNER JOIN (
            SELECT ID, NAME, DESCRIPTION
            FROM `cl-cda-prod.DS_CDA_VW_SMU.DW_VW_FACT_GP_PROMOTIONS`
            WHERE
                STARTDATE <= DATE('${execution_date}')
                AND DATE('${execution_date}') <= ENDDATE
                AND NAME = 'Socio Diamante'
            GROUP BY 1,2,3
        ) GP_PROMOTIONS
            ON DIM_PROMOTIONAL_GROUP.PROM_GROUP_ID = GP_PROMOTIONS.ID
    ) DIAMOND_ITM_TXN_DSC
        ON ITM_TXN.DATE_KEY = DIAMOND_ITM_TXN_DSC.DATE_KEY
        AND ITM_TXN.ITEM_TRANSACTION_KEY = DIAMOND_ITM_TXN_DSC.ITEM_TRANSACTION_KEY

    INNER JOIN `cl-cda-prod.DS_CDA_VW_SMU.DW_VW_DIM_STORE_HIERARCHY` DIM_STORE
    USING (STORE_KEY)

    INNER JOIN (
        SELECT
            hash_string,
            customer_key,
        FROM `cl-cda-unidata-prod.DS_PROD_CLIENTES_IC.CL_HASH` CLHASH
        INNER JOIN `cl-cda-unidata-prod.DS_PROD_CLIENTES_IC.VW_CDA_CST_DEID` DEID
        ON CLHASH.CUSTOMER_ID = DEID.PDA_CUSTOMER_KEY
        INNER JOIN (
            SELECT DISTINCT rut
            FROM `cl-bigdata-analytics-preprod.ECOMMERCE.MARKET_DIAMOND_MEMBERSHIPS`
            WHERE CURRENT_DATE('America/Santiago')
            BETWEEN fecha_inicio AND fecha_fin
        ) YESTERDAY_DIAMOND_CUSTOMERS
        ON LTRIM(id_card_no, '0') = rut
    ) DIM_HASH
        ON ITM_TXN.CUSTOMER_KEY = DIM_HASH.CUSTOMER_KEY

    GROUP BY 1,2,3,4
    """
})


# -------------------------------------------------------------------------
# Main function
# -------------------------------------------------------------------------
def main() -> None:  # noqa: D103
    args = vars(parser.parse_args())
    # Parsed variables
    user = args['project_name'] + '_diamond_discount'
    gcp_project: str = args['gcp_project']
    execution_date: str = args['execution_date']
    logging.info(f'execution_date: {execution_date}')

    gbq_client = Client()

    logging.info('Getting discount table')
    discount_table = readBigQuery(
        query=SQL_QUERIES['discount_table'].substitute(
            execution_date=execution_date
        ),
        user=user,
        gbq_client=gbq_client,
    )

    logging.info('Sneak peek of the table ;)')
    print(discount_table.head(5))

    logging.info('Uploading table to ecommerce S3 bucket')
    wr.s3.to_csv(
        df=discount_table,
        path=(
            's3://s3-bi-ecommerce-prod/'
            'membresia_diamante_venta_tienda_fisica/'
            'discount_data/'
            f"discount_data_{execution_date.replace('-', '')}"
        ),
        header=False,
        index=False,
        boto3_session=Session(**getSecret(
            secret_name='ecommerce_aws_credentials',  # noqa: S106
            project=gcp_project,
        )),
        )


if __name__ == '__main__':
    main()
