# Default
import os
import logging
import argparse
import platform
from logging import config

import boto3

# pip
from google.cloud.bigquery import Client

from common.gcp_extended.secretsmanager import getSecret


# Local testing support
if 'windows' in platform.platform().lower():
    import sys
    sys.path.append(os.path.join(os.path.abspath(__file__), '..', '..', '..'))
# Own
from common.constants import LOGGING_CONFIG
from common.databases.queries import QueryDict
from common.aws_extended.athena import moveDataframeToS3
from common.gcp_extended.bigquery import readBigQuery


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
    '--partition_month', type=str,
    help='AWS table partition month'
)

parser.add_argument(
    '--month_id', type=str,
    help='AWS Table partition month YYYYmm'
)
parser.add_argument(
    '--partition_month_actual', type=str,
    help='AWS table partition month'
)

parser.add_argument(
    '--month_id_actual', type=str,
    help='AWS Table partition month YYYYmm'
)

parser.add_argument(
    '--week_id', type=str,
    help='AWS Table partition week YYYYVV'
)


# -------------------------------------------------------------------------
#  Config
# -------------------------------------------------------------------------
SQL_QUERIES = QueryDict({
    'last_week_customer_organization_shabits': """
    SELECT
        ORGANIZATION_ID,
        WEEK_ISO_ID,
        PDA_CUSTOMER_KEY as CUSTOMER_ID,
        SHABITS,
        UPDATE_MONTH AS CALENDAR_MONTH_NUMBER
    FROM cl-bigdata-analytics-preprod.CDA_VISTAS.VW_FACT_LAST_WEEK_CUSTOMER_ORGANIZATION_SHABITS sh
    left join cl-cda-unidata-prod.DS_PROD_CLIENTES_IC.VW_CDA_CST_DEID id
    on id.customer_key = sh.customer_id
    WHERE WEEK_ISO_ID = ${partition_id}
    """,
    'customer_alvi_type':"""
    SELECT
             MONTH_ID
           , PDA_CUSTOMER_KEY as CUSTOMER_ID
           , CUSTOMER_TYPE
           , CUSTOMER_TYPE_DET

    FROM cl-bigdata-analytics-preprod.CDA_VISTAS.VW_FACT_MONTH_CUSTOMER_ALVI_TYPE
    left join cl-cda-unidata-prod.DS_PROD_CLIENTES_IC.VW_CDA_CST_DEID id using(customer_key)
    WHERE MONTH_ID = ${partition_id}
    """,
    'customer_organization_outlier' :"""
    SELECT
    ORG_IP_ID AS ORGANIZATION_ID,
    MONTH_ID,
    PDA_CUSTOMER_KEY as CUSTOMER_ID,
    FL_OUTLIER
    FROM cl-bigdata-analytics-preprod.CDA_VISTAS.VW_FACT_MONTH_CUSTOMER_ORGANIZATION_OUTLIER
    left join cl-cda-unidata-prod.DS_PROD_CLIENTES_IC.VW_CDA_CST_DEID id using(customer_key)
    WHERE MONTH_ID = ${partition_id}
    """,
    'customer_organization_profile_vf' : """
    SELECT
        ORGANIZATION_ID
        ,MONTH_ID
        ,PDA_CUSTOMER_KEY AS CUSTOMER_ID
        ,RUBRO
    FROM cl-bigdata-analytics-preprod.CDA_VISTAS.VW_FACT_MONTH_CUSTOMER_ORGANIZATION_PROFILE_VF vf
    left join cl-cda-unidata-prod.DS_PROD_CLIENTES_IC.VW_CDA_CST_DEID id
    on id.customer_key = vf.customer_id
    WHERE MONTH_ID = ${partition_id}""",
    'customer_organization_shabits_unidata_alvi': """
    SELECT
        ORG_IP_ID
        ,MONTHID
        ,PDA_CUSTOMER_KEY AS CUSTOMER_KEY
        ,CUSTOMER_TYPE_DET
        ,SHABIT
        ,NIVEL
        ,TIPO_META
        ,META_BOLETAS
        ,META_GASTO
        ,NIVEL_ANTERIOR
        ,FECHA_ACTUALIZACION
        ,NIVEL_INFORMADO
        ,CUSTOMER_TYPE_INFORMADO
        ,CONSECUTIVE_VIPS_COUNTS
    FROM
    cl-bigdata-analytics-preprod.CDA_VISTAS.VW_FACT_MONTH_CUSTOMER_ORGANIZATION_SHABITS_UNIDATA_ALVI
    left join cl-cda-unidata-prod.DS_PROD_CLIENTES_IC.VW_CDA_CST_DEID id using(customer_key)
    WHERE MONTHID = ${partition_id}
    """,
    'customer_organization_shabits_unidata' : """
    SELECT
        ORG_IP_ID
        ,MONTHID
        ,PDA_CUSTOMER_KEY AS CUSTOMER_KEY
        ,SHABIT
        ,NIVEL
        ,TIPO_META
        ,META_BOLETAS
        ,META_GASTO
        ,NIVEL_ANTERIOR
        ,FECHA_ACTUALIZACION
        ,NIVEL_INFORMADO
        ,PREAPROBADOS
        ,TARJETA_HABIENTE
    FROM
    cl-bigdata-analytics-preprod.CDA_VISTAS.VW_FACT_MONTH_CUSTOMER_ORGANIZATION_SHABITS_UNIDATA
    left join cl-cda-unidata-prod.DS_PROD_CLIENTES_IC.VW_CDA_CST_DEID id using(customer_key)
    WHERE MONTHID = ${partition_id}
    """,
    'customer_organization_tyc' : """
    SELECT
        FORMATO
        ,PDA_CUSTOMER_KEY as CUSTOMER_ID
        ,CAST(MONTH_ID AS INT64) AS MONTH_ID
        ,FECHA_TYC
        ,MEDIO
        ,TYC
    FROM cl-bigdata-analytics-preprod.CDA_VISTAS.VW_FACT_MONTH_CUSTOMER_ORGANIZATION_TYC
    left join cl-cda-unidata-prod.DS_PROD_CLIENTES_IC.VW_CDA_CST_DEID id using(customer_key)
    WHERE MONTH_ID = '${partition_id}'
    """
})

# -------------------------------------------------------------------------
# Main function
# -------------------------------------------------------------------------
def main() -> None:  # noqa: D103
    # Parameters
    args = vars(parser.parse_args())
    gcp_project_id: str = args['project_id']
    partition_month: str = args['partition_month']
    month_id: str = args['month_id']
    partition_month_actual: str = args['partition_month_actual']
    month_id_actual: str = args['month_id_actual']
    week_id: str =  args['week_id']

    landing_bucket = 'smu-datalake-test-landing'
    landing_path = 'views/datascience'

    boto3_session=boto3.Session(
                **getSecret(
                    project=gcp_project_id,
                    secret_name='bdaa_aws_credentials'  # noqa: S106
                )
            )
    # Load data from SharePoint to pandas DataFrame

    fact_list = ['last_week_customer_organization_shabits',
                 'customer_alvi_type',
                 'customer_organization_outlier',
                 'customer_organization_profile_vf',
                 'customer_organization_shabits_unidata_alvi',
                 'customer_organization_shabits_unidata',
                 'customer_organization_tyc']
    column_types = {
        'last_week_customer_organization_shabits' : ['Int64','Int64','Int64', 'str','Int64'],
        'customer_alvi_type' : ['Int64', 'Int64', 'str','str'],
        'customer_organization_outlier' : ['Int64','Int64', 'Int64', 'str'],
        'customer_organization_profile_vf' : ['Int64','Int64', 'Int64','str'],
        'customer_organization_shabits_unidata_alvi' :  ['str','str', 'Int64','str', 'str','str',
                                                         'str','Int64','float','str','str','str',
                                                         'str', 'Int32'],
        'customer_organization_shabits_unidata' :  ['str','str', 'Int64', 'str','str','str',
                                                    'Int64','float','str','str','str','Int64',
                                                    'Int64'],
        'customer_organization_tyc' :  ['str','Int64', 'int','datetime64[ns]', 'str','int'],


    }
    table_names = {
        'last_week_customer_organization_shabits' :
            'TMP_LAB_SMU_FACT_LAST_WEEK_CUSTOMER_ORGANIZATION_SHABITS_GCP',
        'customer_alvi_type' : 'TMP_LAB_SMU_FACT_MONTH_CUSTOMER_ALVI_TYPE_GCP',
        'customer_organization_outlier' :
            'TMP_LAB_SMU_FACT_MONTH_CUSTOMER_ORGANIZATION_OUTLIER_GCP',
        'customer_organization_profile_vf' :
            'TMP_LAB_SMU_FACT_MONTH_CUSTOMER_ORGANIZATION_PROFILE_VF_GCP',
        'customer_organization_shabits_unidata_alvi' :
            'TMP_LAB_SMU_FACT_MONTH_CUSTOMER_ORGANIZATION_SHABITS_UNIDATA_ALVI_GCP',
        'customer_organization_shabits_unidata' :
            'TMP_LAB_SMU_FACT_MONTH_CUSTOMER_ORGANIZATION_SHABITS_UNIDATA_GCP',
        'customer_organization_tyc' : 'TMP_LAB_SMU_FACT_MONTH_CUSTOMER_ORGANIZATION_TYC_GCP'
    }


    partition_id = month_id
    partition_value = partition_month
    for fact_table in fact_list :
        if fact_table in ('last_week_customer_organization_shabits'):
            partition_id = week_id
            partition_value = week_id
        if fact_table in ('customer_organization_outlier'):
            partition_id = month_id_actual
            partition_value = partition_month_actual
        logging.info(f'Load the file {fact_table} to DataFrame')

        df_fact = readBigQuery(
            query=SQL_QUERIES[fact_table].substitute(
                partition_id = partition_id
            ),
            user='csotob',
            gbq_client = Client()
        )

        logging.info(f'UpLoad the file {fact_table} to S3')

        moveDataframeToS3(df_file=df_fact,
                        landing_bucket=landing_bucket,
                        landing_path=landing_path,
                        table_name=table_names[fact_table],
                        partition_value=partition_value,
                        column_types=column_types[fact_table],
                        boto3_session=boto3_session
                        )

        logging.info(f'File successfully uploaded to: {landing_bucket}/{landing_path}')



if __name__ == '__main__':
    main()
