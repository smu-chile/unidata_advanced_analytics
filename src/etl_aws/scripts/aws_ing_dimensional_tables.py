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
    '--execution_date', type=str,
    help='DAG execution date'
)




# -------------------------------------------------------------------------
#  Config
# -------------------------------------------------------------------------
SQL_QUERIES = {
    'dim_campaign_dh': """
    SELECT
    ORGANIZATION_ID,
    CAMPAIGN_ID,
    CAMPAIGN_NAME,
    START_DATE,
    END_DATE,
    DESCRIPTION,
    LOAD_DATE,
    LOAD_OWNER,
    CAMPAIGN_CHANEL
    FROM cl-bigdata-analytics-preprod.CDA_VISTAS.VW_DIM_CAMPAIGN_DH
    """,
    'dim_date' : """
    SELECT
    TO_BASE64(DATE_KEY),
    DATE_VALUE,
    CALENDAR_DAY_OF_MONTH,
    CALENDAR_DAY_OF_QUARTER,
    CALENDAR_DAY_OF_YEAR,
    WEEKDAY_NUMBER,
    WEEKDAY_NAME_ABBREVIATED,
    WEEKDAY_NAME,
    CALENDAR_MONTH_NUMBER,
    CALENDAR_MONTH_NAME,
    CALENDAR_MONTH_ABBREVIATION,
    QUARTER,
    QUARTER_TXT,
    SEMESTER,
    SEMESTER_TXT,
    CALENDAR_YEAR,
    WEEK_NUMBER,
    TO_BASE64(CALENDAR_YEAR_MONTH_KEY),
    TO_BASE64(CALENDAR_YEAR_WEEK_KEY)
    FROM cl-bigdata-analytics-preprod.CDA_VISTAS.VW_DIM_DATE
    WHERE WEEKDAY_NUMBER is not null""",
    'dim_fin_doc_tp_type' : """
    SELECT
    TO_BASE64(FIN_DOC_TP_KEY),
    FNC_DOC_TP_DSC,
    FNC_DOC_TP_TXT
    FROM cl-bigdata-analytics-preprod.CDA_VISTAS.VW_DIM_FIN_DOC_TP_TYPE""",
    'dim_product': """"
    SELECT
      EAN AS UPC,
      NM AS PRODUCT_DESCRIPTION,
      BRAND_DESC AS BRAND,
      ANCHO,
      LONGITUD,
      ALTURA,
      CONT_CONV_UMB AS SALES_UNIT,
      CONTENIDO_BRUTO AS PESO,
      UM_CONTENIDO AS PESO_UM,
      UNIDAD_DE_MEDIDA AS SALES_UOM,
      INDIC_EAN_PPAL AS INDICADOR_EAN_PRINCIPAL,
      SKU_PRODUCT AS PRODUCT_ID,
      SKU_PRODUCT AS PRODUCT_CODE,
      GRUPO_ID AS SUB_CATEGORY_CODE,
      GRUPO_DSC AS SUB_CATEGORY_DESCRIPTION,
      CAT_ID AS CATEGORY_CODE,
      CAT_DSC AS CATEGORY_DESCRIPTION,
      LIN_ID AS DEPARTMENT_CODE,
      LIN_DESC AS DEPARTMENT_DESCRIPTION,
      SEC_DSC AS OPERATIVE_SECTION_NAME,
      NEG_DSC AS BUSINESS_NAME,
      CAT_H_DSC AS CATEGORY_CODE_H,
      CAT_H_ID AS CATEGORY_DESCRIPTION_H,
      LIN_H_DSC AS DEPARTMENT_CODE_H,
      LIN_H_ID AS DEPARTMENT_DESCRIPTION_H,
      '' AS PRODUCT_AGGREGATE_CODE,
      '' PRODUCT_AGGREGATE_DESCRIPTION,
      SKU_PRODUCT,
      SKU_DESCRIPTION
    FROM cl-bigdata-analytics-preprod.CDA_VISTAS.VW_DIM_PRODUCT
    """,
    'dim_offer_cycle' : """
    SELECT
    CYCLE_ID,
    OFFER_ID,
    OFFER_RANK,
    MAX_UNITS,
    MAX_UNITS_GEO,
    UNIDAD_MEDIDA,
    DISCOUNT_PERC,
    LOYALTY_ID,
    DISCOUNT_ID,
    OFFER_KPI_CODE,
    OFFER_BAR_CODE,
    OFFER_DESC,
    OFFER_MAX_USES,
    OFFER_START_DATE,
    OFFER_START_DATE_GEO,
    OFFER_END_DATE,
    OFFER_END_DATE_GEO,
    TRANSLATE(OFFER_LEGAL, CHR(10), '') AS OFFER_LEGAL,
    OFFER_PRODUCT_LIST,
    OFFER_PROMOTION_TYPE,
    FINANCE_TYPE,
    FINANCING_FLAG,
    OFFER_STOCK,
    OFFER_TYPE_ID,
    CONTENIDO_MECANICA
    FROM cl-bigdata-analytics-preprod.CDA_VISTAS.VW_DIM_OFFER_CYCLE_DH
    """,
    'dim_offer_cycle_sku_ean_dh' : """
    SELECT
    CYCLE_ID,
    OFFER_ID,
    MATERIAL,
    EAN	NATIONAL,
    START_DATE,
    END_DATE
    FROM cl-bigdata-analytics-preprod.CDA_VISTAS.VW_DIM_OFFER_CYCLE_SKU_EAN_DH
    """,
    'dim_organization_homolog' : """
    SELECT
    ORGANIZATION_KEY,
	ORG_IP_ID,
	PRIM_CMRCL_NM,
	ID_FORMATO_BDF
    FROM cl-bigdata-analytics-preprod.CDA_VISTAS.VW_DIM_ORGANIZATION_HOMOLOG""",
    'dim_organization_shabits_dh' : """
    SELECT  ORGANIZATION_ID,
    SEGMENT_ID,
    DH_SHABIT,
    SHABIT
    FROM cl-bigdata-analytics-preprod.CDA_VISTAS.VW_DIM_ORGANIZATION_SHABITS_DH
    """,
    'dim_product_hierarchy' : """
    SELECT
    TO_BASE64(PRODUCT_KEY),
    EAN,
    NM,
    TO_BASE64(SKU_KEY),
    SKU_PRODUCT,
    TO_BASE64(GRUPO_KEY),
    GRUPO_ID,
    GRUPO_DSC,
    TO_BASE64(CATEGORIA_KEY),
    CAT_ID,
    CAT_DSC,
    TO_BASE64(LINEA_KEY),
    LIN_ID,
    LIN_DESC,
    TO_BASE64(SECCION_KEY),
    SEC_ID,
    SEC_DSC,
    TO_BASE64(NEGOCIO_KEY),
    NEG_ID,
    NEG_DSC,
    MARCA_PROPIA,
    BRAND_DESC,
    BRND_ID,
    TO_BASE64(UOM_VTA_KEY),
    PRODUCT_NK
    FROM cl-bigdata-analytics-preprod.CDA_VISTAS.VW_DIM_PRODUCT_HIERARCHY
    """,
    'dim_store' : """
    SELECT
    STORE_ID,
    STORE_NAME,
    TO_BASE64(STORE_CODE),
    STORE_BANNER,
    LATITUDE,
    LONGITUDE
    FROM cl-bigdata-analytics-preprod.CDA_VISTAS.VW_DIM_STORE
    """,
    'dim_store_hierarchy' : """
    SELECT
    TO_BASE64(STORE_KEY),
    TO_BASE64(ORG_IP_KEY),
    ORG_IP_ID,
    ORG_IP,
    TO_BASE64(GERENTE_ZONA_KEY),
    ZONA_OPERACIONES_ID,
    ZONA_OPERACIONES,
    GERENTE_ZONA_ID,
    GERENTE_ZONA,
    TO_BASE64(GERENTE_TIENDA_KEY),
    GERENTE_TIENDA_ID,
    GERENTE_TIENDA,
    STORE_ID,
    STORE,
    CITY_ID,
    CTY_ID,
    COUNTY_DESC,
    STE_ID,
    GRUPO_SOCIOECONOMICO,
    TAMANO,
    SSS_F,
    FLRSP_AREA,
    FLOORSPACE_TYPE,
    NUMERO_CAJAS_POS,
    LISTA_DE_PRECIO
    FROM cl-bigdata-analytics-preprod.CDA_VISTAS.VW_DIM_STORE_HIERARCHY
    """,
    'dim_supplier' : """
    SELECT SUPPLIER_ID,
    MATERIAL,
    SUPPLIER_NM
    FROM cl-bigdata-analytics-preprod.CDA_VISTAS.VW_DIM_SUPPLIER
    """
}

# -------------------------------------------------------------------------
# Main function
# -------------------------------------------------------------------------
def main() -> None:  # noqa: D103
    # Parameters
    args = vars(parser.parse_args())
    gcp_project_id: str = args['project_id']

    landing_bucket = 'smu-datalake-test-landing'
    landing_path = 'views/datascience'
    dimensional_list = ['dim_campaign_dh','dim_date','dim_fin_doc_tp_type',
                        'dim_product','dim_offer_cycle','dim_offer_cycle_sku_ean_dh',
                        'dim_organization_homolog','dim_organization_shabits_dh',
                        'dim_product_hierarchy','dim_store','dim_store_hierarchy',
                        'dim_supplier']

    column_types ={
        'dim_campaign_dh' : ['int', 'int', 'str', 'str', 'str', 'str', 'str', 'str', 'str'],
        'dim_date' :  ['str', 'str', 'Int64', 'Int64', 'Int64', 'int', 'str', 'str', 'Int64',
                       'str', 'str', 'Int64', 'str', 'Int64', 'str', 'Int64', 'int',
                       'Int64', 'Int64'],
        'dim_fin_doc_tp_type' : ['str', 'str', 'str'],
        'dim_product' : ['str', 'str', 'str', 'float', 'float', 'float', 'float',
                         'float', 'str', 'str', 'str', 'str', 'str', 'str', 'str',
                         'str', 'str', 'str', 'str', 'str', 'str', 'str', 'str', 'str',
                         'str', 'str', 'str', 'str', 'str'],
        'dim_offer_cycle' :['int', 'int', 'float', 'float', 'float', 'str', 'float',
                            'str', 'str', 'str', 'float', 'str', 'float', 'str',
                            'str', 'str', 'str', 'str', 'str', 'str', 'str', 'float',
                            'float', 'float', 'str'],
        'dim_offer_cycle_sku_ean_dh' : ['int', 'int', 'str', 'str', 'str', 'str'],
        'dim_organization_homolog' : ['Int64','str', 'str', 'Int64'],
        'dim_organization_shabits_dh' :  ['Int64','Int64', 'str', 'str'],
        'dim_product_hierarchy' : ['str', 'str', 'str', 'str', 'str', 'str',
                                   'str', 'str', 'str', 'str', 'str', 'str',
                                   'str', 'str', 'str', 'str', 'str', 'str',
                                   'str', 'str', 'str', 'str', 'str', 'str', 'str'],
        'dim_store' : ['str', 'str', 'str', 'str', 'str', 'str'],
        'dim_store_hierarchy' :  ['str', 'str', 'str', 'str', 'str',
                                    'str', 'str', 'str', 'str', 'str',
                                    'str', 'str', 'str', 'str', 'str',
                                    'str', 'str', 'str', 'str', 'str',
                                    'str', 'Int64', 'str', 'Int64', 'str'],
        'dim_supplier' : ['str', 'Int64', 'str']
    }

    table_names ={
        'dim_campaign_dh' : 'TMP_LAB_SMU_DIM_CAMPAIGN_GCP',
        'dim_date' : 'TMP_LAB_SMU_DIM_DATE_GCP',
        'dim_fin_doc_tp_type' : 'TMP_LAB_SMU_DIM_FIN_DOC_TP_TYPE_GCP',
        'dim_product' : 'TMP_LAB_SMU_DIM_NEW_PRODUCTS_GCP',
        'dim_offer_cycle' : 'TMP_LAB_SMU_DIM_OFFER_CYCLE_GCP',
        'dim_offer_cycle_sku_ean_dh' : 'TMP_LAB_SMU_DIM_OFFER_CYCLE_SKU_EAN_DH_GCP',
        'dim_organization_homolog' : 'TMP_LAB_SMU_DIM_ORGANIZATION_HOMOLOG_GCP',
        'dim_organization_shabits_dh' : 'TMP_LAB_SMU_DIM_ORGANIZATION_SHABITS_DH_GCP',
        'dim_product_hierarchy' : 'TMP_LAB_SMU_DIM_PRODUCT_HIERARCHY_GCP',
        'dim_store' : 'TMP_LAB_SMU_DIM_STORE_GCP',
        'dim_store_hierarchy' : 'TMP_LAB_SMU_DIM_STORE_HIERARCHY_GCP',
        'dim_supplier' : 'TMP_LAB_SMU_DIM_SUPPLIER_GCP'
    }

    boto3_session=boto3.Session(
            **getSecret(
                project=gcp_project_id,
                secret_name='bdaa_aws_credentials'  # noqa: S106
            )
        )

    for dimensional_table in dimensional_list :
        # Read BigQuery Query
        logging.info(f'Loading BQ Result into Dataframe for {dimensional_table}')
        table_df = readBigQuery(
            query=SQL_QUERIES[dimensional_table],
            user='csotob',
            gbq_client = Client()
        )


        # Upload to S3
        logging.info(f'Uploading {dimensional_table} to S3')
        moveDataframeToS3(df_file=table_df,
                        landing_bucket=landing_bucket,
                        landing_path=landing_path,
                        table_name=table_names[dimensional_table],
                        column_types=column_types[dimensional_table],
                        boto3_session=boto3_session
                        )

    logging.info(f'File successfully uploaded to: {landing_bucket}/{landing_path}')



if __name__ == '__main__':
    main()
