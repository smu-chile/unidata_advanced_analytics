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

parser.add_argument(
    '--partition_value', type=str,
    help='AWS Table partition value'
)


# -------------------------------------------------------------------------
#  Config
# -------------------------------------------------------------------------
SQL_QUERIES = {
    'dim_cycle_dh': """
    SELECT
    ORGANIZATION_ID,
    CAMPAIGN_ID,
    CAMPAIGN_TYPE_ID,
    CYCLE_ID,
    CYCLE_NUMBER,
    CYCLE_DESCRIPTION,
    START_DATE,
    END_DATE,
    SOURCE_ID,
    LOAD_DATE,
    LOAD_OWNER
    FROM cl-bigdata-analytics-preprod.CDA_VISTAS.VW_DIM_CYCLE_DH
    """,
    'fact_market_basket_e_commerce' :
    """
    SELECT
    TO_BASE64(MARKET_BASKET_KEY) AS MARKET_BASKET_KEY_GCP,
    CANAL_VENTA
    FROM cl-bigdata-analytics-preprod.CDA_VISTAS.VW_FACT_MARKET_BASKET_E_COMMERCE
    """,
    'customer_organization_dh_shabits': """
    SELECT
        PDA_CUSTOMER_KEY as CUSTOMER_ID,
        SEGMENT_ID,
        WEEK_ID,
        ORGANIZATION_ID
    FROM
    cl-bigdata-analytics-preprod.CDA_VISTAS.VW_FACT_MONTH_CUSTOMER_ORGANIZATION_DH_SHABITS sh
     left join cl-cda-unidata-prod.DS_PROD_CLIENTES_IC.VW_CDA_CST_DEID id
    on id.customer_key = sh.customer_id
    """,
    'fact_workflow' : """
    SELECT
    N_PROMOCION,
    NOMBRE_PROMOCION,
    ID_EVENTO,
    DESCRIPCION_EVENTO_PROMOCIONAL,
    ID_MECANICA,
    DESCRIPCION_MECANICA,
    MATERIAL,
    DESC_MATERIAL,
    UN_MEDIDA_VENTA,
    ORGANIZACION_VENTAS,
    CANAL_DISTRIBUCION,
    EAN,
    CATEGORIA,
    DESC_CATEGORIA,
    LINEA,
    DESCRIPCION_LINEA,
    MARCA,
    DESC_MARCA,
    PROVEEDOR_SELL_IN,
    NOMBRE_DEL_PROVEEDOR,
    PROVEEDOR_SELL_OUT,
    NOMBRE_DEL_PROVEEDOR_SELL_OUT,
    PROVEEDOR_SIN_FINANCIAMIENTO,
    NOMBRE_DEL_PROVEEDOR_SIN_FINANCIAMIENTO,
    LISTA_DE_PRECIOS,
    DES_LISTA_PRECIO,
    TIENDA,
    NOMBRE_TIENDA,
    TIPO_PROMOCION,
    DESC_PROMOCION,
    PRECIO_MODAL,
    PRECIO_MODAL_TOTAL,
    PRECIO_PROMOCIONAL,
    PRECIO_TOTAL_PROMOCIONAL,
    AHORRO,
    AHORRO_TOTAL,
    PORCENTAJE_DE_DESCUENTO,
    CANTIDAD_N,
    CANTIDAD_M,
    ARTICULO_1_PACK_VIRTUAL,
    ARTICULO_2_PACK_VIRTUAL,
    ARTICULO_3_PACK_VIRTUAL,
    ARTICULO_4_PACK_VIRTUAL,
    ARTICULO_5_PACK_VIRTUAL,
    PRECIO_UNITARIO_PACK_VIRTUAL,
    PRECIO_MODAL_PACK,
    PRECIO_FIJO,
    PLU_X,
    PLU_Y,
    PLU_Z,
    PRECIO_PLU_Z,
    DESDE_KG,
    PRECIO_KILO,
    LLEVAS_N,
    PRECIO_N,
    PORCENTAJE_N,
    MONEDA,
    UNIDAD_MEDIDA_PEDIDO,
    CAPACIDAD_X_UMP,
    COSTO_UNIDAD_MEDIDA_DE_PEDIDO,
    COSTO_UNIDAD_MEDIDA_DE_VENTA,
    FACTOR,
    TIPO_FINANCIAMIENTO,
    IMPORTE_NEGOCIADO,
    PORCENTAJE_FINANCIAMIENTO,
    COSTO_NETO_UMP,
    PORCENTAJE_COSTO_PROMOCIONAL,
    COSTO_PROMOCIONAL_UMV,
    DESDE_SELL_IN,
    HASTA_SELL_IN,
    FECHA_LIMITE_ULTIMO_MINUTO,
    FECHA_LIMITE_MODELO_EXEPCION,
    FECHA_INGRESO_PLU,
    FECHA_INGRESO_DE_DATOS_MARKETING,
    FECHA_INICIO_DE_PROMOCION,
    FECHA_FIN_DE_PROMOCION,
    ESTIMACION_DE_VENTA,
    CTO_ESPERADO_VENTAS,
    UN_MINIMA_MPLEMENTACION,
    SUGERIDO_DE_PUBLICACION,
    PORCENTAJE_DESCUENTO,
    USUARIO_COMPRADOR,
    NOMBRE_COMPRADOR,
    MODIFICADO_EL,
    NIVEL_DE_AGRESIVIDAD,
    LOCALES_CATALOGADOS,
    TIENDAS_HABILITADAS,
    PORCENTAJE_COBERTURA,
    CLUB_AHORRO,
    PRODUCTO_FOCO,
    PROMOCION_PUBLICADA,
    ID_UBICACION_CATALOGO,
    DESCRIPCION_UBICACION_CATALOGO,
    ID_UBICACION_EXHIBICION_ADICIONAL,
    DES_UBI_EXHIBICION,
    ID_TIPO_DE_PUBLICACION,
    DESCRIPCION_TIPO_DE_PUBLICACION,
    CODIGO_DE_BLOQUEO,
    DESCRIPCION_BLOQUEO,
    OCASION_DE_CONSUMO,
    DESCRIPCION_DE_LA_OCASION_DE_CONSUMO,
    NEGOCIO,
    DESCRIPCION_DEL_NEGOCIO,
    NUMERO_DE_LA_OFERTA,
    LISTA_SEGMENTADA_SELL_IN,
    DESCRIPCION_LS_SELL_IN,
    CENTROS_LS_SELL_IN,
    LISTA_SEGMENTADA_SELL_OUT,
    DESCRIPCION_LS_SELL_OUT,
    CENTROS_LS_SELL_OUT,
    PORCENTAJE_COBERTURA_PRECIO,
    CATALOGID,
    UOM,
    PROMOEVENTMECHANISM,
    FINANCIAMIENTO,
    PUBLISHEDFLAG,
    STARTDATE_ADJ,
    ENDDATE_ADJ,
    LENGTH_ADJ,
    PROMOID,
    WORLD,
    PROMOTYPE,
    DESCUENTOFINAL,
    BLOQUEO,
    PRODUCTOFOCO,
    UNIDADES_VENTAUMV,
    UNIDADES_VENTAUMB,
    IN_OUT,
    CAPACIDAD_DEL_UMV,
    VARIANTE,
    PRECIO_PROMOCIONAL_2,
    PRECIO_TOTAL_PROMOCIONAL_2,
    CANTIDAD_N2,
    ID_PACK,
    PRODUCTO_BASE_PACK,
    PRECIO_UNITARIO_PACK_VIRTUALV,
    FACTOR_2,
    IMPORTE_NEGOCIADO_2,
    PORCENTAJE_FINANCIAMIENTO_2,
    ID_GRUPO_GEO,
    DESC_GRUPO_GEO,
    ID_WORKFLOW,
    FECHA_CARGA,
    FECHA_MODIFICACION,
    REGISTRO_VALIDO,
    ULTIMA_CARGA,
    PROM_CANCEL,
    SKU_CANCEL
    FROM cl-bigdata-analytics-preprod.CDA_VISTAS.VW_FACT_WORKFLOW
    """
}

# -------------------------------------------------------------------------
# Main function
# -------------------------------------------------------------------------
def main() -> None:  # noqa: D103
    # Parameters
    args = vars(parser.parse_args())
    gcp_project_id: str = args['project_id']

    tables = ['dim_cycle_dh',
            'fact_market_basket_e_commerce',
            'customer_organization_dh_shabits',
            'fact_workflow']

    column_types = {'dim_cycle_dh': ['Int64','Int64', 'Int64', 'Int64', 'Int64',
                      'str', 'str', 'str', 'Int64', 'str', 'str'],
                    'fact_market_basket_e_commerce' : ['str', 'str'],
                    'customer_organization_dh_shabits' : ['Int64','Int64', 'Int64', 'Int64'],
                    'fact_workflow' : ['Int64', 'str', 'Int64', 'str', 'Int64', 'str', 'Int64',
                                       'str', 'str', 'str', 'str', 'str', 'str', 'str', 'str',
                                       'str', 'str', 'str', 'str', 'str', 'str', 'str', 'str',
                                       'str', 'str', 'str', 'str', 'str', 'Int64', 'str', 'float',
                                       'float', 'float', 'float', 'float', 'Int64', 'float',
                                       'float', 'float', 'str', 'str', 'str', 'str', 'str',
                                       'float', 'float', 'float', 'str', 'str', 'str', 'float',
                                       'float', 'float', 'float', 'float', 'float', 'str', 'str',
                                       'float', 'float', 'float', 'float', 'str', 'float', 'float',
                                       'float', 'float', 'float', 'str', 'str', 'str', 'str',
                                       'str', 'str', 'str', 'str', 'Int64', 'float', 'float',
                                       'str', 'float', 'str', 'str', 'str', 'str', 'float',
                                       'float', 'float', 'str', 'str', 'str', 'str', 'str', 'str',
                                       'str', 'str', 'str', 'str', 'str', 'str', 'str', 'str',
                                       'str', 'str', 'float', 'str', 'str', 'float', 'str', 'str',
                                       'float', 'Int64', 'str', 'str', 'str', 'str', 'str', 'str',
                                       'Int64', 'Int64', 'str', 'str', 'float', 'Int64', 'Int64',
                                       'float', 'float', 'str', 'float', 'float', 'float', 'float',
                                       'float', 'str', 'str', 'float', 'float', 'float', 'float',
                                       'str', 'str', 'str', 'str', 'str', 'str', 'str', 'str',
                                       'str']

                    }

    table_names = {'dim_cycle_dh':  'TMP_LAB_SMU_DIM_CYCLE_DH_GCP',
                   'fact_market_basket_e_commerce' :
                        'TMP_LAB_SMU_FACT_MARKET_BASKET_E_COMMERCE_GCP',
                    'customer_organization_dh_shabits' :
                        'TMP_LAB_SMU_FACT_MONTH_CUSTOMER_ORGANIZATION_DH_SHABITS_GCP',
                    'fact_workflow' : 'TMP_LAB_SMU_FACT_WORKFLOW_GCP'
                    }

    landing_bucket = 'smu-datalake-test-landing'
    landing_path = 'views/datascience'

    # Load data from SharePoint to pandas DataFrame
    boto3_session=boto3.Session(
                **getSecret(
                    project=gcp_project_id,
                    secret_name='bdaa_aws_credentials'  # noqa: S106
                )
            )

    for dim_table in tables :
        logging.info(f'Load the file {dim_table} to DataFrame')
        table_df = readBigQuery(
            query=SQL_QUERIES[dim_table],
            user='csotob',
            gbq_client = Client()
        )

        logging.info(f'Load the file {dim_table} to S3')
        # Upload to S3
        moveDataframeToS3(df_file=table_df,
                        landing_bucket=landing_bucket,
                        landing_path=landing_path,
                        table_name=table_names[dim_table],
                        column_types=column_types[dim_table],
                        boto3_session=boto3_session
                        )

        logging.info(f'File successfully uploaded to: {landing_bucket}/{landing_path}')



if __name__ == '__main__':
    main()
