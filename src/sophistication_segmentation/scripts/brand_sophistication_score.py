from __future__ import annotations

# Default
import logging
import argparse
from logging import config

# pip
import pendulum
from google.cloud.bigquery import Client, TimePartitioning

# Own
from common.constants import LOGGING_CONFIG
from common.databases.queries import QueryDict
from common.gcp_extended.bigquery import (
    deleteFromTable,
    createTableAsSelect,
)


# -------------------------------------------------------------------------
#  Config
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
parser.add_argument(
    '--store_banner', type=str, required=True,
    choices=['Unimarc', 'Alvi', 'Super 10', 'Mayorista'],
    help='SMU subsidiary for which the allocation will be made'
)
parser.add_argument(
    '--min_category_transacted_items', type=int, required=True,
    help='Minimum number of baskets a client must have to be considered'
)

# -------------------------------------------------------------------------
#  SQL Queries
# -------------------------------------------------------------------------
SQL_QUERIES = QueryDict({
    'brand_sophistication_score':"""
    WITH distinct_products AS (
        SELECT
            ean,
            cat_dsc AS category_description,
            brand_desc AS brand,
            SAFE_CAST(contenido_bruto AS FLOAT64) AS weight,
            grupo_dsc AS sub_category_description,
            um_contenido AS weight_um,
            neg_dsc AS business_name
        FROM `${gcp_project}.CDA_VISTAS.VW_DIM_PRODUCT`
    ),

    customer_category_counts_filter AS (
        SELECT
            customer_key,
            category_description,
            COUNT(*) AS category_count
        FROM `${gcp_project}.CDA_VISTAS.VW_SALES_ITEM`
        INNER JOIN distinct_products
        USING(ean)
        INNER JOIN `${gcp_project}.CDA_VISTAS.VW_DIM_STORE`
        USING (store_id)
        WHERE
            transaction_date >= DATE('${execution_date}') - INTERVAL 1 YEAR
            AND transaction_date < DATE('${execution_date}')
            AND transaction_type IN ('TN','TF','BX','B','BE','F','NC')
            AND itm_txn_fcn_tp_dsc = 'V'
            AND store_banner = '${store_banner}'
        GROUP BY 1,2
        HAVING COUNT(*) >= ${min_category_transacted_items}
    )

    SELECT
        '${execution_date}' AS date,
        '${store_banner}' AS store_banner,
        CATEGORY_DESCRIPTION,
        BRAND,
        2 * (
            (SUM(INDEXED_PPUM * QUANTITY) / SUM(QUANTITY))
            * (SUM(INDEXED_PU * QUANTITY) / SUM(QUANTITY))
        ) / (
            (SUM(INDEXED_PPUM * QUANTITY) / SUM(QUANTITY))
            + (SUM(INDEXED_PU * QUANTITY) / SUM(QUANTITY))
        ) AS HM_PU_PPUM
    FROM (
        SELECT
            CATEGORY_DESCRIPTION,
            BRAND,
            QUANTITY,
            VALUE / QUANTITY / AVG(VALUE / QUANTITY) OVER (
                PARTITION BY CONCAT(SUB_CATEGORY_DESCRIPTION, ' - ', WEIGHT_UM)
            ) AS INDEXED_PU,
            VALUE / (distinct_products.WEIGHT * QUANTITY) / AVG(VALUE / (distinct_products.WEIGHT * QUANTITY)) OVER (
                PARTITION BY CONCAT(SUB_CATEGORY_DESCRIPTION, ' - ', WEIGHT_UM)
            ) AS INDEXED_PPUM

        FROM `${gcp_project}.CDA_VISTAS.VW_SALES_ITEM`
        INNER JOIN `${gcp_project}.CDA_VISTAS.VW_DIM_STORE`
        USING (STORE_ID)
        INNER JOIN distinct_products
        USING (EAN)
        INNER JOIN customer_category_counts_filter
        USING (CUSTOMER_KEY, CATEGORY_DESCRIPTION)
        LEFT JOIN (
            SELECT
                market_basket_key,
                TRUE AS from_other_ecommerce
            FROM `${gcp_project}.CDA_VISTAS.VW_FACT_MARKET_BASKET_E_COMMERCE`
            WHERE canal_venta IN ('PEDIDOS YA','CORNER SHOP','RAPPI','RAPPI TURBO')
        ) external_ecommerce_filter
        USING (market_basket_key)

        WHERE
            transaction_date >= DATE('${execution_date}') - INTERVAL 1 YEAR
            AND transaction_date < DATE('${execution_date}')
            AND store_banner = '${store_banner}'
            AND sku_product IS NOT NULL
            AND sku_product != 'None'
            AND transaction_type IN ('TN','TF','BX','B','BE','F','NC')
            AND itm_txn_fcn_tp_dsc = 'V'
            AND unit_price > 0
            AND value > 0
            AND business_name NOT IN ('SERVICIOS COMERCIALES', 'NO RETAIL', 'None')
            AND from_other_ecommerce IS NULL
            AND customer_key <> MD5('CST^CL^-1')
    )
    GROUP BY 1,2,3,4
    """  # noqa: E501
})


# -------------------------------------------------------------------------
#                        Main Function
# -------------------------------------------------------------------------
def main():
    # ----------
    # Parameters
    # ----------
    args = vars(parser.parse_args())
    # Environment
    user: str = (  # noqa: F841
        'brand_category_'
        + args['project_name']
        + '_score'
    )
    gcp_project: str = args['gcp_project']
    execution_date: pendulum.Date = pendulum.date(
        *list(map(int, args['execution_date'].split('-')))
    ).set(
        day=1 # Allways ensures first day of the month
    )
    min_category_transacted_items: int = args['min_category_transacted_items']
    store_banner: str = args['store_banner']

    # Hardcoded
    gbq_client = Client()
    table_ref=f'{gcp_project}.ML_LAB.BRAND_SOPHISTICATION_SCORE'

    logging.info(f'gcp_project = {gcp_project}')
    logging.info(f'execution_date = {execution_date}')
    logging.info(f'min_category_transacted_items = {min_category_transacted_items}')
    logging.info(f'store_banner = {store_banner}')


    # Remove past run if needed
    logging.info(f'Removing past run from {table_ref}')
    deleteFromTable(
        table_ref=table_ref,
        where_clause=f"""
            date = '{execution_date}'
            AND store_banner = '{store_banner}'
        """,
        gbq_client=gbq_client,
    )

    # Create the table
    logging.info('Creating new partition')
    createTableAsSelect(
        query=SQL_QUERIES['brand_sophistication_score'].substitute(
           gcp_project=gcp_project,
           execution_date=execution_date,
           store_banner=store_banner,
           min_category_transacted_items=min_category_transacted_items,
        ),
        table_ref=table_ref,
        create_disposition='CREATE_IF_NEEDED',
        write_disposition='WRITE_APPEND',
        time_partitioning=TimePartitioning(
            field='date',
            type_='DAY',
        ),
        clustering_fields=['store_banner'],
        gbq_client=gbq_client,
    )

    logging.info('Done!')


if __name__ == '__main__':
    main()
