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
    '--rollback_months', default=6, type=int,
    help='Number of months of transactions from the execution date taken'
)
parser.add_argument(
    '--rollback_months_filter', default=12, type=int,
    help='Number of months of transactions from the execution date taken'
)
parser.add_argument(
    '--min_transacted_months', default=3, type=int,
    help='Min number of different months with purchases for each user'
)
parser.add_argument(
    '--top_n', default=100, type=int,
    help='Max number of my usuals to be assigned to each user'
)


# -------------------------------------------------------------------------
#  SQL Queries
# -------------------------------------------------------------------------
SQL_QUERIES = QueryDict({
    'compute_usuals_partition':
    """
    WITH max_usuals_per_customer AS (
        SELECT
            customer_key,
            CASE
                WHEN (lt_50 + lt_30) = 0 THEN ${top_n}
                WHEN (lt_50 + lt_30) = 1 THEN 50
                ELSE 30
            END AS max_usuals

        FROM (
            SELECT
                customer_key,
                MAX(basket_quantity) AS max_items,
                CASE
                    WHEN MAX(basket_quantity) <= 30 THEN 1
                    ELSE 0
                END AS lt_30,
                CASE
                    WHEN MAX(basket_quantity) <= 50 THEN 1
                    ELSE 0
                END AS lt_50
            FROM `${gcp_project}.CDA_VISTAS.VW_SALES_BASKET` sales_basket


            INNER JOIN `${gcp_project}.CDA_VISTAS.VW_DIM_STORE` dim_store
            USING (store_id)

            INNER JOIN `${gcp_project}.CDA_VISTAS.VW_FACT_MARKET_BASKET_E_COMMERCE`
            USING (market_basket_key)

            WHERE
                transaction_date >= DATE('${execution_date}') - INTERVAL ${rollback_months_filter} MONTH
                AND transaction_date < DATE('${execution_date}')
                AND canal_venta = 'E-COMMERCE'
                AND store_banner = '${store_banner}'
                AND fnc_doc_tp_dsc IN ('NE','FX','BX','B ','BE','FE','F ','NC','TN','TF')
                AND itm_txn_fcn_tp_dsc = 'V'
                AND total_value > 0
            GROUP BY 1
        )
    ),

    max_usuals_filter AS (
        SELECT
            customer_key,
            30 AS max_usuals
        FROM `${gcp_project}.CDA_VISTAS.VW_FACT_MONTH_CUSTOMER_APP_USERS`
        LEFT JOIN (
            SELECT
                customer_key,
                1 AS in_filter
            FROM max_usuals_per_customer
        )
        USING (customer_key)
        WHERE in_filter IS NULL
            AND unimarc_logged = 1
        GROUP BY 1

        UNION ALL

        SELECT *
        FROM max_usuals_per_customer
    ),

    transactions AS (
        SELECT
            customer_key AS customer_id,
            CAST(ean AS BIGINT) AS ean,
            txn_key,
            -- I'm using log2() inspired by DCG@K
            LOG(2,
                -- Must add bc starts from 0
                2 + DATE_DIFF(
                    DATE('${execution_date}'),
                    transaction_date,
                    ISOWEEK
                )
            ) AS backwards_date_week_diff,
            FORMAT_DATE('%m%Y', DATE(transaction_date)) AS transaction_month

        FROM `${gcp_project}.CDA_VISTAS.VW_SALES_ITEM` sales_item

        INNER JOIN `${gcp_project}.CDA_VISTAS.VW_DIM_STORE` dim_store
        USING (store_id)

        LEFT JOIN (
            SELECT
                market_basket_key,
                TRUE AS from_other_ecommerce
            FROM `${gcp_project}.CDA_VISTAS.VW_FACT_MARKET_BASKET_E_COMMERCE`
            WHERE canal_venta IN ('PEDIDOS YA','CORNER SHOP','RAPPI','RAPPI TURBO')
        ) external_ecommerce_filter
        USING (market_basket_key)

        WHERE
            transaction_date >= DATE('${execution_date}') - INTERVAL ${rollback_months} MONTH
            AND transaction_date < DATE('${execution_date}')
            AND store_banner = '${store_banner}'
            AND sku_product <> 'None'
            AND transaction_type IN ('NE','FX','BX','B ','BE','FE','F ','NC','TN','TF')
            AND itm_txn_fcn_tp_dsc = 'V'
            AND from_other_ecommerce IS NULL
    ),

    week_weighted_transactions AS (
        SELECT
            customer_id,
            ean,
            CAST(COUNT(DISTINCT txn_key) AS FLOAT64) / backwards_date_week_diff AS weighted_week_frequency
        FROM transactions
        INNER JOIN (
            SELECT customer_id
            FROM transactions
            -- Removes customers with less than min_transacted_months transactions in diferent months
            GROUP BY customer_id
            HAVING COUNT(DISTINCT transaction_month) >= ${min_transacted_months}
        ) filtered_transactions
        USING (customer_id)
        GROUP BY customer_id, ean, backwards_date_week_diff
    ),

    ranked_transactions AS (
        SELECT
            customer_id AS customer_key,
            ean,
            ROW_NUMBER() OVER(PARTITION BY customer_id ORDER BY SUM(weighted_week_frequency) DESC) AS relevance
        FROM week_weighted_transactions
        GROUP BY customer_id, ean
    ),

    base_my_usuals AS (
        SELECT
            DATE('${execution_date}') AS date,
            customer_key,
            ean,
            relevance,
            '${store_banner}' AS store_banner
        FROM ranked_transactions
        INNER JOIN max_usuals_filter
        USING (customer_key)
        WHERE relevance <= max_usuals
    )


    SELECT
        date,
        customer_key,
        hash_string,
        sku_product,
        ean,
        relevance,
        CASE
            WHEN store_banner = 'Unimarc' THEN 1
            WHEN store_banner = 'Mayorista' THEN 4
            WHEN store_banner = 'Alvi' THEN 5
            WHEN store_banner = 'Super 10' THEN 15
        END AS store_banner,
            CASE
            WHEN unidad_de_medida LIKE '%ST%' THEN LPAD(sku_product, 18, '0') || '-' || 'UN'
            ELSE LPAD(sku_product, 18, '0') || '-' || unidad_de_medida
        END AS vtexrefid

    FROM base_my_usuals

    INNER JOIN  `${gcp_project}.CDA_VISTAS.VW_DIM_PRODUCT`
    USING (EAN)

    INNER JOIN (
        SELECT
            customer_key,
            pda_customer_key AS customer_id
        FROM `${gcp_project_cda}.DS_PROD_CLIENTES_IC.VW_CDA_CST_DEID`
    )
    USING (customer_key)

    INNER JOIN (
        SELECT
            customer_id,
            hash_string
        FROM `${gcp_project_cda}.DS_PROD_CLIENTES_IC.CL_HASH`
    )
    USING (customer_id)
    """,  # noqa: E501
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
    user: str = args['project_name']  # noqa: F841
    gcp_project: str = args['gcp_project']
    gcp_project_cda: str = {
        'cl-bigdata-analytics': 'cl-cda-unidata-dev',
        'cl-bigdata-analytics-dev': 'cl-cda-unidata-dev',
        'cl-bigdata-analytics-preprod': 'cl-cda-unidata-prod',
        'cl-bigdata-analytics-prod': 'cl-cda-unidata-prod',
    }[args['gcp_project']]
    execution_date: pendulum.Date = pendulum.date(
        *list(map(int, args['execution_date'].split('-')))
    )
    store_banner: str = args['store_banner']
    rollback_months: int = args['rollback_months']
    rollback_months_filter: int = args['rollback_months_filter']
    min_transacted_months: int = args['min_transacted_months']
    top_n: int = args['top_n']

    # Hardcoded
    gbq_client = Client()
    table_ref=f'{gcp_project}.ECOMMERCE.MY_USUALS'

    logging.info(f'gcp_project = {gcp_project}')
    logging.info(f'gcp_project_cda = {gcp_project_cda}')
    logging.info(f'execution_date = {execution_date}')
    logging.info(f'store_banner = {store_banner}')
    logging.info(f'rollback_months: {rollback_months}')
    logging.info(f'rollback_months_filter: {rollback_months_filter}')
    logging.info(f'min_transacted_months: {min_transacted_months}')
    logging.info(f'top_n: {top_n}')

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
        query=SQL_QUERIES['compute_usuals_partition'].substitute(
            gcp_project=gcp_project,
            gcp_project_cda=gcp_project_cda,
            execution_date=execution_date,
            store_banner=store_banner,
            rollback_months=rollback_months,
            rollback_months_filter=rollback_months_filter,
            min_transacted_months=min_transacted_months,
            top_n=top_n,
        ),
        table_ref=table_ref,
        create_disposition='CREATE_IF_NEEDED',
        write_disposition='WRITE_APPEND',
        time_partitioning=TimePartitioning(
            field='date',
            type_='DAY',
        ),
        use_legacy_sql=False,
        clustering_fields=['store_banner'],
        gbq_client=gbq_client,
    )

    logging.info('Done!')




if __name__ == '__main__':
    main()
