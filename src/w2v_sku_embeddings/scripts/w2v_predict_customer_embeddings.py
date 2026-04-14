"""Embedding trainning script."""
from __future__ import annotations

# Default
import logging
import argparse
from logging import config
from textwrap import dedent

# pip
from google.cloud.bigquery import Client, TimePartitioning

# Own
import common.gcp_extended.bigquery as gbq_extended
from common.constants import LOGGING_CONFIG
from common.databases.queries import QueryDict


# -------------------------------------------------------------------------
# Package config
# -------------------------------------------------------------------------
# Logging config
config.dictConfig(LOGGING_CONFIG)
# Parser
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
    help='SMU format in which the embeddings will be allocated'
)
parser.add_argument(
    '--month_interval', default=12, type=int,
    help='Number of months of past transactions from the execution date to view'
)


# -------------------------------------------------------------------------
# SQL Queries
# -------------------------------------------------------------------------
SQL_QUERIES = QueryDict({
    'customer_embeddings': (
    dedent("""
    SELECT
        date,
        store_banner,
        customer_key,
    """)
    + ',\n'.join([
        f'sum(dim_{i} * frecuencia * ppum_pu_score / backwards_date_week_diff) / COUNT(DISTINCT sku) AS dim_{i}'  # noqa: E501
        for i in range(100)
        ])
    + dedent("""
    FROM (
        SELECT
            customer_key,
            CAST(sku_product AS BIGINT) AS sku,
            ppum_pu_score,
            COUNT(DISTINCT market_basket_key) AS frecuencia,
            MIN(
                LOG(2,
                    -- Must add bc starts from 0
                    2 + DATE_DIFF(
                        DATE('${execution_date}'),
                        transaction_date,
                        ISOWEEK
                    )
                )
            ) AS backwards_date_week_diff

        FROM `${gcp_project}.CDA_VISTAS.VW_SALES_ITEM` sales_item

        INNER JOIN `${gcp_project}.CDA_VISTAS.VW_DIM_STORE` dim_store
        USING (store_id)

        INNER JOIN (
            SELECT
                sku_product,
                MAX(grupo_dsc) AS category_description
            FROM `${gcp_project}.CDA_VISTAS.VW_DIM_PRODUCT`
            GROUP BY 1
            HAVING MAX(neg_dsc) NOT IN ('SERVICIOS COMERCIALES', 'NO RETAIL')
        ) dim_product
        USING (sku_product)

        LEFT JOIN (
            SELECT
                market_basket_key,
                TRUE AS from_other_ecommerce
            FROM `${gcp_project}.CDA_VISTAS.VW_FACT_MARKET_BASKET_E_COMMERCE`
            WHERE canal_venta IN ('PEDIDOS YA','UBER EATS','RAPPI','RAPPI TURBO')
        ) external_ecommerce_filter
        USING (market_basket_key)

        INNER JOIN (
            SELECT
                customer_key,
                category_description,
                hm_pu_ppum * (total_category_value / (SUM(total_category_value) OVER (PARTITION BY customer_key))) AS ppum_pu_score
            FROM ${gcp_project}.ML_LAB.CUSTOMER_SOPHISTICATION_SCORE
        )
        USING (customer_key, category_description)

        WHERE sales_item.transaction_date >= DATE('${execution_date}') - INTERVAL ${month_interval} MONTH
            AND sales_item.transaction_date < DATE('${execution_date}')
            AND store_banner = '${store_banner}'
            AND transaction_type IN ('TN','TF','BX','B','BE','F')
            AND itm_txn_fcn_tp_dsc = 'V'
            AND from_other_ecommerce IS NULL
        GROUP BY 1,2,3
    )

    INNER JOIN ${gcp_project}.ML_LAB.W2V_SKU_EMBEDDINGS
    USING (sku)

    GROUP BY 1,2,3
    """)  # noqa: E501
)})


def main() -> None:  # noqa: D103
    args = vars(parser.parse_args())

    # --------------------
    # Parameters
    # --------------------
    # Environment
    user = args['project_name']  # noqa: F841
    gcp_project = args['gcp_project']
    execution_date = args['execution_date']
    store_banner = args['store_banner']
    month_interval = args['month_interval']
    logging.info(f'Execution date: {execution_date}')
    logging.info(f'Store banner: {store_banner}')
    logging.info(f'Month interval: {month_interval}')

    # Fixed
    gbq_client = Client()
    final_table_ref = f'{gcp_project}.ML_LAB.W2V_CUSTOMER_EMBEDDINGS'


    # Remove past run output
    logging.info('Removing past run if exists')
    gbq_extended.deleteFromTable(
        table_ref=final_table_ref,
        where_clause=f"""
            date = '{execution_date}'
            AND store_banner = '{store_banner}'
        """,
        gbq_client=gbq_client,
    )

    # Rebuild customer embeddings table
    logging.info('Create new partition')
    gbq_extended.createTableAsSelect(
        query=SQL_QUERIES['customer_embeddings'].substitute(
            gcp_project=gcp_project,
            execution_date=execution_date,
            store_banner=store_banner,
            month_interval=month_interval,
        ),
        table_ref=final_table_ref,
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


if __name__ == '__main__':
    main()
