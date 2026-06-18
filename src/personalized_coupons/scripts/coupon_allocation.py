# Default
import os
import base64
import logging
import argparse
from logging import config

# pip
import pandas as pd
import pendulum
from google.cloud.bigquery import Client
from sklearn.metrics.pairwise import cosine_distances

# Own
from common.constants import LOGGING_CONFIG
from common.databases.queries import QueryDict
from common.gcp_extended.bigquery import (
    uploadFrame,
    readBigQuery,
    deleteFromTable,
    setTableExpiration,
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
    '--uuid', type=str, required=True,
    help='Unique identifier of the current run. Used to differenciate GCP objects'
)
parser.add_argument(
    '--batch_size', type=int, default=100000, required=True,
    help='Batch size'
)


# -------------------------------------------------------------------------
#  SQL Queries
# -------------------------------------------------------------------------
SQL_QUERIES = QueryDict({
    'cycle_table':
    # Gets the next cycle number and initial date
    """
    SELECT
        ciclo_interno AS ciclo,
        FORMAT_DATE('%Y-%m-%d', inicio_ciclo) as inicio_ciclo
    FROM ${gcp_project}.FIDELIZACION.PLAN_DE_CAMPANAS_CUPONES_PERSONALIZADOS
    WHERE
        store_banner = '${store_banner}'
        AND inicio_ciclo >= '${execution_date}'
    ORDER BY inicio_ciclo ASC
    LIMIT 1;
    """,

    'offer_sku_table':
    # Gets offer_id and SKU from the consolidate table for some cycle
    """
    SELECT
        offer_id,
        material,
        bm_priority

    FROM (
        SELECT
            offer_id,
            material,
        FROM ${gcp_project}.FIDELIZACION.EAN_POR_OFFER_ID_CUPONES_PERSONALIZADOS
        WHERE
            fecha_inicio_ciclo = '${cycle_start_date}'
            AND store_banner = '${store_banner}'
            AND tipo_oferta LIKE '%PERSONALIZA%'
    )

    INNER JOIN (
        SELECT
            offer_id,
            CASE
                WHEN SUM(COALESCE(segmento_bm, 0)) > 0 THEN TRUE
                ELSE FALSE
            END AS bm_priority

        FROM `${gcp_project}.FIDELIZACION.EAN_POR_OFFER_ID_CUPONES_PERSONALIZADOS` coupons

        LEFT JOIN (
            SELECT
                store_banner,
                CAST(material AS INT64) AS material,
                CASE
                    WHEN segmento_bm IN ('Hi-Lo', 'Low-Lower') THEN 1
                    ELSE 0
                END AS segmento_bm
            FROM `${gcp_project}.PRECIO_PROMOCIONES.BALANCE_MATRIX`
        ) balance_matrix
        USING (material, store_banner)

        WHERE
            fecha_inicio_ciclo = '${cycle_start_date}'
            AND store_banner = '${store_banner}'
            AND tipo_oferta LIKE '%PERSONALIZA%'

        GROUP BY 1
    )
    USING (offer_id)
    """,

    'customer_embedding_table':
    # Gets the customer embeddings matrix
    """
    SELECT *
    FROM (
        SELECT *
        FROM ${gcp_project}.ML_LAB.W2V_CUSTOMER_EMBEDDINGS
        WHERE
            date = '${embedding_date}'
            AND store_banner = '${store_banner}'
    )
    INNER JOIN (
        SELECT
            customer_key
        FROM `${gcp_project_unidata}.DS_PROD_CLIENTES_IC.VW_FACT_MONTH_CUSTOMER_ORGANIZATION_SHABITS_UNIDATA_ALVI`
        WHERE monthid = CAST(FORMAT_DATE('%Y%m', DATE('${execution_date}') - INTERVAL 1 MONTH) AS INT64)
    )
    USING (customer_key)
    """,  # noqa: E501

    'product_embedding_table':
    # Gets the product embedding matrix with group, category and brand only
    # for products with valid offer_id
    """
    SELECT *
    FROM ${gcp_project}.ML_LAB.W2V_SKU_EMBEDDINGS

    INNER JOIN (
        SELECT
            CAST(sku_product AS BIGINT) AS sku,
            grupo_key,
            categoria_key,
            brnd_id
        FROM (
            SELECT
                *,
                ROW_NUMBER() OVER (PARTITION BY SKU_PRODUCT) AS SKU_INDEX
            FROM ${gcp_project}.CDA_VISTAS.VW_DIM_PRODUCT_HIERARCHY
        )
        WHERE
            SKU_INDEX = 1
            AND sku_product IS NOT NULL
        GROUP BY 1,2,3,4
    )
    USING (sku)

    INNER JOIN (
        SELECT
            material as sku
        FROM ${gcp_project}.FIDELIZACION.EAN_POR_OFFER_ID_CUPONES_PERSONALIZADOS
        WHERE
            fecha_inicio_ciclo = '${cycle_start_date}'
        AND store_banner = '${store_banner}'
        AND tipo_oferta LIKE '%PERSONALIZA%'
    )
    USING (sku)

    WHERE
        date = '${embedding_date}'
        AND store_banner = '${store_banner}'
    """,

    'create_transactions_table':
    # Create the full transactions table: customer id | product id |
    # last transaction date (max date) | client index (rnk) | period
    """
    SELECT
        customer_key,
        CAST(sku_product AS INT64) AS product_id,
        last_transaction_date,
        nivel,
        DENSE_RANK() OVER(ORDER BY customer_key) AS rnk

    FROM (
        SELECT
            customer_key,
            sku_product,
            MAX(transaction_date) AS last_transaction_date

        FROM `${gcp_project_aa}.CDA_VISTAS.VW_SALES_ITEM` sales_item

        INNER JOIN `${gcp_project_aa}.CDA_VISTAS.VW_DIM_STORE` dim_store
        USING (store_id)

        LEFT JOIN (
            SELECT
                market_basket_key,
                TRUE AS from_other_ecommerce
            FROM `${gcp_project_aa}.CDA_VISTAS.VW_FACT_MARKET_BASKET_E_COMMERCE`
            WHERE canal_venta IN ('PEDIDOS YA','UBER EATS','RAPPI','RAPPI TURBO')
        ) external_ecommerce_filter
        ON sales_item.market_basket_key = external_ecommerce_filter.market_basket_key

        WHERE
            transaction_date >= DATE('${execution_date}') - INTERVAL 1 YEAR
            AND transaction_date < DATE('${execution_date}')
            AND store_banner = '${store_banner}'
            AND sku_product <> 'None'
            AND transaction_type  IN ('NE','FX','BX','B ','BE','FE','F ','NC','TN','TF')
            AND itm_txn_fcn_tp_dsc = 'V'
            AND from_other_ecommerce IS NULL
        GROUP BY 1,2
    )

    INNER JOIN (
        SELECT
            customer_key,
            nivel_informado AS nivel
        FROM `${gcp_project_unidata}.DS_PROD_CLIENTES_IC.VW_FACT_MONTH_CUSTOMER_ORGANIZATION_SHABITS_UNIDATA_ALVI`
        WHERE monthid = CAST(FORMAT_DATE('%Y%m', DATE('${execution_date}') - INTERVAL 1 MONTH) AS INT64)
    )
    USING (customer_key)
    """,  # noqa: E501

    'batch_transactions_table':
    # Extracts a batch from the transaction table. Includes nivel info
    """
    SELECT
        customer_key,
        product_id,
        last_transaction_date,
        nivel
    FROM `${tmp_transaction_table_ref}`
    WHERE rnk >= ${initial_client_number}
          AND rnk < ${end_client_number}
    """,
})


# -------------------------------------------------------------------------
#                     Functions and Classes
# -------------------------------------------------------------------------
def verifyTask(task: str) -> None:
    """Verify if the task name is either 'loyalty' or 'acquisition'.

    Parameters
    ----------
    task : {'loyalty', 'acquisition'}
        Type of allocationto be verified, either loyalty or acquisition

    Raises
    ------
    ValueError
        When task name is incorrect or misspelled
    """
    if task not in ['loyalty', 'acquisition']:
        err_msg = ("task must be either 'loyalty' or 'acquisition'. "
                   f'You pass {task}.')
        raise ValueError(err_msg)


def createRankings(basket_freq: pd.DataFrame, customer_emb: pd.DataFrame,
                   product_emb: pd.DataFrame, sku_hierarchy: pd.DataFrame,
                   offer_sku: pd.DataFrame, task: str) -> pd.DataFrame:
    """Generate recommendations by cosine similarity.

    Parameters
    ----------
    basket_freq : pd.DataFrame
        Batch of the transactions table.
    customer_emb : pd.DataFrame
        Dataframe with the customer embeddings.
    product_emb : pd.DataFrame
        Dataframe with the product embeddings.
    sku_hierarchy : pd.DataFrame
        Dataframe with the product hierarchy.
    offer_sku : pd.DataFrame
        Dataframe with the SKU and its OfferID.
    task : {'loyalty', 'acquisition'}
        Type of allocation, either loyalty or acquisition.

    Returns
    -------
    rankings : pd.DataFrame
        DataFrame with the distances between OfferID/SKU and client vectors
    """
    # Verify correctly spelled task
    verifyTask(task)
    # Get customer list for the batch
    customers_list = basket_freq['customer_key'].unique()
    logging.info(f'Number of customers in the batch: {len(customers_list)}')
    logging.info(f'Number of products in the embeddings: {product_emb.shape[0]}')
    # Calculate cosine distance between product and customer embeddings
    # Returns a matrix with dim n_customers x n_products
    cosines_dist = cosine_distances(
        customer_emb[customer_emb.index.isin(customers_list)].to_numpy(),
        product_emb.to_numpy()
    )
    df_cosines_dist = pd.DataFrame(
        cosines_dist,
        index=customer_emb[customer_emb.index.isin(customers_list)].index,
        columns=product_emb.index
    )
    # "Flattens" the rectangular matrix into three columns:
    # n_customer | n_product | cosine distance
    # So now the df_cosines_dist matrix have dim:
    # (n_customer x n_product) x 3
    df_cosines_dist = pd.melt(df_cosines_dist.reset_index(),
                              id_vars='customer_id')

    # Delete cosine distances matrix as its values are now stored as df
    del cosines_dist

    # Rename columns
    aux_cols_rename_map = {'value': 'cosine'}
    if task == 'loyalty':
        aux_cols_rename_map['sku'] = 'material'
    df_cosines_dist = df_cosines_dist.rename(
        columns=aux_cols_rename_map,
        errors='raise'
    )

    # Attatch the group and brand identificator to the:
    # SKU's in loyalty allocation
    # OfferIDs in acquisition allocation
    df_cosines_dist = df_cosines_dist.join(
        other = sku_hierarchy[['grupo_key_brnd_id']] if task == 'loyalty'\
                else offer_sku[['grupo_key_brnd_id']],
        on='material' if task == 'loyalty' else 'offer_id',
        how='inner'
    )
    # Table with the customer and the categories and brands he/she buys
    loyalty_basket = basket_freq.join(
        sku_hierarchy[['grupo_key_brnd_id']],
        on='product_id',
        how='inner'
    ).copy()[
        ['customer_key', 'grupo_key_brnd_id','nivel']
    ].drop_duplicates(
        ['customer_key', 'grupo_key_brnd_id','nivel'],
        keep='last'
    )

    # Filter:
    # - Loyalty: Only products in the sub categories (grupo_key_brand) and
    #            brands the client buy
    # - Acquisition: Only OfferIDs in the sub categories and brands the
    #                client dont buy
    rankings = df_cosines_dist.merge(
        loyalty_basket,
        right_on=['customer_key', 'grupo_key_brnd_id'],
        left_on=['customer_id', 'grupo_key_brnd_id'],
        how='left' if task == 'acquisition' else 'inner',
        indicator=(task == 'acquisition')
        )
    if task == 'acquisition':
        rankings = rankings[rankings['_merge'] == 'left_only']
        rankings = rankings[[
            'customer_id', 'offer_id', 'cosine', 'grupo_key_brnd_id'
        ]].merge(
            basket_freq.groupby('customer_key').agg(nivel=('nivel','max')),
            right_on='customer_key',
            left_on='customer_id',
            how='inner'
        )
        rankings = rankings.rename(
            columns={'customer_id':'customer_key'})

    # Return:
    # - Loyalty: SKU, Customer ID and cosine distance
    # - Acquisition: OfferID, Customer ID and cosine distance
    return rankings[
        ['material' if task == 'loyalty' else 'offer_id',
         'cosine', 'customer_key','nivel']
        ]


def processRankings(rankings_df: pd.DataFrame, sku_hierarchy_df: pd.DataFrame,
                    offer_sku_df: pd.DataFrame, task: str,
                    loyalty_rankings_df: pd.DataFrame | None = None) -> pd.DataFrame:
    """Process rankings for posterior filters.

    Parameters
    ----------
    rankings_df : pd.DataFrame
        DataFrame with the distances between OfferID/SKU and client vectors
    sku_hierarchy_df : pd.DataFrame
        Dataframe with the product hierarchy.
    offer_sku_df : pd.DataFrame
        Dataframe with the SKU and its OfferID.
    task : {'loyalty', 'acquisition'}
        Type of allocation, either loyalty or acquisition.
    loyalty_rankings_df: pd.DataFrame
        Dataframe with loyalty rankings.

    Returns
    -------
    filtered_ranking_df : pd.DataFrame

    """
    # Verify correctly spelled task
    verifyTask(task)
    # Loyalty task path
    if task == 'loyalty':
        # Add OfferID to the ranking
        rankings_df = rankings_df.join(
            offer_sku_df.groupby('material').max()['offer_id'],
            on='material'
        )
        # TODO(ecastrot): Maybe the next two lines can be optimized
        rankings_df = rankings_df.sort_values(by=['customer_key', 'cosine'])
        rankings_df = rankings_df.drop_duplicates(['customer_key', 'offer_id'])
        # Adds SKU category and group columns
        rankings_df = rankings_df.join(
            sku_hierarchy_df[['categoria_key', 'grupo_key']],
            on='material'
        )

    # Acquisition task path
    elif task == 'acquisition':
        # Add category and group key to rankings
        rankings_df = rankings_df.join(
            offer_sku_df[['categoria_key', 'grupo_key']],
            on='offer_id'
        )
        # Remove already used offer ids
        rankings_df = rankings_df.merge(
            loyalty_rankings_df[
            ['customer_key','offer_id']],
            on=['offer_id','customer_key'],
            how='left',
            indicator=True
        )
        rankings_df = rankings_df[rankings_df['_merge'] == 'left_only']
        # Sort ascending using the customer key and cosine value
        rankings_df = rankings_df.sort_values(by=['customer_key', 'cosine'])
    return rankings_df


def maxCategoryFilter(rankings_df: pd.DataFrame,
                      n_cycle_1: int,
                      task: str) -> pd.DataFrame:
    """Filter the offers by the businness rules.

    Parameters
    ----------
    rankings_df : pd.DataFrame
        DataFrame with the distances between OfferID/SKU and client vectors
    n_cycle_1 : int
        Number of the first cycle for which coupons will be allocated
    task : {'loyalty', 'acquisition'}
        Type of allocation, either loyalty or acquisition.

    Returns
    -------
    filtered_rankings_df : pd.Dataframe
        The final offer id client assignation
    """
    # Verify correctly spelled task
    verifyTask(task)
    # Define exclusions per categories
    # No exclusions in loyalty, max 1 per category in acquisition
    rankings_df = rankings_df.groupby(
        ['customer_key', 'categoria_key']
        ).head(1) if task == 'acquisition' else rankings_df

    # Del columns
    rankings_df = rankings_df.drop(columns=[
        'grupo_key', 'categoria_key'
    ])
    pd.options.mode.chained_assignment = None
    # Group the cosine values by their customer ID. Then, asign a number to
    # the ocurrences by order of appearance. This step is ranking the
    # offers from each client from least to the greatest cosine value
    rankings_df['rank'] = rankings_df.groupby('customer_key')['cosine'].rank(
        ascending=True, method='first'
    ).astype(int)
    logging.info(f'rankings df second group by: {rankings_df.columns}')
    pd.options.mode.chained_assignment = 'warn'
    rankings_df = rankings_df.drop(columns=['cosine'])

    # Adds the cycle mark. Odd ranked offers are assigned to the first
    # cycle and even to the second half of the cycle
    logging.info(f'n cycle: {n_cycle_1}')
    rankings_df['ciclo'] = n_cycle_1
    return rankings_df


def reorganizeAllocations(
        acquisition_rankings:pd.DataFrame, loyalty_rankings:pd.DataFrame,
        max_coupons:int
    ) -> pd.DataFrame:
    """Reorganize ac.rankings to get max coupons per customer.

    Parameters
    ----------
    acquisition_rankings: pd.DataFrame
        contains acquisition rankings
    loyalty_rankings: pd.DataFrame
        contains loyalty rankings
    max_coupons: int
        max coupons per customer according to corresponding level

    Returns
    -------
    processed_rankings: pd.DataFrame
        Reorganized rankings considering balance matrix priority and max coupons
    """  # noqa: W505
    # Sort acquisition considering balance matrix priority
    acquisition_rankings = acquisition_rankings.sort_values(
        by=['customer_key', 'bm_priority'],
        ascending=[True, False]
    )

    acquisition_rankings['rank'] = (
        acquisition_rankings.groupby(
            'customer_key'
        ).cumcount()
        + 1
    )

    # Concat loyalty and acquisition rankings
    processed_rankings = pd.concat([
        loyalty_rankings, acquisition_rankings
    ]).sort_values(by=[
        'customer_key', 'campaign_type_id'
    ])

    # delete duplicates in customer_key and offer_id
    processed_rankings = processed_rankings.drop_duplicates(
        subset=['customer_key', 'offer_id'],
        keep='first'
    )

    # ranking
    processed_rankings['rank'] = (
    processed_rankings.groupby(['customer_key', 'campaign_type_id'])
    .cumcount() + 1
    )

    # Get max_coupons per customer
    if 'Socio VIP' not in processed_rankings['nivel'].to_numpy():
        return processed_rankings.groupby('customer_key').head(max_coupons)
    return processed_rankings[~((processed_rankings['campaign_type_id'] == 6) & (processed_rankings['rank'] > 30))]  # noqa: E501


def filterAndMerge(df:pd.DataFrame,nivel:str,
                   consolidado:pd.DataFrame)-> pd.DataFrame:
    """Filter rankings by level and adds balance matrix priority column."""
    level_df = df[df['nivel'] == nivel]
    return level_df.merge(
        consolidado[['offer_id', 'bm_priority']],
        how='left',
        on='offer_id'
    )


# -------------------------------------------------------------------------
#                        Main Function
# -------------------------------------------------------------------------
def main() -> None:
    args = vars(parser.parse_args())
    # Environment parameters
    user: str = args['project_name']
    gcp_project: str = args['gcp_project']
    execution_date: pendulum.Date = pendulum.date(
        *list(map(int, args['execution_date'].split('-')))
    )
    store_banner: str = args['store_banner']
    uuid: str = args['uuid']
    batch_size: int = args['batch_size']

    # Static parameters
    gbq_client = Client()
    tmp_transaction_table_ref = f'{gcp_project}.TMP.TMP_PERSONALIZED_COUPONS_TRANSACTIONS_{uuid}'
    max_coupons_per_lvl = {
        'Socio Club': 10,
        'Socio Plata': 10,
        'Socio Oro': 20,
        'Socio VIP': 45
    }
    gcp_project_unidata = {
        'cl-bigdata-analytics': 'cl-cda-unidata-dev',
        'cl-bigdata-analytics-dev': 'cl-cda-unidata-dev',
        'cl-bigdata-analytics-preprod': 'cl-cda-unidata-prod',
        'cl-bigdata-analytics-prod': 'cl-cda-unidata-prod',
    }[gcp_project]

    logging.info(f'execution_date: {execution_date}')
    logging.info(f'store_banner: {store_banner}')

    # ---------------------------------------------------------------------
    #                       Stage 1: Data load
    # ---------------------------------------------------------------------
    logging.info('STARTING: Data loading process')
    # Load coupon id + start date information
    # -------------------------------------
    n_cycle, cycle_start_date = readBigQuery(
        query=SQL_QUERIES['cycle_table'].substitute(
            gcp_project=gcp_project,
            execution_date=execution_date,
            store_banner=store_banner,
        ),
        user=user,
        gbq_client=gbq_client
    ).to_numpy()[0]

    # Load Offer ID + SKU code
    # ------------------------
    offer_ids = readBigQuery(
        query=SQL_QUERIES['offer_sku_table'].substitute(
            gcp_project=gcp_project,
            cycle_start_date=cycle_start_date,
            store_banner=store_banner,
        ),
        user=user,
        gbq_client=gbq_client
    )

    # Load product embeddings
    # ------------------------
    sku_embeddings = readBigQuery(
        query=SQL_QUERIES['product_embedding_table'].substitute(
            gcp_project=gcp_project,
            store_banner=store_banner,
            embedding_date=execution_date.replace(day=1),
            cycle_start_date=cycle_start_date,
        ),
        user=user,
        gbq_client=gbq_client
    ).drop(columns=[
        'date', 'store_banner'
    ])

    # Load customer embeddings
    # ------------------------
    customer_embeddings = readBigQuery(
        query=SQL_QUERIES['customer_embedding_table'].substitute(
            gcp_project=gcp_project,
            store_banner=store_banner,
            embedding_date=execution_date.replace(day=1),
            gcp_project_unidata=gcp_project_unidata,
            execution_date=execution_date,
        ),
        user=user,
        gbq_client=gbq_client
    ).drop(columns=[
        'date', 'store_banner'
    ])
    logging.info('ENDED: Data loading process')

    # Rebuild table with client historical transactional data on Athena
    # -----------------------------------------------------------------
    logging.info('Building temporal transaction table...')
    createTableAsSelect(
        query=SQL_QUERIES['create_transactions_table'].substitute(
            gcp_project_aa=gcp_project,
            gcp_project_unidata=gcp_project_unidata,
            store_banner=store_banner,
            execution_date=execution_date,
        ),
        table_ref=tmp_transaction_table_ref,
        gbq_client=gbq_client,
        clustering_fields=['rnk'],
        use_legacy_sql=False,
    )

    # Secure the temporal table is removed tomorrow
    expiration_date = pendulum.now(tz='America/Santiago').add(days=1)
    logging.info(f'Setting its expiration date to {expiration_date}')
    setTableExpiration(
        table_ref=tmp_transaction_table_ref,
        expiration=expiration_date,
        gbq_client=gbq_client,
    )

    # ---------------------------------------------------------------------
    #                   Stage 2: Data preparation
    # ---------------------------------------------------------------------
    logging.info('STARTING: Data preparation process')

    # Stage 2.1: Common data preparation
    # ---------------------------------------------------------------------
    logging.info('STARTING: Data preparation process (common)')
    # Decode bytes
    customer_embeddings = customer_embeddings.rename(columns={
        'customer_key': 'customer_id'
    }).set_index('customer_id')

    sku_embeddings['grupo_key'] = sku_embeddings['grupo_key'].apply(
        base64.b64encode
    ).str.decode('utf-8')

    sku_embeddings['categoria_key'] = sku_embeddings['categoria_key'].apply(
        base64.b64encode
    ).str.decode('utf-8')

    # Create hierarchy df
    sku_hierarchy = sku_embeddings[[
        'sku', 'grupo_key', 'categoria_key', 'brnd_id'
    ]].copy().rename(columns={
        'sku': 'sku_product'
    }).set_index('sku_product')

    # Create balance matrix priority df
    bm_priority = offer_ids[['offer_id', 'bm_priority']]

    # Remove extra columns from sku embeddings table
    sku_embeddings = sku_embeddings.drop(columns=[
        'grupo_key', 'categoria_key', 'brnd_id'
    ]).set_index('sku')

    # Add column with subcat/brand ID
    sku_hierarchy['grupo_key_brnd_id'] = (
        sku_hierarchy['grupo_key'].astype(str)
        + '_'
        + sku_hierarchy['brnd_id'].astype(str)
    )

    # Adds hierarchy information to the offer/sku table
    # The resultant table contains OfferID | material | group | cat | brand
    offer_ids = offer_ids[[
        'offer_id', 'material'
    ]].join(
        sku_hierarchy, on='material', how='inner'
    ).drop_duplicates(
        subset=['offer_id', 'material'],
        keep='first'
    )

    # Number of customers used for the allocation
    n_customers = customer_embeddings.shape[0]
    logging.info(f'n customers: {n_customers}')
    logging.info(f'n skus: {len(sku_embeddings)}')

    logging.info('ENDED: Data preparation process (common)')

    # Stage 2.2: Acquisition specific data preparation
    # ---------------------------------------------------------------------
    logging.info('STARTING: Data preparation process (acquisition)')
    # Calculate OfferID embedding representation as the mean of all product
    # embeddings associated to one OfferID
    # (one OfferID can represent more than one sku)
    offer_id_emb = offer_ids[['offer_id', 'material']].join(
        sku_embeddings, on='material', how='inner'
    ).copy().groupby('offer_id').mean()

    if 'material' in offer_id_emb:
        del offer_id_emb['material']

    # Generate an Offer ID hierarchy table. Rows here are the OfferID
    # unique values and the columns are the group and category of the
    # SKU's associated to that OfferID.
    # The max() function is safe to use here as all SKU's linked to one
    # OfferID are in the same group and category, so the max gives the
    # common (only) value in that columns.
    offer_id_hierarchy = offer_ids[
        ['offer_id', 'grupo_key', 'categoria_key', 'grupo_key_brnd_id']
    ].groupby('offer_id').max().copy()
    # Removes OfferIDs that don't have an embedding
    offer_id_hierarchy = offer_id_hierarchy[
        offer_id_hierarchy.index.isin(offer_id_emb.index)
    ]
    logging.info('ENDED: Data preparation process (acquisition)')
    logging.info('ENDED: Data preparation process')


    # ---------------------------------------------------------------------
    # Stage 3: Allocation process
    # ---------------------------------------------------------------------
    logging.info('Removing past run')
    deleteFromTable(
        table_ref=os.path.join('gbq_objects', 'personalized_coupons.json'),
        where_clause=f"""
            ciclo_interno = {n_cycle}
        """,
        gbq_client=gbq_client,
        if_not_exists='ignore',
        project=gcp_project,
    )

    logging.info('STARTING: Coupon allocation process')
    initial_client_number = 0
    while initial_client_number < n_customers:
        logging.info(f'initial_client_number = {initial_client_number}')
        logging.info(f'end_client_number = {initial_client_number + batch_size}')
        # Get a batch of client transactions. This table contains:
        # Customer id | product id | frequency | last transaction date
        basket_freq = readBigQuery(
            query=SQL_QUERIES['batch_transactions_table'].substitute(
                tmp_transaction_table_ref=tmp_transaction_table_ref,
                initial_client_number=initial_client_number,
                end_client_number=initial_client_number + batch_size,
            ),
            user=user,
            gbq_client=gbq_client,
        )

        # Adds month delta column with quantity of months passed from now
        # to the last day when the client bought something
        basket_freq['month_delta'] = (
            (
                pd.to_datetime(execution_date)
                - pd.to_datetime(basket_freq['last_transaction_date'])
            ) / pd.Timedelta(days=30)
        ).astype(int)


        logging.info('STARTING: Creating rankings')
        acquisition_rankings = createRankings(
            basket_freq=basket_freq,
            customer_emb=customer_embeddings,
            product_emb=offer_id_emb,
            sku_hierarchy=sku_hierarchy,
            offer_sku=offer_id_hierarchy,
            task='acquisition',
            )
        loyalty_rankings = createRankings(
            basket_freq=basket_freq,
            customer_emb=customer_embeddings,
            product_emb=sku_embeddings,
            sku_hierarchy=sku_hierarchy,
            offer_sku=offer_ids,
            task='loyalty',
            )
        logging.info('ENDED: Creating rankings')

        logging.info('STARTING: Aplying ranking process')
        loyalty_rankings = processRankings(
            rankings_df=loyalty_rankings,
            sku_hierarchy_df=sku_hierarchy,
            offer_sku_df=offer_ids,
            task='loyalty'
            )
        acquisition_rankings = processRankings(
            rankings_df=acquisition_rankings,
            sku_hierarchy_df=sku_hierarchy,
            offer_sku_df=offer_id_hierarchy,
            task='acquisition',
            loyalty_rankings_df=loyalty_rankings
            )
        logging.info('ENDED: Aplying process to rankings')

        logging.info('STARTING: Aplying maxCategory filter')
        acquisition_rankings =  maxCategoryFilter(
            rankings_df=acquisition_rankings,
            n_cycle_1=n_cycle,
            task='acquisition',
            )
        loyalty_rankings =  maxCategoryFilter(
            rankings_df=loyalty_rankings,
            n_cycle_1=n_cycle,
            task='loyalty',
            )
        logging.info('ENDED: Aplying maxCategory filter')

        loyalty_rankings['campaign_type_id'] = 2
        acquisition_rankings['campaign_type_id'] = 6
        # Actions for new benefits grid
        processed_rankings =  pd.DataFrame()
        for nivel in max_coupons_per_lvl:
            level_acquisition = filterAndMerge(
                acquisition_rankings,
                nivel,
                bm_priority
            )
            level_loyalty = filterAndMerge(
                loyalty_rankings,
                nivel,
                bm_priority
            )
            processed_level = reorganizeAllocations(
                acquisition_rankings=level_acquisition,
                loyalty_rankings=level_loyalty,
                max_coupons=max_coupons_per_lvl[nivel]
            )
            processed_rankings = pd.concat([
                processed_rankings, processed_level
            ])

        processed_rankings['store_banner'] = store_banner

        processed_rankings = processed_rankings.drop_duplicates()

        # Upload
        uploadFrame(
            df=processed_rankings[[
                'store_banner', 'ciclo', 'customer_key',
                'offer_id', 'rank', 'campaign_type_id'
            ]].copy(),
            table_ddl_json_path=os.path.join('gbq_objects', 'personalized_coupons.json'),
            project=gcp_project,
            gbq_client=gbq_client,
            if_exists='append',
        )

        initial_client_number += batch_size
    logging.info('ENDED: Coupon allocation process')
    logging.info(':D')


if __name__ == '__main__': main()
