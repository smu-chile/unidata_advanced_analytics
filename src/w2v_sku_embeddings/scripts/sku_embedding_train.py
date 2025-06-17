"""Embedding trainning script."""
from __future__ import annotations

# Default
import os
import logging
import argparse
from string import Template
from logging import config

# pip
import pendulum
from gensim.models import Word2Vec
from google.cloud.bigquery import Client

# Own
import common.gcp_extended.bigquery as gbq_extended
from common.constants import LOGGING_CONFIG, SHORT_STORE_BANNERS
from common.utils.queries import QueryDict


# -------------------------------------------------------------------------
# Package config
# -------------------------------------------------------------------------
# Logging config
config.dictConfig(LOGGING_CONFIG)
# Parser
parser = argparse.ArgumentParser()
parser.add_argument(
    '--uuid', type=str, required=True,
    help='Unique identifier of the current run. Used to differenciate GCP objects'
)
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
    help='SMU format in which the embeddings will be allocated'
)
parser.add_argument(
    '--epochs', type=int, default=10,
    help='Trainning epochs'
)
parser.add_argument(
    '--batch_size', type=int, default=100,
    help='Batch size'
)
parser.add_argument(
    '--sg', type=int, default=1, choices=[0, 1],
    help='Trainning algorithm 1: skip-gram, 0: CBOW'
)
parser.add_argument(
    '--hs', type=int, default=0, choices=[0, 1],
    help='Activation. 1: hierarchical softmax, 0: negative sampling'
)
parser.add_argument(
    '--min_count', type=int, default=100,
    help='Ignores all words with total frequency lower than this'
)
parser.add_argument(
    '--window_size', type=int, default=100,
    help='Maximum distance between the current and predicted word within a sentence'
)
parser.add_argument(
    '--ns_exponent', type=float, default=-0.5,
    help='The exponent used to shape the negative sampling distribution'
)
parser.add_argument(
    '--embedding_dim', type=int, default=100,
    help='Dimensionality of the word vectors'
)
parser.add_argument(
    '--n_negative_samples', type=int, default=20,
    help='How many "noise words" should be drawn '
)
parser.add_argument(
    '--cart_lenght', nargs=2, default=[2, 100],
    help='Min and max values for the cart lenght filter'
)


# -------------------------------------------------------------------------
# SQL Queries
# -------------------------------------------------------------------------
SQL_QUERIES = QueryDict({
    'last_year_transactions':
    """
    WITH past_year_transactions AS (
        SELECT
            customer_key,
            txn_key,
            basket_quantity,
            sku_product

        FROM `${gcp_project}.ML_LAB.VW_SALES_ITEM` sales_item

        INNER JOIN `${gcp_project}.ML_LAB.VW_SALES_BASKET` sales_basket
        USING (txn_key, customer_key, store_id, itm_txn_fcn_tp_dsc)

        INNER JOIN `${gcp_project}.ML_LAB.VW_DIM_STORE` dim_store
        USING (store_id)

        INNER JOIN (
            SELECT
                sku_product
            FROM `${gcp_project}.ML_LAB.VW_DIM_PRODUCT`
            GROUP BY 1
            HAVING MAX(neg_dsc) NOT IN ('SERVICIOS COMERCIALES', 'NO RETAIL')
        ) dim_product
        USING (sku_product)

        LEFT JOIN (
            SELECT
                market_basket_key,
                TRUE AS from_other_ecommerce
            FROM `${gcp_project}.ML_LAB.VW_FACT_MARKET_BASKET_E_COMMERCE`
            WHERE canal_venta IN ('PEDIDOS YA','CORNER SHOP','RAPPI','RAPPI TURBO')
        ) external_ecommerce_filter
        ON sales_item.market_basket_key = external_ecommerce_filter.market_basket_key

        WHERE
            sales_basket.transaction_date >= DATE('${execution_date}') - INTERVAL 1 MONTH
            AND sales_basket.transaction_date < DATE('${execution_date}')
            AND sales_item.transaction_date >= DATE('${execution_date}') - INTERVAL 1 MONTH
            AND sales_item.transaction_date < DATE('${execution_date}')
            AND channel = 'SALA'
            AND itm_txn_fcn_tp_dsc = 'V'
            AND transaction_type IN ('TN','TF','BX','B','BE','F','NC')
            AND store_banner = '${store_banner}'
            AND from_other_ecommerce IS NULL
    ),

    customer_outliers AS (
        SELECT
            customer_key,
            TRUE AS is_outlier
        FROM past_year_transactions
        GROUP BY 1
        HAVING COUNT(txn_key) > 350
    )

    SELECT
        *,
        ROW_NUMBER() OVER () AS basket_index
    FROM (
        SELECT
            txn_key,
            ARRAY_AGG(CAST(CAST(sku_product AS BIGINT) AS STRING)) AS sku_product,
        FROM past_year_transactions
        LEFT JOIN customer_outliers
        USING (customer_key)
        WHERE
            is_outlier IS NULL
            AND basket_quantity > 1
            AND basket_quantity <= 200
        GROUP BY 1
    )
    WHERE
        ${min_cart_lenght} < ARRAY_LENGTH(sku_product)
        AND ARRAY_LENGTH(sku_product) < ${max_cart_lenght}
    """,

    'last_year_transactions_batch':
    """
    SELECT sku_product
    FROM ${gcp_project}.ML_LAB.TMP_W2V_LAST_YEAR_TRANSACTIONS_${uuid}
    WHERE basket_index >= ${start_index}
        AND basket_index < ${end_index}
    """
})


# -------------------------------------------------------------------------
# Functions and Classes
# -------------------------------------------------------------------------
class QueryDataLoader:
    """Custom dataloader for trainning the Word2Vec embedding model.

    The class generates an iterator that makes various sql queries (of size
    batch_size) to an Athena table. Then pass the registers of the response
    one by one. Once the Athena table is exhaust ends.

    Parameters
    ----------
    query : string.Template
        Query from which the trainning samples will be obtained. must be a
        template with the parameters `offset` and `limit`.
    batch_size : int
        Size of the query batches.
    max_retries : int
        Size of the query batches.
    """
    def __init__(
            self, user: str, query: Template, batch_size: int,
            gbq_client: Client, max_retries: int=3,
        ):
        self.gbq_client = gbq_client

        self.user = user
        self.query = query
        self.batch_size = batch_size
        self.max_retries = max_retries

        # Set index at 0 on class instance build
        self.start_index = 0


    def __iter__(self):
        while True:
            # Retry query handler
            retries = 0
            response_df = None
            while retries < self.max_retries:
                try:
                    # Make query
                    response_df = gbq_extended.readBigQuery(
                        user=self.user,
                        query=self.query.substitute(
                            # Rolling window
                            start_index=self.start_index,
                            end_index=self.start_index + self.batch_size
                        ),
                        gbq_client=self.gbq_client
                    )
                # TODO(ecastrot): Stablish a specific cloud error
                except Exception as e:
                    # Catches especific error
                    # TODO(ecastrot): Catch specific error message
                    if 'Query exhausted resources at this scale factor' in str(e):
                        retries += 1
                    # Some other error ocurred. Raise
                    else:
                        raise
                    # Save last error
                    last_error = e

                # Last retry wasn't successfull. Raise
                if retries == self.max_retries:
                    err_msg = ('Query never was executed correctly. '
                            f'The last error was: {last_error}')
                    raise Exception(err_msg)

                # DataFrame was assigned correctly
                if response_df is not None:
                    break

            # Query is empty, the table was exhausted
            if response_df.empty:
                # Restore offset
                self.start_index = 0
                break
            # Query is OK
            else:
                # Filter by column and cart lenght
                response_df = response_df['sku_product']

                # Augment offset
                self.start_index += self.batch_size

                # Map list of ints into list of strings
                yield from response_df


# -------------------------------------------------------------------------
#                        Main Function
# -------------------------------------------------------------------------
def main() -> None:  # noqa: D103
    # ----------
    # Parameters
    # ----------
    args = vars(parser.parse_args())
    # Environment
    uuid: str = args['uuid'].replace('-', '_')
    user: str = args['project_name']
    gcp_project: str = args['gcp_project']
    execution_date: pendulum.Date = pendulum.date(
        *list(map(int, args['execution_date'].split('-')))
    )
    store_banner: str = args['store_banner']
    short_store_banner: str = SHORT_STORE_BANNERS[store_banner]

    # Model
    epochs: int = args['epochs']
    batch_size: int = args['batch_size']
    sg: int = args['sg']
    hs: int = args['hs']
    min_count: int = args['min_count']
    window_size: int = args['window_size']
    ns_exponent: float = args['ns_exponent']
    embedding_dim: int = args['embedding_dim']
    n_negative_samples: int = args['n_negative_samples']

    # Filters
    cart_lenght_filter: list[int] = args['cart_lenght']


    # --------------------
    # Automatic parameters
    # --------------------
    # Output
    output_uri = (
        'gs://cl-bigdata-analytics-dev-us-sandbox-models/'
        f'{short_store_banner}/'
        f'{user}/'
        f"{execution_date.format('YYYYMM')}.kv"
    )

    logging.info('Parameters:')
    logging.info(f'Execution date: {execution_date.isoformat()}')
    logging.info(f'Output URI: {output_uri}')

    logging.info('Hyperparameters:')
    logging.info(f'sg: {sg}')
    logging.info(f'hs: {hs}')
    logging.info(f'Epochs: {epochs}')
    logging.info(f'Min count: {min_count}')
    logging.info(f'Batch size: {batch_size}')
    logging.info(f'Window size: {window_size}')
    logging.info(f'NS Exponent: {ns_exponent}')
    logging.info(f'Embedding size: {embedding_dim}')
    logging.info(f'Number of negative samples: {n_negative_samples}')
    logging.info(f'Basket lenght filter: {cart_lenght_filter}')

    # ----------
    # GBQ Client
    # ----------
    gbq_client = Client()

    # --------------------------------
    # Create table with trainning data
    # --------------------------------
    logging.info('Building table with last year transactions')
    gbq_extended.createTableAsSelect(
        query=SQL_QUERIES['last_year_transactions'].substitute(
            store_banner=store_banner,
            execution_date=execution_date,
            gcp_project=gcp_project,
            min_cart_lenght=cart_lenght_filter[0],
            max_cart_lenght=cart_lenght_filter[1],
        ),
        table_ref=f'{gcp_project}.ML_LAB.TMP_W2V_LAST_YEAR_TRANSACTIONS_{uuid}',
        gbq_client=gbq_client,
        use_legacy_sql=False
    )


    expiration_date = pendulum.now(tz='America/Santiago').add(days=1)
    logging.info(f'Setting its expiration date to {expiration_date}')
    gbq_extended.setTableExpiration(
        table_ref=f'{gcp_project}.ML_LAB.TMP_W2V_LAST_YEAR_TRANSACTIONS_{uuid}',
        expiration=expiration_date,
        gbq_client=gbq_client,
    )


    # ---------
    # Trainning
    # ---------
    # Instance dataloader iterator
    logging.info('Building the DataLoader')
    sentences = QueryDataLoader(
        user=user,
        query=Template(
            SQL_QUERIES['last_year_transactions_batch'].safe_substitute(
                gcp_project=gcp_project,
                uuid=uuid,
            )
        ),
        batch_size=batch_size,
        gbq_client=gbq_client,
        max_retries=1,
    )

    # Train the model
    logging.info('Starting the trainning')
    model = Word2Vec(
        sentences,
        sg=sg,
        vector_size=embedding_dim,
        window=window_size,
        min_count=min_count,
        workers=os.cpu_count(),
        hs=hs,
        negative=n_negative_samples,
        ns_exponent=ns_exponent,
        epochs=epochs
    )

    # Save the keyed vectors
    model.wv.save(fname_or_handle=output_uri)


if __name__ == '__main__':
    main()
