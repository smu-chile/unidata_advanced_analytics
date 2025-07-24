import os
import logging
import argparse
from logging import config

from google.cloud.bigquery import Client

from common.constants import LOGGING_CONFIG
from common.databases.queries import QueryDict
from common.databases.postgresql import readPostgresQuery
from common.gcp_extended.bigquery import uploadFrame


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
SQL_QUERIES = QueryDict({
    'get_data': (
        """
        SELECT
            CAST(product_ean AS BIGINT) AS ean,
            brand_name AS brand,
            description,
            flavor,
            CAST(REPLACE(drained_size_value, ',', '.') AS FLOAT) AS drained_size_value,
            CAST(REPLACE(num_portions, ',', '.') AS FLOAT) AS num_portions,
            basic_unit,
        """

        + ''.join([
            f"\tCAST(REPLACE({col + suffix}, ',', '.') AS FLOAT) AS {col + suffix},\n"
            if suffix == '_value'
            else f'\t{col + suffix},\n'
            for col in [
                'size', 'portion', 'energy', 'protein', 'fat_total', 'fat_sat',
                'fat_mono', 'fat_poli', 'fat_trans', 'fat_cholesterol', 'carb',
                'sugars', 'fiber', 'sodium'
            ]
            for suffix in [
                '_value', '_unit'
            ]
        ])

        + ''.join([
            f'\tCAST({col} AS INTEGER) AS {col},\n' for col in [
                'minsal_cl_high_sugar', 'minsal_cl_high_saturated_fat', 'minsal_cl_high_sodium',
                'minsal_cl_high_calories', 'aplv_suitable', 'gluten_free', 'lactose_free',
                'kosher', 'vegan', 'vegetarian', 'diabetes_suitable', 'soy_free',
                'egg_free', 'fish_free', 'seafood_free', 'peanut_free', 'nuts_free',
                'walnuts_free', 'sulphite_free', 'wheat_free',
            ]
        ])

        + '\talcohol_by_volume,\n'
        + '\talcohol_proof\n'
        + 'FROM catalogo.ok_to_shop_v2'
    )
})


# -------------------------------------------------------------------------
#  Main function
# -------------------------------------------------------------------------
def main() -> None:
    user = 'ingest'  # noqa: F841
    # Parse input variables
    args = vars(parser.parse_args())
    execution_date: str = args['execution_date']
    gcp_project_id: str = args['project_id']
    logging.info(f'execution_date: {execution_date}')

    # Static variables
    gbq_client = Client()

    # Get data
    data = readPostgresQuery(SQL_QUERIES['get_data'])

    # Upload data to the table
    uploadFrame(
        data,
        table_ddl_json_path=os.path.join('gbq_objects', 'dim_ok_to_shop.json'),
        project=gcp_project_id,
        gbq_client=gbq_client,
        if_exists='replace'
    )


if __name__ == '__main__':
    main()
