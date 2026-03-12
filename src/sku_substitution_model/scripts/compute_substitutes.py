# Default  # noqa: D100, ERA001
import os
import re
import logging
import argparse
from string import ascii_lowercase
from typing import Literal
from logging import config

# pip
import numpy as np
import pandas as pd
import pendulum
from google.cloud.bigquery import Client
from scipy.spatial.distance import cdist
from sklearn.feature_extraction.text import TfidfVectorizer

# Own
import common.gcp_extended.bigquery as gbq_extended
from common.constants import LOGGING_CONFIG
from common.ml.recsys import meltedCosineDistance
from common.databases.queries import QueryDict
from common.utils.data_transform import normalizeText


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
    '--transacted_months', default=3, type=int,
    help='Min number months in which a product must were bought for it to be considered'
)
parser.add_argument(
    '--n_substitutes', default=99999999, type=int,
    help='Max number of substitutes per product'
)


# -------------------------------------------------------------------------
#  SQL Queries
# -------------------------------------------------------------------------
SQL_QUERIES = QueryDict({
    'product_names_nielsen':
    """
    SELECT
        segmento,
        subsegmento,
        tipo,
        variedad,
        upc
    FROM cl-bigdata-analytics-preprod.MARKET_SHARE.NIELSEN_ANUAL_VENTA_MERCADO_JERARQUIA_UPC
    """,

    'product_names_sap':
    """
    SELECT
        CAST(sku_product AS INT) AS sku,
        CAST(ean AS BIGINT) AS upc,
        nm AS product_description,
        grupo_dsc AS sub_category_description,
        cat_dsc AS category_description,
        cat_h_dsc AS category_description_h

    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY SKU_PRODUCT) AS SKU_INDEX
        FROM ${gcp_project}.CDA_VISTAS.VW_DIM_PRODUCT
    )
    WHERE
        SKU_INDEX = 1
    GROUP BY 1,2,3,4,5,6
    """,

    'sku_containers':
    """
    SELECT
        sku_product,
        codigo,
        descripcion
    FROM `${gcp_project}.DS_CDA_VW_SMU.DW_VW_DIM_SKU_ATTR` attr
    LEFT JOIN `${gcp_project}.DS_CDA_VW_SMU.DW_VW_DIM_ENVASE` dim_envase
    ON attr.envase = dim_envase.codigo
    WHERE envase IS NOT NULL
    """,

    'brand_sophistication_score':
    """
    SELECT
        category_description,
        brand,
        hm_pu_ppum
    FROM ${gcp_project}.ML_LAB.BRAND_SOPHISTICATION_SCORE
    WHERE
        date = '${execution_date}'
        AND store_banner = '${store_banner}'
    """,

    'ots_embeddings': (
        """
        SELECT
            sku,
            MIN(minsal_cl_high_saturated_fat) AS minsal_cl_high_saturated_fat,
            MIN(minsal_cl_high_sodium) AS minsal_cl_high_sodium,
            MIN(minsal_cl_high_calories) AS minsal_cl_high_calories,
            MIN(aplv_suitable) AS aplv_suitable,
            MIN(gluten_free) AS gluten_free,
            MIN(lactose_free) AS lactose_free,
            MIN(kosher) AS kosher,
            MIN(vegan) AS vegan,
            MIN(vegetarian) AS vegetarian,
            MIN(diabetes_suitable) AS diabetes_suitable,
            MIN(soy_free) AS soy_free,
            MIN(egg_free) AS egg_free,
            MIN(fish_free) AS fish_free,
            MIN(seafood_free) AS seafood_free,
            MIN(peanut_free) AS peanut_free,
            MIN(nuts_free) AS nuts_free,
            MIN(walnuts_free) AS walnuts_free,
            MIN(sulphite_free) AS sulphite_free,
            MIN(wheat_free) AS wheat_free
        FROM (
            SELECT
                EAN,
        """  # noqa: S608
        + ','.join([
            f"""
                CASE
                    WHEN {col_name} = 2 THEN 1
                    WHEN {col_name} IS NULL THEN 0
                    ELSE {col_name}
                END AS {col_name}
            """ for col_name in [
                'MINSAL_CL_HIGH_SUGAR', 'MINSAL_CL_HIGH_SATURATED_FAT',
                'MINSAL_CL_HIGH_SODIUM', 'MINSAL_CL_HIGH_CALORIES',
                'APLV_SUITABLE', 'GLUTEN_FREE', 'LACTOSE_FREE', 'KOSHER',
                'VEGAN', 'VEGETARIAN', 'DIABETES_SUITABLE', 'SOY_FREE',
                'EGG_FREE', 'FISH_FREE', 'SEAFOOD_FREE', 'PEANUT_FREE',
                'NUTS_FREE', 'WALNUTS_FREE', 'SULPHITE_FREE', 'WHEAT_FREE'
            ]
        ])
        + """
            FROM `${gcp_project}.ECOMMERCE.DIM_OK_TO_SHOP`
        )
        INNER JOIN (
            SELECT *
            FROM (
                SELECT
                    CAST(ean AS BIGINT) AS EAN,
                    CAST(sku_product AS BIGINT) AS SKU,
                    ROW_NUMBER() OVER (PARTITION BY ean) AS EAN_INDEX
                FROM `${gcp_project}.CDA_VISTAS.VW_DIM_PRODUCT`
            )
            WHERE EAN_INDEX = 1
        )
        USING (EAN)
        GROUP BY 1
        """
    ),

    'w2v_embeddings':
    """
    WITH unique_products AS (
        SELECT
            CAST(sku_product AS INT) AS sku,
            cat_dsc AS category_description,
            brand_desc AS brand,
            grupo_dsc AS sub_category_description,
            MAX(
                CASE
                    WHEN UM_CONTENIDO IN ('ML', 'M') THEN CAST(contenido_bruto AS BIGNUMERIC)
                    -- L to mL and Kg to g
                    WHEN UM_CONTENIDO IN ('L', 'KG') THEN CAST(contenido_bruto AS BIGNUMERIC) * 1000
                    WHEN UM_CONTENIDO IN ('ST', 'DIS', 'CS') THEN 1
                END
            ) AS units
        FROM (
            SELECT
                *,
                ROW_NUMBER() OVER (PARTITION BY sku_product) AS SKU_INDEX
            FROM `${gcp_project}.CDA_VISTAS.VW_DIM_PRODUCT`
        )
        WHERE
            SKU_INDEX = 1
        GROUP BY 1,2,3,4
    ),

    bought_on_last_n_months_filter AS (
        SELECT DISTINCT
            CAST(sku_product AS BIGINT) AS sku
        FROM `${gcp_project}.CDA_VISTAS.VW_SALES_ITEM` sales_item
        INNER JOIN `${gcp_project}.CDA_VISTAS.VW_DIM_STORE` dim_store
        USING (store_id)
        WHERE
            transaction_date >= DATE('${execution_date}') - INTERVAL ${transacted_months} MONTH
            AND transaction_date < DATE('${execution_date}')
            AND transaction_type IN ('NE','FX','BX','B ','BE','FE','F ','NC','TN','TF')
            AND store_banner = '${store_banner}'
            AND itm_txn_fcn_tp_dsc = 'V'
            AND value > 0
            AND sku_product <> 'None'
    )

    SELECT *
    FROM (
        SELECT sku, ${aux_columns}
        FROM `${gcp_project}.ML_LAB.W2V_SKU_EMBEDDINGS`
        WHERE
            date = '${execution_date}'
            AND store_banner = '${store_banner}'
    )
    INNER JOIN unique_products
    USING(sku)
    INNER JOIN bought_on_last_n_months_filter
    USING(sku)
    """,  # noqa: E501
})


# -------------------------------------------------------------------------
#  Functions and classes
# -------------------------------------------------------------------------
def splitter(text):  # noqa: ANN001, ANN201
    # TODO(ecastrot): Add documentation  # noqa: FIX002
    """"""  # noqa: D419
    return ' '.join(re.findall(r'[a-z]+|\d+|[^\w\s]+', text))

def computeSubstitutes(
    skus: pd.DataFrame,
    sku_w2v_embeddings: pd.DataFrame,
    sku_ots_embeddings: pd.DataFrame,
    sku_tfidf_embeddings: pd.DataFrame,
    sku_containers: pd.DataFrame,
    sku_units: pd.DataFrame,
    sku_sophistication_scores: pd.DataFrame,
    score_ponderations: dict[str, float],
    output: Literal['vector', 'matrix', 'rank'],
    top_k: int = 0
) -> pd.DataFrame:
    # TODO(ecastrot): Add documentation  # noqa: FIX002
    """"""  # noqa: D419
    if output not in ['vector', 'matrix', 'rank']:
        err_msg = (
            'Erroneous value for output argument.'
            ' Must be one of "vector", "matrix" or "rank"'
        )
        raise ValueError(err_msg)

    # Get sku embeddings for products in the bag
    bop_w2v_embeddings = sku_w2v_embeddings.merge(
        skus,
        how='inner',
        on='sku'
    ).set_index(
        'sku'
    )

    # Returns empty DataFrame if no product has embedding
    if bop_w2v_embeddings.empty: return pd.DataFrame()

    # Get TF-IDF embeddings for products in the bag
    bop_tfidf_embeddings = skus.merge(
        sku_tfidf_embeddings,
        how='inner',
        on='sku'
    ).set_index(
        'sku'
    )

    # Get ok to shop embeddings for products in the current subcat
    bop_ots_embeddings = sku_ots_embeddings.merge(
        skus,
        how='inner',
        on='sku'
    # Add missing Ok to Shop vectors if there were any
    ).merge(
        right=pd.Series(bop_w2v_embeddings.index, name='sku'),
        on='sku',
        how='right'
    # Fill with no info flag
    ).fillna(
        0
    # Set sku as index
    ).set_index(
        'sku'
    )

    # Calculate autodistance (Cosine) between sku products
    substitutes = meltedCosineDistance(
        bop_w2v_embeddings,
        antiparallel_filter=False,
    ).rename(columns={
        'cosine_distance': 'w2v_distance',
    }).merge(
        right=meltedCosineDistance(
            bop_tfidf_embeddings,
            antiparallel_filter=False,
        ).rename(columns={
            'cosine_distance': 'tfidf_distance',
        }),
        how='inner',
        on=['sku', 'other_sku']
    ).merge(
        # Calculate autodistance (Roger-Stanimoto) between ok to shop
        # products
        right=pd.DataFrame(
            data=cdist(
                bop_ots_embeddings.astype(float),
                bop_ots_embeddings.astype(float),
                'rogerstanimoto'
            ),
            index=bop_ots_embeddings.index,
            columns=bop_ots_embeddings.index.rename(
                'other_sku'
            )
        ).reset_index().melt(
            id_vars='sku',
            var_name='other_sku',
            value_name='roger_stanimoto',
        ).rename(
            columns={'roger_stanimoto': 'ots_distance'}
        ),
        how='inner',
        on=['sku', 'other_sku']
    )

    # If returning rank or vector, auto-distances must be deleted
    if output in ['rank', 'vector']:
        substitutes = substitutes.query(
            'sku != other_sku',
            engine='python',
        )

    # Add units
    substitutes = substitutes.merge(
        sku_units,
        on='sku',
        how='inner'
    ).merge(
        sku_units.rename(columns={
            'sku': 'other_sku',
            'units': 'other_units',
        }),
        on='other_sku',
        how='inner',
    )
    substitutes['units_diff'] = (
        substitutes['units'] - substitutes['other_units']
    ).abs().astype(np.float32)

    # Add HM(PU, PPUM)
    substitutes = substitutes.merge(
        sku_sophistication_scores,
        on='sku',
        how='inner'
    ).merge(
        sku_sophistication_scores.rename(columns={
            'sku': 'other_sku',
            'hm_pu_ppum': 'other_hm_pu_ppum',
        }),
        on='other_sku',
        how='inner',
    )
    # Compute difference between HM(pu, ppum)
    substitutes['hm_pu_ppum_diff'] = (
        substitutes['hm_pu_ppum'] - substitutes['other_hm_pu_ppum']
    ).abs().astype(np.float32)

    # Add container
    substitutes = substitutes.merge(
        sku_containers,
        on='sku',
        how='left'
    ).merge(
        sku_containers.rename(columns={
            'sku': 'other_sku',
            'code': 'other_code',
        }),
        on='other_sku',
        how='left',
    ).fillna(-1)
    # Compute difference between containers
    substitutes['container_diff'] = (
        substitutes['code'] == substitutes['other_code']
    ).astype(np.float32)

    # Return raw pair vectors
    if output == 'vector':
        return substitutes[[
            'sku', 'other_sku',
            'w2v_distance',
            'tfidf_distance',
            'ots_distance',
            'units_diff',
            'hm_pu_ppum_diff',
            'container_diff'
        ]]

    # Calcultate unified score (All of the metrics need to be minimized)
    substitutes['distance'] = (
        score_ponderations['w2v_distance_filter'] * substitutes['w2v_distance']
        + score_ponderations['tfidf_distance_filter'] * substitutes['tfidf_distance']
        + score_ponderations['ots_distance_filter'] * substitutes['ots_distance']
        + score_ponderations['other_units_filter'] * substitutes['units_diff']
        + score_ponderations['hm_pu_ppum_filter'] * substitutes['hm_pu_ppum_diff']
        + score_ponderations['container_filter'] * substitutes['container_diff']
    )

    substitutes = substitutes.drop(
        columns=[
            'w2v_distance', 'tfidf_distance', 'ots_distance',
            'units', 'other_units', 'units_diff',
            'hm_pu_ppum', 'other_hm_pu_ppum', 'hm_pu_ppum_diff',
            'code', 'other_code', 'container_diff'
        ]
    )

    if output == 'matrix':
        return substitutes.pivot_table(
            index='sku',
            columns='other_sku',
            values='distance'
        )

    # Change column name other_sku to substitute
    substitutes = substitutes.rename(columns={
        'other_sku': 'substitute',
        'distance': 'score'
    # Minimize unified score
    }).sort_values(
        'score', ascending=True
    ).groupby(
        ['sku'], sort=False
    # Get top n substitutes
    ).head(
        top_k
    )

    # Change score for rank
    substitutes['relevance'] = substitutes.groupby(
        'sku'
    )['score'].rank(
        ascending=True, method='first'
    )

    return substitutes.astype({
        'sku': 'Int64',
        'substitute': 'Int64',
        'score': 'double',
        'relevance': 'Int32',
    })


# -------------------------------------------------------------------------
#                        Main Function
# -------------------------------------------------------------------------
def main() -> None:  # noqa: D103
    args = vars(parser.parse_args())
    # Environment parameters
    user: str = args['project_name']
    gcp_project: str = args['gcp_project']
    gcp_project_cda = {
        'cl-bigdata-analytics': 'cl-cda-qa',
        'cl-bigdata-analytics-dev': 'cl-cda-qa',
        'cl-bigdata-analytics-preprod': 'cl-cda-prod',
        'cl-bigdata-analytics-prod': 'cl-cda-prod',
    }[args['gcp_project']]
    execution_date: pendulum.Date = pendulum.date(
        *list(map(int, args['execution_date'].split('-')))
    ).replace(
        day=1
    ).isoformat()
    store_banner: str = args['store_banner']
    transacted_months = args['transacted_months']
    n_substitutes = args['n_substitutes']

    # Static parameters
    gbq_client = Client()

    logging.info(f'execution_date: {execution_date}')

    logging.info('Obtainning data')
    sku_ots_embeddings = gbq_extended.readBigQuery(
        SQL_QUERIES['ots_embeddings'].substitute(
            gcp_project=gcp_project,
        ),
        user=user,
        gbq_client=gbq_client,
    )

    sku_w2v_embeddings = gbq_extended.readBigQuery(
        SQL_QUERIES['w2v_embeddings'].substitute(
            gcp_project=gcp_project,
            store_banner=store_banner,
            transacted_months=transacted_months,
            execution_date=execution_date,
            aux_columns=', '.join([f'dim_{i}' for i in range(100)])
        ),
        user=user,
        gbq_client=gbq_client,
    )

    sophistication_score = gbq_extended.readBigQuery(
        SQL_QUERIES['brand_sophistication_score'].substitute(
            gcp_project=gcp_project,
            execution_date=execution_date,
            store_banner=store_banner,
        ),
        user=user,
        gbq_client=gbq_client,
    )

    product_names = gbq_extended.readBigQuery(
        query=SQL_QUERIES['product_names_sap'].substitute(
            gcp_project=gcp_project,
        ),
        user=user,
        gbq_client=gbq_client,
    )

    nielsen_product_names = gbq_extended.readBigQuery(
        query=SQL_QUERIES['product_names_nielsen'].substitute(
            gcp_project=gcp_project,
        ),
        user=user,
        gbq_client=gbq_client,
    ).drop_duplicates()

    sku_containers = gbq_extended.readBigQuery(
        query=SQL_QUERIES['sku_containers'].substitute(
            gcp_project=gcp_project_cda,
        ),
        user=user,
        gbq_client=gbq_client,
    ).astype({
        'sku_product': 'Int32',
        'codigo': 'Int32',
        'descripcion': str,
    }).rename(columns={
        'sku_product': 'sku',
        'codigo': 'code',
        'descripcion': 'container',
    })

    sku_hierarchy = product_names[[
        'sku', 'sub_category_description', 'category_description', 'category_description_h'
    ]].dropna().drop_duplicates()

    product_names = product_names.drop(columns=[
        'sub_category_description', 'category_description'
    ]).dropna()
    logging.info('Data obtained!')

    # Change upc to int64
    nielsen_product_names = nielsen_product_names[
        ~nielsen_product_names['upc'].str.lower().str.contains(
            r'|'.join(list(ascii_lowercase))
        )
    ].astype({'upc': 'int64'})

    # Remove unused data
    for col in nielsen_product_names.columns[:-1]:
        nielsen_product_names[col] = nielsen_product_names[col].str.split('/').str[-1]

    aux = nielsen_product_names.merge(
        (nielsen_product_names['upc'].value_counts() > 1).reset_index().rename(columns={
            'count': 'counts_filter',
        }),
        how='left',
        on='upc'
    )

    aux['other_filter'] = aux.astype(str).map(
        lambda x: 'OTROS' in x
    ).sum(
        axis=1
    )

    nielsen_product_names = pd.concat([
        # Fix duplicates
        aux[
            aux['counts_filter']
        ].sort_values(
            'other_filter',
            ascending=True,
        ).groupby(
            'upc'
        ).head(
            1
        ),

        # Without duplicates
        aux[
            ~aux['counts_filter']
        ]
    ],
    ).drop(columns=[
        'other_filter',
        'counts_filter',
    ])

    product_names['product_description'] = product_names[
        'product_description'
    ].apply(
        normalizeText,
        replace_spaces=False
    ).apply(
        splitter
    )

    # Build filters
    sku_units = sku_w2v_embeddings[['sku', 'units']]

    # Data tables
    sku_category_brand = sku_w2v_embeddings[['sku', 'category_description', 'brand']]

    # Drop unused columns
    sku_w2v_embeddings = sku_w2v_embeddings.drop(
        columns=['category_description', 'brand', 'sub_category_description', 'units']
    )

    # Add sku to sophistication_score
    sophistication_score = sophistication_score.merge(
        sku_category_brand,
        on=['brand', 'category_description']
    # Drop unused columns
    ).drop(
        columns=['brand', 'category_description']
    )

    # Better names using nielsen data
    product_names = product_names.merge(
        nielsen_product_names,
        how='left',
        on='upc'
    ).drop(columns=[
        'upc'
    ]).merge(
        sku_containers[['sku', 'container']],
        how='left',
        on='sku'
    ).replace({
        pd.NA: '',
        '_OTROS SEGMENTOS': '',
        '_OTROS SUB SEGMENTOS': '',
        '_OTROS TIPO': '',
        '_OTROS VARIEDAD': '',
        'SIN ATRIBUTO': '',
        'SIN ENVASE': '',
    })

    product_names['norm_product_description'] = (
        product_names['product_description'].str.split(' ')
        + product_names['segmento'].apply(normalizeText, replace_spaces=' ').str.split(' ')
        + product_names['subsegmento'].apply(normalizeText, replace_spaces=' ').str.split(' ')
        + product_names['tipo'].apply(normalizeText, replace_spaces=' ').str.split(' ')
        + product_names['variedad'].apply(normalizeText, replace_spaces=' ').str.split(' ')
        + product_names['container'].apply(normalizeText, replace_spaces=' ').str.split(' ')
    ).apply(
        dict.fromkeys
    ).apply(
        ' '.join
    ).apply(
        normalizeText, replace_spaces=' '
    ).apply(
        splitter
    )
    product_names = product_names.drop(columns=[
        'segmento', 'subsegmento', 'tipo', 'variedad', 'container'
    ])
    # Remove text from product descriptions
    for t in [
        'gr',
        'g',
        'otros'
    ]:
        product_names['norm_product_description'] = (
            ' ' + product_names['norm_product_description'] + ' '
        ).str.replace(f' {t} ', ' ')
    product_names['norm_product_description'] = product_names['norm_product_description'].apply(
        normalizeText,
        replace_spaces=' '
    )

    # Build TF-IDF embeddings for the products
    aux = product_names.merge(
        sku_w2v_embeddings['sku'],
        on='sku',
        how='inner'
    )
    sku_tfidf_embeddings = pd.DataFrame(
        TfidfVectorizer().fit_transform(
            aux['norm_product_description'].to_list()
        ).toarray(),
        index=aux['sku']
    )

    logging.info(f'There are {len(sku_ots_embeddings.index):,} products with Ok to Shop embeddings')  # noqa: E501
    logging.info(f'There are {len(sku_ots_embeddings.index):,} products with Word2Vec embeddings')

    logging.info('Removing past run data')
    gbq_extended.deleteFromTable(
        os.path.join('gbq_objects', 'sku_substitutes_by_category.json'),
        project=gcp_project,
        where_clause=f"""
            date = DATE('{execution_date}')
            AND store_banner = '{store_banner}'
        """,
        gbq_client=gbq_client,
    )

    logging.info('Computing substitutes...')
    for category in sku_hierarchy['category_description'].drop_duplicates():
        bag_of_products = sku_hierarchy[
            sku_hierarchy['category_description'] == category
        ]['sku'].reset_index(
            drop=True
        ).to_frame()

        logging.info(f'Computing {category.lower()} substitutes')
        substitutes = computeSubstitutes(
            skus=bag_of_products,
            sku_w2v_embeddings=sku_w2v_embeddings,
            sku_tfidf_embeddings=sku_tfidf_embeddings,
            sku_ots_embeddings=sku_ots_embeddings,
            sku_containers=sku_containers[['sku', 'code']],
            sku_units=sku_units,
            sku_sophistication_scores=sophistication_score,
            score_ponderations={
                'w2v_distance_filter': 8e-1,
                'tfidf_distance_filter': 0e-1,
                'ots_distance_filter': 4e-2,
                'other_units_filter': 1e-3,
                'hm_pu_ppum_filter': 4e-2,
                'container_filter': 0e-1,
            },
            output='rank',
            top_k=n_substitutes
        )

        # Pass if substitutes is empty
        if substitutes.empty: continue

        substitutes.insert(0, 'date', execution_date)
        substitutes.insert(1, 'store_banner', store_banner)


        gbq_extended.uploadFrame(
            substitutes,
            table_ddl_json_path=os.path.join('gbq_objects', 'sku_substitutes_by_category.json'),
            project=gcp_project,
            gbq_client=gbq_client,
            if_exists='append'
        )

    logging.info('Done!')


if __name__ == '__main__': main()
