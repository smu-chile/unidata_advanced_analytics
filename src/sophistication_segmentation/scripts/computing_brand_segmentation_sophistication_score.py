from __future__ import annotations

import os

# Default
import logging
import argparse
from logging import config

import numpy as np
import pandas as pd

# pip
import pendulum
from sklearn.cluster import KMeans
from google.cloud.bigquery import Client, TimePartitioning  # noqa: F401

# Own
from common.constants import LOGGING_CONFIG
from common.databases.queries import QueryDict
from common.gcp_extended.bigquery import (
    uploadFrame,
    readBigQuery,
    deleteFromTable,
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
    choices=['Unimarc', 'Alvi', 'Super 10'],
    help='SMU subsidiary for which the allocation will be made'
)


# -------------------------------------------------------------------------
#  SQL Queries
# -------------------------------------------------------------------------
SQL_QUERIES = QueryDict({
    'brand_sophistication_score':
    """
    SELECT *
    FROM `${gcp_project}.ML_LAB.BRAND_SOPHISTICATION_SCORE`
    WHERE date = '${execution_date}'
    AND store_banner = '${store_banner}'
    """,

    'mpes':
    """
    WITH TEMP AS (
    SELECT
        B.NM,
        ltrim(B.SKU_PRODUCT, '0') AS MATERIAL,
        B.EAN,
        B.GRUPO_DSC,
        B.BRAND_DESC
    FROM `cl-cda-prod.DS_CDA_VW_SMU.DW_VW_DIM_PRODUCT_HIERARCHY` B

    LEFT JOIN `cl-cda-prod.DS_CDA_VW_SMU.DW_VW_DIM_SKU_ATTR` SKU
    on B.SKU_KEY = SKU.SKU_KEY

    WHERE SKU.TIPO_MARCA IN('1','3')
    ORDER BY B.GRUPO_ID,MATERIAL
    )

    SELECT DISTINCT BRAND_DESC
    FROM TEMP
    """
})


# -------------------------------------------------------------------------
# Functions and Classes
# -------------------------------------------------------------------------
def _hay_outliers(serie: pd.Series, factor: float = 1.5) -> bool:
    """True si hay valores fuera de la regla IQR (en escala log)."""
    lx = np.log(serie[serie > 0])
    if len(lx) < 4:
        return False
    q1, q3 = lx.quantile([0.25, 0.75])
    iqr = q3 - q1
    return bool(((lx < q1 - factor * iqr) | (lx > q3 + factor * iqr)).any())


def _clasificar_serie(serie: pd.Series, n_clusters: int = 3,
                      min_sep: float = 0.04,
                      q: tuple = (0.05, 0.95)):
    """Devuelve grupos 1..3, o None si no hay grupos realmente distintos.

    - Si la categoría tiene outliers, K-means se ajusta sin los extremos
      (percentiles q). Si no, se ajusta con todas las marcas.
    - Todas las marcas se asignan al cluster más cercano.
    - Se exige una separación relativa mínima `min_sep` entre las
      medianas de clusters vecinos; si no se cumple, se baja k.
    """
    if _hay_outliers(serie):
        lo, hi = serie.quantile(q)
        ajuste = serie[(serie > lo) & (serie < hi)]
    else:
        ajuste = serie

    for k in range(n_clusters, 1, -1):
        if ajuste.nunique() < k:
            continue

        km = KMeans(n_clusters=k, random_state=42, n_init=10)
        km.fit(ajuste.to_frame())

        # Separación entre clusters, medida sobre los datos de ajuste
        et_ajuste = pd.Series(km.labels_, index=ajuste.index)
        medianas = ajuste.groupby(et_ajuste).median().sort_values()
        if not (medianas.pct_change().dropna() >= min_sep).all():
            continue

        # Asignar TODAS las marcas (incluidos los extremos)
        orden = {old: i for i, old in enumerate(medianas.index)}
        rango = pd.Series(km.predict(serie.to_frame()),
                          index=serie.index).map(orden)

        if k == n_clusters:
            return rango + 1

        # k=2: el cluster más grande es el "típico" (MAINSTREAM)
        grande = et_ajuste.value_counts().idxmax()
        if medianas.index[0] == grande:    # el grande es el más barato
            return rango.map({0: 2, 1: 3})  # MAINSTREAM y PREMIUM
        return rango.map({0: 1, 1: 2})      # ECONOMY y MAINSTREAM

    return None


def clasificar_categorias_kmeans(df: pd.DataFrame,
                                 n_clusters: int = 3,
                                 min_marcas: int = 3,
                                 min_sep: float = 0.04) -> pd.DataFrame:
    df_resultado = df.copy()
    df_resultado['grupo_kmeans'] = np.nan

    for _categoria, df_cat in df_resultado.groupby('CATEGORY_DESCRIPTION'):
        valores = df_cat['HM_PU_PPUM'].dropna()

        if len(valores) >= min_marcas:
            grupos = _clasificar_serie(valores, n_clusters, min_sep)
            if grupos is not None:
                df_resultado.loc[grupos.index, 'grupo_kmeans'] = grupos

    mapeo_segmentos = {1: 'ECONOMY', 2: 'MAINSTREAM', 3: 'PREMIUM'}
    df_resultado['clasificacion_marca'] = (
        df_resultado['grupo_kmeans'].map(mapeo_segmentos)
        .fillna('SIN CLASIFICACION'))

    return df_resultado

# -------------------------------------------------------------------------
#                        Main Function
# -------------------------------------------------------------------------
def main():
    # ----------
    # Parameters
    # ----------
    args = vars(parser.parse_args())
    # Environment
    usuario = 'brand_segmentation_score'

    gcp_project: str = args['gcp_project']
    execution_date: pendulum.Date = pendulum.date(
        *list(map(int, args['execution_date'].split('-')))
    ).set(
        day=1 # Allways ensures first day of the month
    )
    store_banner: str = args['store_banner']

    # Hardcoded
    gbq_client = Client()
    table_ref=f'{gcp_project}.ML_LAB.BRAND_SEGMENTATION_SOPHISTICATION'

    logging.info(f'gcp_project = {gcp_project}')
    logging.info(f'execution_date = {execution_date}')
    logging.info(f'store_banner = {store_banner}')

    logging.info('Ejecucion query brand_sophistication_score')
    brand_score = readBigQuery(SQL_QUERIES['brand_sophistication_score'].substitute(
        gcp_project = gcp_project,
        execution_date = execution_date,
        store_banner = store_banner
        ),
    user = usuario,
    gbq_client = gbq_client
    )

    logging.info('Ejecucion query mpes')
    mpes = readBigQuery(SQL_QUERIES['mpes'].substitute(
        ),
    user = usuario,
    gbq_client = gbq_client
    )

    logging.info('Ejecucion Kmeans')

    df_clasificacion_marcas = clasificar_categorias_kmeans(brand_score)

    mpes = mpes['BRAND_DESC'].str.strip().str.upper().unique()

    mask = df_clasificacion_marcas['BRAND'].str.strip().str.upper().isin(mpes)
    df_clasificacion_marcas.loc[mask, 'clasificacion_marca'] = 'MPES'

    # Remove past run if needed
    logging.info(f'Removing past run from {table_ref}')
    deleteFromTable(
        table_ref = table_ref,
        where_clause=f"""
            DATE = '{execution_date}'
            AND STORE_BANNER = '{store_banner}'
        """,
        gbq_client=gbq_client,
    )

    # Create the table
    logging.info('Creating new partition')
    uploadFrame(
        df_clasificacion_marcas,
        table_ddl_json_path = os.path.join('gbq_objects', 'ingest_brand_segmentation_sophistication.json'),  # noqa: E501
        project = gcp_project,
        gbq_client = gbq_client,
        if_exists = 'append'
    )

    logging.info('Done!')


if __name__ == '__main__':
    main()
