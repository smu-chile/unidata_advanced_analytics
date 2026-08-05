# Default
from __future__ import annotations

import os
import logging
import argparse
from logging import config

# Pip
import numpy as np
import pandas as pd
import statsmodels.api as sm
from sklearn.cluster import KMeans
from google.cloud.bigquery import Client, DatasetReference

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
config.dictConfig(LOGGING_CONFIG)

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
    '--store_banner', type=str,
    help='Store banner'
)

# -------------------------------------------------------------------------
#  Parametros generales
# -------------------------------------------------------------------------
# Intensidades promocionales (dummies del modelo, 'Regular' es la
# categoria de referencia -- no lleva dummy propia).
ORDEN_INTENSIDAD = [
    '0_10', '10_15', '15_20', '20_25',
    '25_30', '30_40', '40_mas',
]

# Filtro de variacion minima de precio (misma causa raiz ya resuelta en
# el baseline: con poco movimiento real de precio, el coeficiente queda
# mal identificado).
MIN_RANGO_PRECIO_PCT_PARA_ESTIMAR = 5.0
MIN_DIAS_PARA_INTENTAR_ELASTICIDAD_PROPIA = 60

MAX_VIF_ELASTICIDAD = 5.0

# Cascada de contagio de 3 niveles + shrinkage continuo
K_SHRINKAGE_ELASTICIDAD = 150
MIN_SKU_PRIOR_SUBCATEGORIA = 5
MIN_DIAS_SUSTITUTO_CONFIABLE = 60

MIN_DIAS_CONFIANZA_MEDIA = 90
MIN_DIAS_CONFIANZA_ALTA = 200

CANDIDATAS_MECANICA = ['progreso_promocion', 'frecuencia_promocional_90d']

RANGO_ELASTICIDAD_MIN, RANGO_ELASTICIDAD_MAX = -5.0, 0.0
MIN_SKU_PRIOR_SUBCATEGORIA_ACOTADO = 5
ETIQUETA_ACOTADO = 'Sin evidencia - Acotado al rango'

# Ecommerce tiene su PROPIA tabla productiva de regresion.
MAPA_BANNER_REGRESSION_ECOMMERCE = {
    'Ecommerce Unimarc': 'Unimarc',
    'Ecommerce Alvi': 'Alvi',
}


# -------------------------------------------------------------------------
#  SQL Queries
# -------------------------------------------------------------------------
SQL_QUERIES = QueryDict({  # Region: Explicacion de query

    # indice_venta ya viene neto de calendario/sustitutos -- es la
    # variable dependiente del modelo. Solo se traen los SKU con
    # nivel_baseline='blend' (los unicos con indice calculable).
    'query_baseline_panel':
    """
    SELECT
        material,
        ean,
        product_description,
        category_description,
        sub_category_description,
        sales_uom,
        p_date,
        estado,
        precio_promedio,
        indice_venta,
        nivel_baseline
    FROM `${table}`
    WHERE store_banner = '${store_banner}' AND nivel_baseline = 'blend'
    ORDER BY material, p_date
    """,

    # Mecanica promocional (progreso, frecuencia reciente) -- el
    # baseline no sabe nada de esto, asi que no esta duplicado.
    'query_promo_mecanica':
    """
    SELECT
        material,
        p_date,
        atributo_promocion,
        progreso_promocion,
        frecuencia_promocional_90d
    FROM `${table}`
    WHERE store_banner = '${store_banner}'
    ORDER BY material, p_date
    """,

    # Identidad del sustituto mas cercano (para la cascada de 3 niveles)
    'query_sustituto_identidad':
    """
    SELECT
        material,
        p_date,
        ean_sustituto_1
    FROM `${table}`
    WHERE store_banner = '${store_banner}'
    ORDER BY material, p_date
    """,

    # Elasticidades de transicion ya calculadas (para el parche de casos
    # fuera del rango [-5,0] -- regla 1: mediana de las transiciones
    # disponibles para ese material).
    'query_transiciones':
    """
    SELECT
        material,
        tipo_transicion,
        elasticidad_arco_final,
        n_eventos
    FROM `${table}`
    WHERE store_banner = '${store_banner}'
    """,

    # Materiales SIN baseline confiable -- nunca entran a la regresion
    # (no tienen indice_venta usable), pero igual deben recibir
    # elasticidad heredada (subcategoria/categoria) para no quedar sin
    # reportar. Se distingue el motivo: nunca tuvieron un dia Regular,
    # vs. tuvieron pero el baseline no paso la validacion de honestidad.
    'query_materiales_sin_baseline':
    """
    SELECT
        material,
        ean,
        product_description,
        category_description,
        sub_category_description,
        sales_uom,
        MAX(CASE WHEN estado = 'Regular' THEN 1 ELSE 0 END) AS tuvo_dia_regular
    FROM `${table}`
    WHERE store_banner = '${store_banner}' AND nivel_baseline = 'sin_baseline'
    GROUP BY material, ean, product_description, category_description,
            sub_category_description, sales_uom
    """,
})


# -------------------------------------------------------------------------
#  Filtro de multicolinealidad (VIF)
# -------------------------------------------------------------------------
def _filtrar_por_vif(df_material: pd.DataFrame, variables: list[str]) -> list[str]:
    columnas = list(variables)
    if len(columnas) <= 1:
        return columnas

    columnas = [c for c in columnas if df_material[c].nunique() > 1]  # noqa: PD101
    if len(columnas) <= 1:
        return columnas

    while len(columnas) > 1:
        try:
            corr = df_material[columnas].corr().to_numpy()
            vifs = pd.Series(np.diag(np.linalg.inv(corr)), index=columnas)
        except np.linalg.LinAlgError:
            break

        if vifs.isna().any() or np.isinf(vifs).any():
            break
        if vifs.empty or vifs.max() <= MAX_VIF_ELASTICIDAD:
            break

        columnas.remove(vifs.idxmax())

    return columnas


# -------------------------------------------------------------------------
#  Entrenamiento de la elasticidad propia (por material)
# -------------------------------------------------------------------------
def _entrenar_elasticidad(df_material: pd.DataFrame) -> dict:
    """Regresion log(indice_venta) ~ log_precio + dummies de promo +
    mecanica promocional. Retorna todos los coeficientes crudos, mas
    metadatos para la cascada.
    """
    vacio = {'modelo': None, 'variables': [], 'r2_ajustado': np.nan}

    variables_candidatas = ['log_precio', *ORDEN_INTENSIDAD, *CANDIDATAS_MECANICA]
    variables_candidatas = [
        v for v in variables_candidatas
        if v in df_material.columns and df_material[v].notna().any()
        and df_material[v].nunique() > 1  # noqa: PD101
    ]

    df_local = df_material.copy()
    for col in CANDIDATAS_MECANICA:
        if col in df_local.columns:
            df_local[col] = df_local[col].fillna(0.0)

    variables_finales = _filtrar_por_vif(df_local, variables_candidatas)
    if not variables_finales:
        return vacio

    x = sm.add_constant(df_local[variables_finales], has_constant='add').astype(float)
    y = df_local['log_indice']

    try:
        modelo = sm.OLS(y, x).fit()
    except Exception:  # noqa: BLE001
        return vacio

    return {
        'modelo': modelo, 'variables': variables_finales, 'r2_ajustado': modelo.rsquared_adj,
    }


# -------------------------------------------------------------------------
#  Segmento high/low
# -------------------------------------------------------------------------
def asignar_segmento_elasticidad(elasticidades: pd.Series) -> pd.Series:
    """Percentil 1/99 para outliers, KMeans k=2 sobre el interior,
    'high' = el cluster cuyo centroide es MAS NEGATIVO.
    """
    elasticidades_num = pd.to_numeric(elasticidades, errors='coerce')
    mascara_valida = elasticidades_num.notna()

    percentil_bajo = np.percentile(elasticidades_num[mascara_valida], 1)
    percentil_alto = np.percentile(elasticidades_num[mascara_valida], 99)

    mascara_interior = (
        (elasticidades_num >= percentil_bajo) & (elasticidades_num <= percentil_alto)
    )
    mascara_outlier_bajo = elasticidades_num < percentil_bajo
    mascara_outlier_alto = elasticidades_num > percentil_alto

    valores_interiores = elasticidades_num[mascara_interior].to_numpy().reshape(-1, 1)
    kmeans = KMeans(n_clusters=2, n_init=10, random_state=42)
    labels_kmeans = kmeans.fit_predict(valores_interiores)

    centroides = kmeans.cluster_centers_.flatten()
    label_high = np.argmin(centroides)
    mapeo = {label_high: 'high', 1 - label_high: 'low'}
    labels_asignados = [mapeo[label] for label in labels_kmeans]

    segmento = pd.Series(index=elasticidades.index, dtype='object')
    segmento[mascara_interior] = labels_asignados
    segmento[mascara_outlier_bajo] = 'high'
    segmento[mascara_outlier_alto] = 'low'

    return segmento


def _calcular_confianza(n_dias: int) -> str:
    if n_dias >= MIN_DIAS_CONFIANZA_ALTA:
        return 'Alta'
    if n_dias >= MIN_DIAS_CONFIANZA_MEDIA:
        return 'Media'
    return 'Baja'


def main() -> None:  # noqa: D103

    # Parse input variables
    args = vars(parser.parse_args())
    execution_date: str = args['execution_date']
    proyecto: str = args['project_id']
    store_banner: str = args['store_banner']
    logging.info(f'execution_date: {execution_date}')
    logging.info(f'proyecto: {proyecto}')
    logging.info(f'store_banner: {store_banner}')

    # Set gbq client for all subsequent queries
    gbq_client = Client()

    # REGION: Inputs del proceso
    # -------------------------------------------------------------------
    usuario = 'elasticidad_general'
    esquema = 'PRECIO_PROMOCIONES'
    tabla = 'ELASTICITY'
    dataset_regression = 'TMP'

    if store_banner in MAPA_BANNER_REGRESSION_ECOMMERCE:
        table_regression = f'{proyecto}.{dataset_regression}.TMP_ECOMMERCE_REGRESSION_PROCESSED_DATA_ELASTICITY'  # noqa: E501
        store_banner_regression = MAPA_BANNER_REGRESSION_ECOMMERCE[store_banner]
    else:
        table_regression = f'{proyecto}.{dataset_regression}.TMP_REGRESSION_PROCESSED_DATA_ELASTICITY'  # noqa: E501
        store_banner_regression = store_banner

    table_promotion_daily = f'{proyecto}.{esquema}.TMP_PROMOTION_DAILY'
    table_baseline_panel = f'{proyecto}.{esquema}.BASELINE_PANEL'
    table_elasticidad_transiciones = f'{proyecto}.{esquema}.ELASTICITY_TRANSITIONS'
    # ENDREGION

    # REGION: Asegurar que el dataset de destino exista
    # -------------------------------------------------------------------
    dataset_ref = DatasetReference(proyecto, esquema)
    gbq_client.create_dataset(dataset_ref, exists_ok=True)
    # ENDREGION

    # REGION: Carga de datos
    # -------------------------------------------------------------------
    query_baseline_panel = SQL_QUERIES['query_baseline_panel'].substitute(
        table=table_baseline_panel, store_banner=store_banner)
    df_panel = readBigQuery(
        query=query_baseline_panel, user=usuario, gbq_client=gbq_client)

    query_promo_mecanica = SQL_QUERIES['query_promo_mecanica'].substitute(
        table=table_promotion_daily, store_banner=store_banner)
    df_promo_mecanica = readBigQuery(
        query=query_promo_mecanica, user=usuario, gbq_client=gbq_client)

    query_sustituto_identidad = SQL_QUERIES['query_sustituto_identidad'].substitute(
        table=table_regression, store_banner=store_banner_regression)
    df_sustituto_identidad = readBigQuery(
        query=query_sustituto_identidad, user=usuario, gbq_client=gbq_client)

    query_transiciones = SQL_QUERIES['query_transiciones'].substitute(
        table=table_elasticidad_transiciones, store_banner=store_banner)
    df_transiciones_para_parche = readBigQuery(
        query=query_transiciones, user=usuario, gbq_client=gbq_client)

    query_materiales_sin_baseline = SQL_QUERIES['query_materiales_sin_baseline'].substitute(
        table=table_baseline_panel, store_banner=store_banner)
    df_materiales_sin_baseline = readBigQuery(
        query=query_materiales_sin_baseline, user=usuario, gbq_client=gbq_client)
    # ENDREGION

    # REGION: Preparacion del panel: tipos, merge, deduplicacion,
    # dummies de promo
    # -------------------------------------------------------------------
    df_panel['material'] = df_panel['material'].astype(str)
    df_panel['p_date'] = pd.to_datetime(df_panel['p_date'])
    df_panel['precio_promedio'] = df_panel['precio_promedio'].astype(float)
    df_panel['indice_venta'] = df_panel['indice_venta'].astype(float)

    df_panel = df_panel.drop_duplicates(subset=['material', 'p_date'], keep='last')

    df_promo_mecanica['material'] = df_promo_mecanica['material'].astype(str)
    df_promo_mecanica['p_date'] = pd.to_datetime(df_promo_mecanica['p_date'])
    df_promo_mecanica = df_promo_mecanica.drop_duplicates(subset=['material', 'p_date'])

    df_sustituto_identidad['material'] = df_sustituto_identidad['material'].astype(str)
    df_sustituto_identidad['p_date'] = pd.to_datetime(df_sustituto_identidad['p_date'])
    df_sustituto_identidad = df_sustituto_identidad.drop_duplicates(subset=['material', 'p_date'])

    df_panel = df_panel.merge(
        df_promo_mecanica[
            ['material', 'p_date', 'progreso_promocion', 'frecuencia_promocional_90d']],
        on=['material', 'p_date'], how='left',
    )
    df_panel = df_panel.merge(
        df_sustituto_identidad[['material', 'p_date', 'ean_sustituto_1']],
        on=['material', 'p_date'], how='left',
    )

    df_panel['log_precio'] = np.log(df_panel['precio_promedio'].clip(lower=1))
    df_panel['log_indice'] = np.log(df_panel['indice_venta'].clip(lower=0.001))

    # Dummies de intensidad promocional (Regular = referencia,
    #  sin dummy propia)
    for intensidad in ORDEN_INTENSIDAD:
        df_panel[intensidad] = (df_panel['estado'] == intensidad).astype(int)

    df_panel = df_panel.sort_values(['material', 'p_date']).reset_index(drop=True)

    logging.info(f'Panel construido: {df_panel.shape}')
    # ENDREGION

    # REGION: Mapeo material_sustituto_1
    # (identidad, para la cascada de 3 niveles)
    # -------------------------------------------------------------------
    lookup_ean_material = (
        df_panel[['ean', 'material']].dropna().drop_duplicates(subset=['ean'])
        .set_index('ean')['material']
    )

    ean_sustituto_por_material = (
        df_panel.dropna(subset=['ean_sustituto_1'])
        .groupby('material')['ean_sustituto_1']
        .agg(lambda s: s.mode().iloc[0] if not s.mode().empty else np.nan)
    )

    material_sustituto_1 = ean_sustituto_por_material.map(lookup_ean_material)
    logging.info(f'Materiales con sustituto identificado y mapeado: '
                f'{material_sustituto_1.notna().sum():,} de {len(material_sustituto_1):,}')
    # ENDREGION

    # REGION: Loop principal -- elasticidad propia por material
    # -------------------------------------------------------------------
    resultados = []

    for material, df_material in df_panel.groupby('material'):
        n_dias = len(df_material)
        subcat = df_material['sub_category_description'].iloc[0]
        categoria = df_material['category_description'].iloc[0]
        ean = df_material['ean'].iloc[0]
        product_description = df_material['product_description'].iloc[0]
        sales_uom = df_material['sales_uom'].iloc[0]

        precio = df_material['precio_promedio']
        rango_pct = (precio.max() - precio.min()) / precio.mean() * 100 if precio.mean() > 0 else 0

        info_modelo = {'modelo': None, 'variables': [], 'r2_ajustado': np.nan}
        if (n_dias >= MIN_DIAS_PARA_INTENTAR_ELASTICIDAD_PROPIA
                and rango_pct >= MIN_RANGO_PRECIO_PCT_PARA_ESTIMAR):
            info_modelo = _entrenar_elasticidad(df_material)

        fila = {
            'material': material, 'ean': ean, 'product_description': product_description,
            'category_description': categoria, 'sub_category_description': subcat,
            'sales_uom': sales_uom,
            'n_dias': n_dias, 'rango_precio_pct': round(rango_pct, 2),
            'r2_ajustado': info_modelo['r2_ajustado'],
        }

        if info_modelo['modelo'] is not None:
            params = info_modelo['modelo'].params
            fila['coef_intercepto'] = params.get('const', np.nan)
            fila['coef_log_precio'] = params.get('log_precio', np.nan)
            for intensidad in ORDEN_INTENSIDAD:
                fila[f'coef_promo_{intensidad}'] = params.get(intensidad, np.nan)
            for mecanica in CANDIDATAS_MECANICA:
                fila[f'coef_{mecanica}'] = params.get(mecanica, np.nan)
        else:
            fila['coef_intercepto'] = np.nan
            fila['coef_log_precio'] = np.nan
            for intensidad in ORDEN_INTENSIDAD:
                fila[f'coef_promo_{intensidad}'] = np.nan
            for mecanica in CANDIDATAS_MECANICA:
                fila[f'coef_{mecanica}'] = np.nan

        resultados.append(fila)

    df_coeficientes = pd.DataFrame(resultados)

    n_con_propia = df_coeficientes['coef_log_precio'].notna().sum()
    logging.info(f'Materiales con elasticidad propia estimada: '
                f'{n_con_propia:,} de {len(df_coeficientes):,}')
    # ENDREGION

    # REGION: Materiales SIN baseline confiable -- se agregan para heredar
    # elasticidad (nunca pueden tener "propia", pero no deben quedar sin
    # reportar). Se distingue el motivo en una columna aparte.
    # -------------------------------------------------------------------
    filas_sin_baseline = []
    columnas_coef_vacias = (
        ['coef_intercepto', 'coef_log_precio']
        + [f'coef_promo_{i}' for i in ORDEN_INTENSIDAD]
        + [f'coef_{m}' for m in CANDIDATAS_MECANICA]
    )
    for _, fila_material in df_materiales_sin_baseline.iterrows():
        motivo = (
            'nunca_regular' if fila_material['tuvo_dia_regular'] == 0
            else 'honestidad_fallida'
        )
        fila_vacia = {
            'material': fila_material['material'], 'ean': fila_material['ean'],
            'product_description': fila_material['product_description'],
            'category_description': fila_material['category_description'],
            'sub_category_description': fila_material['sub_category_description'],
            'sales_uom': fila_material['sales_uom'],
            'n_dias': 0, 'rango_precio_pct': np.nan, 'r2_ajustado': np.nan,
            'motivo_sin_baseline': motivo,
        }
        for col in columnas_coef_vacias:
            fila_vacia[col] = np.nan
        filas_sin_baseline.append(fila_vacia)

    df_sin_baseline = pd.DataFrame(filas_sin_baseline)
    df_coeficientes = pd.concat([df_coeficientes, df_sin_baseline], ignore_index=True)

    n_nunca_regular = (df_coeficientes['motivo_sin_baseline'] == 'nunca_regular').sum()
    n_honestidad = (df_coeficientes['motivo_sin_baseline'] == 'honestidad_fallida').sum()
    logging.info(f'Materiales sin baseline agregados para heredar elasticidad: '
                f'{len(df_sin_baseline):,} '
                f'({n_nunca_regular:,} nunca tuvieron dia Regular, '
                f'{n_honestidad:,} fallaron honestidad del baseline)')
    # ENDREGION

    # REGION: Cascada de 3 niveles + shrinkage continuo
    # -------------------------------------------------------------------
    df_coeficientes['n_dias_evidencia'] = np.where(
        df_coeficientes['coef_log_precio'].notna(), df_coeficientes['n_dias'], 0
    )

    con_propia = df_coeficientes[df_coeficientes['coef_log_precio'].notna()]

    prior_subcat = (
        con_propia.groupby('sub_category_description')
        .agg(elasticidad_subcat_prior=('coef_log_precio', 'median'),
            n_sku_subcat_prior=('material', 'nunique'))
        .reset_index()
    )
    prior_categoria = (
        con_propia.groupby('category_description')['coef_log_precio']
        .median().rename('elasticidad_categoria_prior').reset_index()
    )

    df_coeficientes = df_coeficientes.merge(prior_subcat, on='sub_category_description', how='left')  # noqa: E501
    df_coeficientes = df_coeficientes.merge(prior_categoria, on='category_description', how='left')  # noqa: E501

    df_coeficientes['material_sustituto_1'] = df_coeficientes['material'].map(material_sustituto_1)

    lookup_propio = (
        df_coeficientes.set_index('material')[['coef_log_precio', 'n_dias_evidencia']]
        .rename(columns={'coef_log_precio': 'elasticidad_sustituto_prior',
                        'n_dias_evidencia': 'n_dias_sustituto'})
    )

    df_coeficientes = df_coeficientes.merge(
        lookup_propio, left_on='material_sustituto_1', right_index=True, how='left'
    )

    sustituto_calificado = (
        df_coeficientes['material_sustituto_1'].notna()
        & (df_coeficientes['n_dias_sustituto'] >= MIN_DIAS_SUSTITUTO_CONFIABLE)
    )
    subcat_calificada = df_coeficientes['n_sku_subcat_prior'] >= MIN_SKU_PRIOR_SUBCATEGORIA

    df_coeficientes['elasticidad_prior_final'] = np.select(
        [sustituto_calificado, subcat_calificada],
        [df_coeficientes['elasticidad_sustituto_prior'], df_coeficientes['elasticidad_subcat_prior']],  # noqa: E501
        default=df_coeficientes['elasticidad_categoria_prior'],
    )

    df_coeficientes['origen_elasticidad'] = np.select(
        [df_coeficientes['n_dias_evidencia'] > 0, sustituto_calificado, subcat_calificada,
        df_coeficientes['elasticidad_categoria_prior'].notna()],
        ['propia', 'heredada_sustituto', 'heredada_subcategoria', 'heredada_categoria'],
        default='sin_evidencia',
    )

    # Los materiales que nunca tuvieron baseline confiable (motivo_sin_
    # baseline no-nulo) jamas pueden tener origen 'propia' -- pero para
    # que no se confundan con los "sin_evidencia" que si tuvieron
    # baseline y la cascada tampoco encontro nada, se les marca el
    # origen con el prefijo 'sin_baseline_'. El motivo especifico
    # (nunca_regular vs. honestidad_fallida) queda disponible en la
    # columna 'motivo_sin_baseline' para diagnostico, sin saturar el
    # string de origen reportado.
    mascara_sin_baseline = df_coeficientes['motivo_sin_baseline'].notna()
    df_coeficientes.loc[mascara_sin_baseline, 'origen_elasticidad'] = (
        'sin_baseline_' + df_coeficientes.loc[mascara_sin_baseline, 'origen_elasticidad']
    )

    df_coeficientes['peso_propio'] = (
        df_coeficientes['n_dias_evidencia']
        / (df_coeficientes['n_dias_evidencia'] + K_SHRINKAGE_ELASTICIDAD)
    )

    coef_log_precio_seguro = df_coeficientes['coef_log_precio'].fillna(0)

    df_coeficientes['elasticidad_final'] = np.where(
        df_coeficientes['elasticidad_prior_final'].notna(),
        (df_coeficientes['peso_propio'] * coef_log_precio_seguro
        + (1 - df_coeficientes['peso_propio']) * df_coeficientes['elasticidad_prior_final']),
        np.where(df_coeficientes['n_dias_evidencia'] > 0, df_coeficientes['coef_log_precio'], np.nan),  # noqa: E501
    )

    df_coeficientes['confianza'] = df_coeficientes['n_dias_evidencia'].apply(_calcular_confianza)
    df_coeficientes['store_banner'] = store_banner

    logging.info('Distribucion de origen de la elasticidad:')
    logging.info(df_coeficientes['origen_elasticidad'].value_counts().to_dict())
    # ENDREGION

    # REGION: Parche -- casos de elasticidad_final fuera del rango [-5, 0]
    # -------------------------------------------------------------------
    df_transiciones_para_parche['material'] = df_transiciones_para_parche['material'].astype(str)

    evidencia_por_material = df_transiciones_para_parche.groupby('material')['n_eventos'].sum()
    mediana_por_material = df_transiciones_para_parche.groupby('material')['elasticidad_arco_final'].median()  # noqa: E501

    df_coeficientes['mediana_transiciones'] = df_coeficientes['material'].map(mediana_por_material)
    df_coeficientes['evidencia_transiciones_total'] = (
        df_coeficientes['material'].map(evidencia_por_material).fillna(0)
    )

    df_coeficientes['elasticidad_final_parcheada'] = df_coeficientes['elasticidad_final']

    es_positiva = df_coeficientes['elasticidad_final'] > 0
    tiene_evidencia_transiciones = (
        (df_coeficientes['evidencia_transiciones_total'] > 0) & df_coeficientes['mediana_transiciones'].notna()  # noqa: E501
    )

    regla_1 = es_positiva & tiene_evidencia_transiciones
    df_coeficientes.loc[regla_1, 'elasticidad_final_parcheada'] = df_coeficientes.loc[regla_1, 'mediana_transiciones']  # noqa: E501

    regla_2 = es_positiva & (~tiene_evidencia_transiciones)
    df_coeficientes.loc[regla_2, 'elasticidad_final_parcheada'] = -1.0

    regla_3 = df_coeficientes['elasticidad_final_parcheada'] < RANGO_ELASTICIDAD_MIN
    df_coeficientes.loc[regla_3, 'elasticidad_final_parcheada'] = RANGO_ELASTICIDAD_MIN

    parcheado = regla_1 | regla_2 | regla_3
    df_coeficientes.loc[parcheado & mascara_sin_baseline, 'origen_elasticidad'] = (
        'sin_baseline_Sin evidencia - Acotado al rango'
    )
    df_coeficientes.loc[parcheado & ~mascara_sin_baseline, 'origen_elasticidad'] = (
        'Sin evidencia - Acotado al rango'
    )
    df_coeficientes['elasticidad_final'] = df_coeficientes['elasticidad_final_parcheada']

    logging.info(f'Regla 1 (positiva + evidencia en transiciones -> mediana): {regla_1.sum():,} materiales')  # noqa: E501
    logging.info(f'Regla 2 (positiva + sin evidencia -> -1.0): {regla_2.sum():,} materiales')
    logging.info(f'Regla 3 (mas negativa que {RANGO_ELASTICIDAD_MIN} -> {RANGO_ELASTICIDAD_MIN}): {regla_3.sum():,} materiales')  # noqa: E501
    logging.info(f'Total parcheados: {parcheado.sum():,} de {len(df_coeficientes):,}')
    # ENDREGION

    # REGION: Re-parche -- heredar de subcategoria/categoria antes del
    # -------------------------------------------------------------------
    con_propia_acotado = df_coeficientes[df_coeficientes['origen_elasticidad'] == 'propia']

    prior_subcat_acotado = (
        con_propia_acotado.groupby('sub_category_description')
        .agg(elasticidad_prior_subcat_acotado=('elasticidad_final', 'median'),
            n_sku_subcat_prior_acotado=('material', 'nunique'))
        .reset_index()
    )
    prior_cat_acotado = (
        con_propia_acotado.groupby('category_description')['elasticidad_final']
        .median().rename('elasticidad_prior_cat_acotado').reset_index()
    )

    df_coeficientes = df_coeficientes.merge(prior_subcat_acotado, on='sub_category_description', how='left')  # noqa: E501
    df_coeficientes = df_coeficientes.merge(prior_cat_acotado, on='category_description', how='left')  # noqa: E501

    ETIQUETA_ACOTADO_SIN_BASELINE = f'sin_baseline_{ETIQUETA_ACOTADO}'  # noqa: N806
    necesita_reparche = df_coeficientes['origen_elasticidad'].isin(
        [ETIQUETA_ACOTADO, ETIQUETA_ACOTADO_SIN_BASELINE])
    era_sin_baseline = df_coeficientes['origen_elasticidad'] == ETIQUETA_ACOTADO_SIN_BASELINE
    subcat_califica_acotado = df_coeficientes['n_sku_subcat_prior_acotado'].fillna(0) >= MIN_SKU_PRIOR_SUBCATEGORIA_ACOTADO  # noqa: E501
    subcat_disponible_acotado = necesita_reparche & subcat_califica_acotado
    categoria_disponible_acotado = (
        necesita_reparche & (~subcat_califica_acotado) & df_coeficientes['elasticidad_prior_cat_acotado'].notna()  # noqa: E501
    )

    df_coeficientes.loc[subcat_disponible_acotado, 'elasticidad_final'] = (
        df_coeficientes.loc[subcat_disponible_acotado, 'elasticidad_prior_subcat_acotado']
    )
    df_coeficientes.loc[subcat_disponible_acotado & era_sin_baseline, 'origen_elasticidad'] = (
        'sin_baseline_heredada_subcategoria_tras_acotado')
    df_coeficientes.loc[subcat_disponible_acotado & ~era_sin_baseline, 'origen_elasticidad'] = (
        'heredada_subcategoria_tras_acotado')

    df_coeficientes.loc[categoria_disponible_acotado, 'elasticidad_final'] = (
        df_coeficientes.loc[categoria_disponible_acotado, 'elasticidad_prior_cat_acotado']
    )
    df_coeficientes.loc[categoria_disponible_acotado & era_sin_baseline, 'origen_elasticidad'] = (
        'sin_baseline_heredada_categoria_tras_acotado')
    df_coeficientes.loc[categoria_disponible_acotado & ~era_sin_baseline, 'origen_elasticidad'] = (
        'heredada_categoria_tras_acotado')

    logging.info(f'De los {necesita_reparche.sum():,} materiales "acotados al rango" (con o sin '
                f'baseline): {subcat_disponible_acotado.sum():,} resueltos por subcategoria, '
                f'{categoria_disponible_acotado.sum():,} por categoria')
    # ENDREGION

    # REGION: Segmento high/low
    # -------------------------------------------------------------------
    df_coeficientes['segmento_elasticidad'] = asignar_segmento_elasticidad(df_coeficientes['elasticidad_final'])  # noqa: E501

    logging.info('Distribucion del segmento (high/low):')
    logging.info(df_coeficientes['segmento_elasticidad'].value_counts().to_dict())
    # ENDREGION

    # REGION: Tabla final -- esquema solicitado para consumo del negocio
    # -------------------------------------------------------------------
    df_elasticidad_final = df_coeficientes.rename(columns={
        'store_banner': 'STORE_BANNER',
        'category_description': 'CATEGORIA',
        'material': 'MATERIAL',
        'product_description': 'DESCRIPCION_MATERIAL',
        'ean': 'EAN',
        'sales_uom': 'UMV',
        'origen_elasticidad': 'ORIGEN',
        'elasticidad_final': 'ELASTICIDAD',
        'segmento_elasticidad': 'SEGMENTO_ELASTICIDAD',
    })[[
        'STORE_BANNER', 'CATEGORIA', 'MATERIAL', 'DESCRIPCION_MATERIAL',
        'EAN', 'UMV', 'ORIGEN', 'ELASTICIDAD', 'SEGMENTO_ELASTICIDAD',
    ]]

    logging.info(f'Tabla final: {df_elasticidad_final.shape}')
    # ENDREGION

    # REGION: Carga a BigQuery
    # -------------------------------------------------------------------
    where_clause = f"STORE_BANNER = '{store_banner}'"

    deleteFromTable(table_ref=f'{proyecto}.{esquema}.{tabla}',
                    where_clause=where_clause,
                    gbq_client=gbq_client)

    uploadFrame(
        df_elasticidad_final,
        table_ddl_json_path=os.path.join('gbq_objects', 'ingest_elasticity.json'),
        project=proyecto,
        gbq_client=gbq_client,
        if_exists='append'
    )

    logging.info(f'Se sube la tabla a GCP: {proyecto}.{esquema}.{tabla} ({len(df_elasticidad_final):,} filas)')  # noqa: E501
    # ENDREGION


if __name__ == '__main__':
    main()
