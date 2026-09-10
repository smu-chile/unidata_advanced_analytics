# Default
"""Elasticidad de precios V7.x -- metodo por zona comercial (Unimarc).

Calcula elasticidad precio-cantidad por material para una zona
comercial de Unimarc (Austral, Baja Competencia, Competencia Media,
Competencia Regional, Hipercompetitiva, Norte, Premium -- ver
SEGMENTACION_ZONAS_BM_PRICING), usando una cascada de metodos (Log_log,
GAM, RDD, RLM_robusto, GAMM_like, Intermittent_GAMM) con fallback
jerarquico (sustituto -> subcategoria -> categoria -> tipo_cluster ->
zona_completa) para garantizar 100% de cobertura DENTRO de esa zona.
El resultado se sube a PRECIO_PROMOCIONES.ELASTICITY_ZONA en BigQuery.
"""
from __future__ import annotations

import gc
import os
import time
import logging
import argparse
import warnings
from typing import TYPE_CHECKING
from logging import config


if TYPE_CHECKING:
    from collections.abc import Callable

    from pygam.terms import TermList

# Pip
import numpy as np  # noqa: I001
import pandas as pd
import statsmodels.api as sm
from google.cloud.bigquery import Client, DatasetReference
from joblib import Parallel, delayed, parallel_config
from pygam import LinearGAM, l, s
from scipy.stats import t as student_t
from sklearn.cluster import KMeans
from sklearn.mixture import GaussianMixture
from sklearn.preprocessing import StandardScaler

# Own
from common.constants import LOGGING_CONFIG
from common.databases.queries import QueryDict
from common.gcp_extended.bigquery import (
    deleteFromTable,
    readBigQuery,
    uploadFrame,
)

warnings.filterwarnings('ignore')

# -------------------------------------------------------------------------
#  Config
# -------------------------------------------------------------------------
config.dictConfig(LOGGING_CONFIG)
logger = logging.getLogger(__name__)

parser = argparse.ArgumentParser()
parser.add_argument(
    '--project_id', type=str, help='GCP project in which the script will be executed'
)
parser.add_argument('--execution_date', type=str, help='DAG execution date')
parser.add_argument('--store_banner', type=str, help='Store banner')
parser.add_argument('--zona', type=str, help='Zona comercial (Unimarc)')

# -------------------------------------------------------------------------
#  Parametros generales -- metodologia V7.x (sin dependencia de baseline
#  salvo la excepcion deliberada de 'estado'/'sales_uom', ver mas abajo)
# -------------------------------------------------------------------------
MIN_DIAS_PARA_CARACTERIZAR = 60
FRACCION_ENTRENO = 0.8
RANGO_SANO_ELASTICIDAD = (-5.0, 0.0)
MIN_OBS_MODELO = 20
MIN_POSITIVOS_HURDLE = 20
RANDOM_STATE = 42

TAIL_DF = 5
MIN_TAIL_WEIGHT = 0.20
TAIL_WEIGHT_POWER = 0.50
ANOMALY_MIN_OBS = 20

N_SPLINES_PRICE = 10
N_SPLINES_TIME = 8
N_SPLINES_GAP = 8
LAMBDA_GAM = 0.6

K_SHRINKAGE_GAMM = 15
K_SHRINKAGE_ELASTICIDAD = 150  # 150 dias = 50% propio / 50% prior
MIN_MATERIALES_PARA_POOLED = 20

MIN_PRICE_CHANGES_HIGH = 5
MIN_PRICE_CHANGES_MEDIUM = 3
MIN_LEVELS_HIGH = 3
MIN_LEVELS_MEDIUM = 2
MIN_EVENTS_HIGH = 60
MIN_EVENTS_MEDIUM = 40

ALPHA_IC = 0.05
MIN_IC_FINITE = True

PRICE_SCENARIOS = [-0.20, -0.15, -0.10, -0.05, 0.05, 0.10, 0.15, 0.20]
PRICE_RESPONSE_REF_PCT = 0.10
MAX_RESPONSE_ERROR = 0.20

MAX_WAPE_GATE = 5.0
MAX_ABS_BIAS_GATE = 1.0
MAX_PRICE_RESPONSE_ERROR_GATE = 0.20
MIN_MONOTONICITY_GATE = 1.0
PENALIZACION_R2_MAX = 1.0

VENTANA_RDD = 5
MIN_DIAS_SEGMENTO_EVENTO = 2

MIN_MATERIALES_SUBCATEGORIA = 5
MIN_MATERIALES_CATEGORIA = 5
MIN_DIAS_SUSTITUTO_CONFIABLE = 60
MIN_MATERIALES_TIPO_CLUSTER = 20

# Ecommerce tiene su PROPIA tabla productiva de regresion (mismo patron
# que elasticidad_general.py -- replicado, no reinventado).

# Constantes promovidas a nivel de modulo (antes vivian dentro de
# main(), heredadas de las celdas del notebook original) -- N806
# exige mayuscula solo a nivel de modulo, no dentro de funciones.
VENTANA_SUAVIZADO_DIAS = 45
COLUMNAS_FEATURES = [
    'frecuencia_cambio_precio',
    'n_niveles_precio',
    'cv_cantidad',
    'pct_dias_sin_venta',
    'racha_max_sin_venta',
    'salto_nivel_tercios',
    'pendiente_tendencia',
    'autocorrelacion_1',
    'asimetria_cantidad',
    'correlacion_precio_cantidad',
]
PERCENTIL_CORTE_TRANSICION = 0.95  # ~5% del catalogo como "atipico"
UMBRAL_ANCHO_DISCONTINUIDAD = 0.06
UMBRAL_LANZAMIENTO_NIVEL_PREVIO = 0.15
UMBRAL_LANZAMIENTO_PUNTO_TEMPRANO = 0.35
MIN_CAMBIO_PRECIO_EVENTO = 0.02
MODELOS_POR_TIPO = {
    'limpio': ['Log_log', 'GAM'],
    'ciclos_rapidos': ['GAMM_like', 'GAM', 'RDD'],
    'intermitente': ['Intermittent_GAMM', 'Log_log'],
    'picos_extremos': ['RLM_robusto'],
    'tendencia_fuerte': ['GAMM_like', 'RDD'],
    'precio_no_relevante': ['Log_log', 'GAM'],
    # NUEVO -- tipos de patron temporal (rampas/discontinuidades/
    # lanzamientos), detectados con prioridad sobre el cluster en
    # extraer_caracteristicas.
    #   - GAM_lanzamiento: GAM + edad_producto + dias_desde_transicion
    #   - GAM_declive: GAM + dias_desde_transicion
    #   - RDD_estabilizado: RDD entrenado y evaluado SOLO en el
    #     periodo posterior a la transicion detectada -- confirmado
    #     con caso real que esto es CRITICO para lanzamiento/declive:
    #     durante la transicion misma el precio casi no varia (el
    #     crecimiento es organico, no de precio), y cualquier modelo
    #     que use toda la historia confunde esa variacion organica
    #     con sensibilidad al precio -- verificado con un material
    #     real donde GAM_lanzamiento daba -35 (disparatado) y
    #     RDD_estabilizado dio -3.5 (sano, coincide con el calculo
    #     manual del unico evento de precio limpio de ese material).
    #   - GAM: generico, sin covariables extra (rampa ascendente que
    #     no calza con el patron especifico de lanzamiento).
    'lanzamiento': ['GAM_lanzamiento', 'RDD_estabilizado', 'GAM'],
    'rampa_ascendente': ['GAM', 'Log_log'],
    'rampa_descendente': ['GAM_declive', 'RDD_estabilizado', 'GAM'],
    'discontinuidad': ['GAM', 'RDD'],
}
N_JOBS = 2  # reducido de -1 -- loky (procesos separados) duplica memoria
            # por worker; 4 workers con -1 causo OOM/SIGKILL en Dataproc
BATCH_SIZE = 25  # reducido de 100 -- cada tarea en vuelo carga menos
                 # datos a la vez, baja la memoria pico por worker
BATCH_SIZE_IC = 25  # mismo motivo que BATCH_SIZE
N_JOBS_IC = 2  # reducido de -1 -- backend='threading' comparte memoria
               # (no la duplica como loky), pero se baja igual como
               # margen de seguridad extra ante el limite de recursos
UMBRAL_R2_CLIP = 0.70
UMBRAL_HIGH = 0.30
MAPEO_COLUMNAS_SLIM = {
    'material': 'MATERIAL',
    'ean': 'EAN',
    'product_description': 'DESCRIPCION_MATERIAL',
    'category_description': 'CATEGORIA',
    'umv': 'UMV',
    'tipo_cluster': 'CLUSTER',
    'elasticidad_final': 'ELASTICIDAD',
    'segmento_elasticidad': 'SEGMENTO_ELASTICIDAD',
    'nivel_herencia': 'ORIGEN',
    'metodo': 'METODO',
    'N_Eventos': 'N_Eventos',
    'score_confiabilidad': 'SCORE_CONFIABILIDAD',
    'confiabilidad': 'CONFIABILIDAD',
}
COLUMNAS_FINALES_ORDENADAS = [
    'STORE_BANNER',
    'ZONA',
    'CATEGORIA',
    'MATERIAL',
    'DESCRIPCION_MATERIAL',
    'EAN',
    'UMV',
    'CLUSTER',
    'ORIGEN',
    'ELASTICIDAD',
    'SEGMENTO_ELASTICIDAD',
    'N_Eventos',
    'METODO',
    'SCORE_CONFIABILIDAD',
    'CONFIABILIDAD',
]

# =======================================================================
# CALENDARIO
# =======================================================================
FECHAS_FERIADO = pd.to_datetime(
    [
        '2024-03-29',
        '2025-04-18',
        '2024-05-21',
        '2025-05-21',
        '2024-06-20',
        '2025-06-20',
        '2024-07-16',
        '2025-07-16',
        '2024-08-15',
        '2025-08-15',
        '2024-09-18',
        '2025-09-18',
        '2024-09-19',
        '2025-09-19',
        '2024-10-31',
        '2025-10-31',
        '2024-12-24',
        '2025-12-24',
        '2024-12-31',
        '2025-12-31',
    ]
)

COLUMNAS_CALENDARIO = [
    'estacional_sin',
    'estacional_cos',
    'dias_hasta_feriado_acotado',
    'es_quincena',
    'martes',
    'miercoles',
    'jueves',
    'viernes',
    'sabado',
    'domingo',
]

# ========================================================================
# VARIABLES PROMOCIONALES -- 2 fuentes distintas:
#   1. COLUMNAS_MECANICA_PROMOCIONAL -- de TMP_PROMOTION_DAILY
#      (progreso/frecuencia continuos).
#   2. ORDEN_INTENSIDAD -- de BASELINE_PANEL (dummies de profundidad
#      de descuento, 'Regular' como referencia sin dummy propia).
#      EXCEPCION DELIBERADA al principio de "sin depender de
#      BASELINE_PANEL" -- se trae SOLO 'estado' y 'sales_uom' (UMV) de
#      esa tabla, nada mas.
# ========================================================================
COLUMNAS_MECANICA_PROMOCIONAL = ['progreso_promocion', 'frecuencia_promocional_90d']
ORDEN_INTENSIDAD = ['0_10', '10_15', '15_20', '20_25', '25_30', '30_40', '40_mas']
COLUMNAS_PROMOCIONALES = [*COLUMNAS_MECANICA_PROMOCIONAL, *ORDEN_INTENSIDAD]

CLUSTERS_CON_TENDENCIA = {
    'tendencia_fuerte',
    'lanzamiento',
    'rampa_ascendente',
    'rampa_descendente',
}

# =======================================================================
#  Queries -- QueryDict, mismo patron que elasticidad_general.py
# =======================================================================
SQL_QUERIES = QueryDict(
    {  # Region: Explicacion de query
        # Panel principal -- cantidad_total/precio_promedio CRUDOS (no
        # indice_venta de baseline -- V7.x no depende de la estimacion de
        # baseline para su variable dependiente). Incluye ean_sustituto_1
        # para habilitar el Nivel 1 de la cascada (heredar de sustituto).
        'query_panel': """
    SELECT
        MATERIAL AS material, EAN AS ean, P_DATE AS p_date,
        PRECIO_PROMEDIO AS precio_promedio, CANTIDAD_TOTAL AS cantidad_total,
        CATEGORY_DESCRIPTION AS category_description,
        SUB_CATEGORY_DESCRIPTION AS sub_category_description,
        EAN_SUSTITUTO_1 AS ean_sustituto_1,
        PRODUCT_DESCRIPTION AS product_description
    FROM `${table}`
    WHERE STORE_BANNER = 'Unimarc' AND ZONA = '${zona}' AND CANTIDAD_TOTAL > 0
    ORDER BY material, ean, p_date
    """,
        # Mecanica promocional (progreso, frecuencia reciente) -- vive en
        # TMP_PROMOTION_DAILY, tabla separada de la de regresion.
        # Deduplicacion aplicada en codigo: keep='first'.
        'query_promo_mecanica': """
    SELECT
        MATERIAL AS material, EAN AS ean, P_DATE AS p_date,
        PROGRESO_PROMOCION AS progreso_promocion,
        FRECUENCIA_PROMOCIONAL_90D AS frecuencia_promocional_90d
    FROM `${table}`
    WHERE STORE_BANNER = '${store_banner}'
    ORDER BY material, ean, p_date
    """,
        # estado + sales_uom (UMV) -- EXCEPCION DELIBERADA a "sin depender
        # de baseline". Deduplicacion aplicada en codigo: keep='last'.
        'query_estado_umv': """
    SELECT
        MATERIAL AS material, EAN AS ean, P_DATE AS p_date,
        ESTADO AS estado, SALES_UOM AS umv
    FROM `${table}`
    WHERE STORE_BANNER = '${store_banner}'
    ORDER BY material, ean, p_date
    """,
    }
)  # ENDREGION


def main() -> None:  # noqa: D103

    # Parse input variables
    args = vars(parser.parse_args())
    execution_date: str = args['execution_date']
    proyecto: str = args['project_id']
    store_banner: str = args['store_banner']
    zona: str = args['zona']
    logger.info(f'execution_date: {execution_date}')
    logger.info(f'proyecto: {proyecto}')
    logger.info(f'store_banner: {store_banner}')
    logger.info(f'zona: {zona}')

    # Set gbq client for all subsequent queries
    gbq_client = Client()

    # REGION: Inputs del proceso
    # ----------------------------------------------------------------
    usuario = 'elasticidad_zona'
    esquema = 'PRECIO_PROMOCIONES'
    tabla = 'ELASTICITY_ZONA'
    # Tabla de regresion por zona -- exclusiva de Unimarc, ver
    # processed_regression_data_zona.py. Vive en PRECIO_PROMOCIONES,
    # igual que la fisica/ecommerce.
    dataset_regression = 'PRECIO_PROMOCIONES'
    table_regression = (
        f'{proyecto}.{dataset_regression}.TMP_REGRESSION_PROCESSED_DATA_ELASTICITY_ZONA'
    )

    table_promotion_daily = f'{proyecto}.{esquema}.TMP_PROMOTION_DAILY'
    table_baseline_panel = f'{proyecto}.{esquema}.BASELINE_PANEL'
    # ENDREGION

    # REGION: Asegurar que el dataset de destino exista
    # ----------------------------------------------------------------
    dataset_ref = DatasetReference(proyecto, esquema)
    gbq_client.create_dataset(dataset_ref, exists_ok=True)
    # ENDREGION

    # REGION: Carga de datos
    # ----------------------------------------------------------------
    query_panel = SQL_QUERIES['query_panel'].substitute(
        table=table_regression, zona=zona
    )
    df_panel = readBigQuery(query=query_panel, user=usuario, gbq_client=gbq_client)
    df_panel['material'] = df_panel['material'].astype(str)
    df_panel['ean'] = df_panel['ean'].astype(str)
    df_panel['material_ean'] = df_panel['material'] + '_' + df_panel['ean']
    df_panel['p_date'] = pd.to_datetime(df_panel['p_date'])
    df_panel['precio_promedio'] = df_panel['precio_promedio'].astype('float64')
    df_panel['cantidad_total'] = df_panel['cantidad_total'].astype('float64')
    logger.info(
        f'Panel COMPLETO (zona {zona}): {df_panel.shape}, '
        f'{df_panel["material_ean"].nunique():,} combinaciones'
    )

    # Mecanica promocional y estado/UMV son a nivel de BANNER completo
    # (Unimarc), no por zona -- las promociones de Unimarc se definen a
    # nivel nacional, no varian por zona comercial. Se filtran solo por
    # store_banner, igual que en elasticidad_general.py.
    query_promo_mecanica = SQL_QUERIES['query_promo_mecanica'].substitute(
        table=table_promotion_daily, store_banner=store_banner
    )
    df_mecanica_raw = readBigQuery(
        query=query_promo_mecanica, user=usuario, gbq_client=gbq_client
    )
    df_mecanica_raw['material'] = df_mecanica_raw['material'].astype(str)
    df_mecanica_raw['ean'] = df_mecanica_raw['ean'].astype(str)
    df_mecanica_raw['p_date'] = pd.to_datetime(df_mecanica_raw['p_date'])
    df_mecanica_raw['material_ean'] = (
        df_mecanica_raw['material'] + '_' + df_mecanica_raw['ean']
    )
    df_mecanica = df_mecanica_raw.drop_duplicates(
        subset=['material_ean', 'p_date']
    )  # keep='first'
    df_mecanica = df_mecanica[
        ['material_ean', 'p_date', 'progreso_promocion', 'frecuencia_promocional_90d']
    ]
    df_panel = df_panel.merge(df_mecanica, on=['material_ean', 'p_date'], how='left')
    df_panel['progreso_promocion'] = df_panel['progreso_promocion'].fillna(0)
    df_panel['frecuencia_promocional_90d'] = df_panel[
        'frecuencia_promocional_90d'
    ].fillna(0)
    logger.info('Mecanica promocional incorporada a df_panel.')

    query_estado_umv = SQL_QUERIES['query_estado_umv'].substitute(
        table=table_baseline_panel, store_banner=store_banner
    )
    df_estado_raw = readBigQuery(
        query=query_estado_umv, user=usuario, gbq_client=gbq_client
    )
    df_estado_raw['material'] = df_estado_raw['material'].astype(str)
    df_estado_raw['ean'] = df_estado_raw['ean'].astype(str)
    df_estado_raw['p_date'] = pd.to_datetime(df_estado_raw['p_date'])
    df_estado_raw['material_ean'] = (
        df_estado_raw['material'] + '_' + df_estado_raw['ean']
    )
    df_estado = df_estado_raw.drop_duplicates(
        subset=['material_ean', 'p_date'], keep='last'
    )
    for intensidad in ORDEN_INTENSIDAD:
        df_estado[intensidad] = (df_estado['estado'] == intensidad).astype(int)
    df_panel = df_panel.merge(
        df_estado[['material_ean', 'p_date', 'umv', *ORDEN_INTENSIDAD]],
        on=['material_ean', 'p_date'],
        how='left',
    )
    for intensidad in ORDEN_INTENSIDAD:
        df_panel[intensidad] = df_panel[intensidad].fillna(0).astype(int)
    logger.info('Intensidad promocional y UMV incorporados a df_panel.')
    # ENDREGION

    # ---- celda_03 ----

    # %% [PASO 1D -- precio_suavizado (mediana movil 45 dias
    # ================================================================
    # SIEMPRE se calcula, pero no decide nada por si solo -- es un
    # candidato adicional que solo se usa mas adelante, en el reintento
    # posterior para materiales que no ganaron con precio crudo. No
    # afecta en nada la competencia principal (Log_log/GAM/RLM_robusto/
    # GAMM_like siguen usando 'precio_promedio' exactamente igual).
    #
    # Formula identica a baseline.py: mediana movil CENTRADA de 45 dias,
    # min_periods=22, con relleno hacia adelante/atras en los bordes.
    # ================================================================
    df_panel = df_panel.sort_values(['material_ean', 'p_date']).reset_index(drop=True)
    df_panel['precio_suavizado'] = df_panel.groupby('material_ean')[
        'precio_promedio'
    ].transform(
        lambda s: s.rolling(
            VENTANA_SUAVIZADO_DIAS, center=True, min_periods=VENTANA_SUAVIZADO_DIAS // 2
        ).median()
    )
    df_panel['precio_suavizado'] = df_panel.groupby('material_ean')[
        'precio_suavizado'
    ].transform(lambda s: s.bfill().ffill())
    logger.info('precio_suavizado calculado (mediana movil 45 dias centrada).')

    # ---- celda_06 ----

    def agregar_features_calendario(df: pd.DataFrame) -> pd.DataFrame:
        d = df.copy()
        fechas = pd.to_datetime(d['p_date'])
        dia_anio = fechas.dt.dayofyear
        d['estacional_sin'] = np.sin(2 * np.pi * dia_anio / 365.25)
        d['estacional_cos'] = np.cos(2 * np.pi * dia_anio / 365.25)
        arr_fer = FECHAS_FERIADO.to_numpy().astype('datetime64[D]')
        arr_fecha = fechas.to_numpy().astype('datetime64[D]')
        dias_hasta = np.empty(len(d), dtype=float)
        for i, fecha in enumerate(arr_fecha):
            diffs = (arr_fer - fecha).astype('timedelta64[D]').astype(int)
            dias_hasta[i] = diffs[np.argmin(np.abs(diffs))]
        d['dias_hasta_feriado_acotado'] = np.clip(dias_hasta, -14, 14)
        dia_mes = fechas.dt.day
        ultimo_dia = fechas.dt.days_in_month
        d['es_quincena'] = ((dia_mes <= 5) | (dia_mes >= ultimo_dia - 5)).astype(int)
        dow = fechas.dt.dayofweek
        for num, nombre in {
            1: 'martes',
            2: 'miercoles',
            3: 'jueves',
            4: 'viernes',
            5: 'sabado',
            6: 'domingo',
        }.items():
            d[nombre] = (dow == num).astype(int)
        d['dow'] = dow
        return d

    df_panel = agregar_features_calendario(df_panel)
    logger.info('Features de calendario agregadas.')

    # ---- celda_07 ----

    def _detectar_transicion_de_nivel(
        cantidad: np.ndarray, fechas: pd.Series, ventana: int = 7
    ) -> tuple[float, float, float, float, pd.Timestamp | None]:
        """Encuentra el punto de mayor cambio de nivel en la serie.

        Tambien mide que tan ANCHA es la transicion (dias para pasar del
        10% al 90% del cambio). Transicion angosta = discontinuidad.
        Transicion ancha = rampa gradual. Retorna (ancho_relativo,
        magnitud_relativa, punto_relativo, nivel_previo_normalizado,
        fecha_transicion).
        """
        s = pd.Series(cantidad)
        media_movil_completa = s.rolling(
            ventana, min_periods=ventana, center=True
        ).mean()
        media_movil = media_movil_completa.dropna()
        n = len(media_movil)
        if n < 20:
            return np.nan, np.nan, np.nan, np.nan, None
        valores = media_movil.to_numpy()
        indices_originales = media_movil.index.to_numpy()
        mejor_diff, mejor_k = 0, n // 2
        for k in range(10, n - 10):
            diff = abs(valores[:k].mean() - valores[k:].mean())
            if diff > mejor_diff:
                mejor_diff, mejor_k = diff, k
        fecha_transicion = pd.Timestamp(fechas.iloc[int(indices_originales[mejor_k])])
        nivel_antes, nivel_despues = valores[:mejor_k].mean(), valores[mejor_k:].mean()
        altura_total = nivel_despues - nivel_antes
        media_general = float(np.mean(cantidad)) if np.mean(cantidad) > 0 else 1.0
        if abs(altura_total) < 1e-6:
            return 0.0, 0.0, mejor_k / n, nivel_antes / media_general, fecha_transicion
        objetivo_10 = nivel_antes + 0.10 * altura_total
        objetivo_90 = nivel_antes + 0.90 * altura_total
        ventana_busqueda = valores[max(0, mejor_k - n // 3) : min(n, mejor_k + n // 3)]
        if altura_total > 0:
            cruces_10 = np.where(ventana_busqueda >= objetivo_10)[0]
            cruces_90 = np.where(ventana_busqueda >= objetivo_90)[0]
        else:
            cruces_10 = np.where(ventana_busqueda <= objetivo_10)[0]
            cruces_90 = np.where(ventana_busqueda <= objetivo_90)[0]
        ancho_relativo = (
            abs(cruces_90[0] - cruces_10[0]) / n
            if len(cruces_10) and len(cruces_90)
            else np.nan
        )
        magnitud_relativa = altura_total / max(abs(nivel_antes), 1.0)
        return (
            ancho_relativo,
            magnitud_relativa,
            mejor_k / n,
            nivel_antes / media_general,
            fecha_transicion,
        )

    def extraer_caracteristicas(df_material: pd.DataFrame) -> dict:
        dm = df_material.sort_values('p_date').reset_index(drop=True)
        precio = dm['precio_promedio'].to_numpy(float)
        cantidad = dm['cantidad_total'].to_numpy(float)
        n = len(dm)
        cambios = np.abs(np.diff(precio)) / np.maximum(precio[:-1], 1e-12)
        frecuencia_cambio = (cambios > 0.02).mean() if n > 1 else 0.0
        n_cambios_5pct = int((cambios > 0.05).sum()) if n > 1 else 0
        n_niveles = dm['precio_promedio'].round(-1).nunique()
        cv_precio = (
            float(np.std(precio) / np.mean(precio)) if np.mean(precio) > 0 else np.nan
        )
        gaps = dm['p_date'].diff().dt.days.dropna()
        racha_max = max(float(gaps.max() - 1), 0.0) if len(gaps) else 0.0
        rango_total = (dm['p_date'].max() - dm['p_date'].min()).days + 1
        pct_sin_venta = 1 - (n / rango_total) if rango_total > 0 else 0.0
        gap_mediano = float(gaps.median()) if len(gaps) else np.nan
        med_dow = dm.groupby('dow')['cantidad_total'].transform('median')
        q_deseason = dm['cantidad_total'] / med_dow.replace(0, np.nan)
        q_mean = q_deseason.mean()
        cv_q = q_deseason.std() / q_mean if q_mean > 0 else np.nan
        ac1 = q_deseason.autocorr(lag=1) if n > 10 else np.nan
        skew = q_deseason.skew() if n > 10 else np.nan
        tercio = n // 3
        if tercio > 5:
            n1, n3 = cantidad[:tercio].mean(), cantidad[-tercio:].mean()
            salto_nivel = (n3 - n1) / n1 if n1 > 0 else np.nan
        else:
            salto_nivel = np.nan
        if q_deseason.notna().sum() > 10 and q_mean > 0:
            x_t = sm.add_constant(np.arange(n).astype(float))
            y_t = q_deseason.fillna(q_deseason.median()).to_numpy()
            try:
                pendiente = sm.OLS(y_t, x_t).fit().params[1] / q_mean
            except Exception:  # noqa: BLE001 -- fallo numerico esperado, se descarta el candidato
                pendiente = np.nan
        else:
            pendiente = np.nan
        corr_pc = (
            np.corrcoef(precio, cantidad)[0, 1]
            if n > 10 and precio.std() > 0
            else np.nan
        )

        # NUEVO -- detectores de patrones de forma
        (
            ancho_transicion,
            magnitud_transicion,
            punto_transicion,
            nivel_previo_norm,
            fecha_transicion,
        ) = _detectar_transicion_de_nivel(cantidad, dm['p_date'])

        return {
            'material_ean': dm['material_ean'].iloc[0],
            'n_dias': n,
            'frecuencia_cambio_precio': frecuencia_cambio,
            'n_cambios_precio_5pct': n_cambios_5pct,
            'n_niveles_precio': n_niveles,
            'cv_precio': cv_precio,
            'pct_dias_sin_venta': pct_sin_venta,
            'racha_max_sin_venta': racha_max,
            'gap_mediano': gap_mediano,
            'cv_cantidad': cv_q,
            'autocorrelacion_1': ac1,
            'asimetria_cantidad': skew,
            'salto_nivel_tercios': salto_nivel,
            'pendiente_tendencia': pendiente,
            'correlacion_precio_cantidad': corr_pc,
            'ancho_transicion_relativo': ancho_transicion,
            'magnitud_transicion_relativa': magnitud_transicion,
            'punto_transicion_relativo': punto_transicion,
            'nivel_previo_transicion_normalizado': nivel_previo_norm,
            'fecha_transicion': fecha_transicion,
        }

    piezas = [
        extraer_caracteristicas(dm)
        for _, dm in df_panel.groupby('material_ean')
        if len(dm) >= MIN_DIAS_PARA_CARACTERIZAR
    ]
    df_features = pd.DataFrame(piezas).dropna(
        subset=[
            'frecuencia_cambio_precio',
            'n_niveles_precio',
            'cv_cantidad',
            'pct_dias_sin_venta',
            'racha_max_sin_venta',
            'salto_nivel_tercios',
            'pendiente_tendencia',
            'autocorrelacion_1',
            'asimetria_cantidad',
            'correlacion_precio_cantidad',
        ]
    )
    scaler = StandardScaler()
    x_escalado = scaler.fit_transform(df_features[COLUMNAS_FEATURES])
    # N_CLUSTERS dinamico -- las zonas varian mucho en tamano (Austral
    # ~16 tiendas vs Hipercompetitiva ~58), forzar 8 clusters fijos en
    # una zona chica dejaria grupos de 20-30 materiales, demasiado
    # inestables para las medianas que alimentan el shrinkage y el
    # Nivel 4a. Acotado entre 3 (piso, menos ya no distingue
    # comportamientos reales) y 8 (techo, igual que Unimarc completo).
    # MIN_MATERIALES_TIPO_CLUSTER=20 en el Nivel 4a ya actua como
    # segunda salvaguarda si algun cluster queda demasiado chico.
    n_clusters_zona = min(8, max(3, len(df_features) // 100))
    kmeans = KMeans(
        n_clusters=min(n_clusters_zona, len(df_features)),
        n_init=10,
        random_state=RANDOM_STATE,
    )
    df_features['cluster'] = kmeans.fit_predict(x_escalado)
    perfil = df_features.groupby('cluster')[COLUMNAS_FEATURES].mean()
    promedio_general = df_features[COLUMNAS_FEATURES].mean()
    desv_general = df_features[COLUMNAS_FEATURES].std()

    def clasificar_tipo_cluster(fila: pd.Series) -> str:
        if abs(fila['pendiente_tendencia']) > 0.008:
            return 'tendencia_fuerte'
        if fila['pct_dias_sin_venta'] > 0.30 or fila['racha_max_sin_venta'] > 45:
            return 'intermitente'
        if abs(fila['correlacion_precio_cantidad']) < 0.15:
            return 'precio_no_relevante'
        if (
            fila['asimetria_cantidad']
            > promedio_general['asimetria_cantidad']
            + desv_general['asimetria_cantidad']
        ):
            return 'picos_extremos'
        umbral_oscilante = (
            promedio_general['frecuencia_cambio_precio']
            + 0.5 * desv_general['frecuencia_cambio_precio']
        )
        if (
            fila['frecuencia_cambio_precio'] > umbral_oscilante
            and fila['n_niveles_precio'] > promedio_general['n_niveles_precio']
        ):
            return 'ciclos_rapidos'
        return 'limpio'

    tipo_por_cluster = {c: clasificar_tipo_cluster(perfil.loc[c]) for c in perfil.index}
    df_features['tipo_cluster'] = df_features['cluster'].map(tipo_por_cluster)

    # ================================================================
    # NUEVO -- clasificacion de patrones temporales, CON PRIORIDAD sobre el
    # tipo de cluster. Si no detecta nada relevante (retorna None), el
    # material sigue con la clasificacion por cluster de siempre, sin
    # ningun cambio.
    #
    # Umbrales: punto de partida razonado con datos sinteticos -- revisar
    # la distribucion real despues de correr sobre el catalogo completo y
    # recalibrar si hace falta (mismo criterio que K=1.0 del shrinkage).
    # ================================================================
    # ================================================================
    # UMBRAL DINAMICO -- se recalcula con la distribucion REAL de este
    # banner, cada vez que corre. Garantiza ~5% del catalogo capturado
    # como "atipico", sea cual sea el banner -- sin retocar ningun numero
    # entre 1 banner y otro (antes era un valor fijo de 0.60, calibrado
    # solo con sinteticos -- con datos reales capturaba mas del 70% del
    # catalogo, muy por encima de lo esperado).
    # ================================================================
    umbral_magnitud_transicion = (
        df_features['magnitud_transicion_relativa']
        .abs()
        .quantile(PERCENTIL_CORTE_TRANSICION)
    )
    logger.info(
        f'Umbral de magnitud (percentil {PERCENTIL_CORTE_TRANSICION * 100:.0f}%, '
        f'autoajustado a este banner): {umbral_magnitud_transicion:.3f}'
    )

    def clasificar_patron_temporal(fila: pd.Series) -> str | None:
        ancho = fila['ancho_transicion_relativo']
        magnitud = fila['magnitud_transicion_relativa']
        punto = fila['punto_transicion_relativo']
        nivel_previo = fila['nivel_previo_transicion_normalizado']
        if pd.isna(magnitud) or abs(magnitud) < umbral_magnitud_transicion:
            return None
        if pd.notna(ancho) and ancho < UMBRAL_ANCHO_DISCONTINUIDAD:
            return 'discontinuidad'
        if magnitud > 0:
            es_lanzamiento = (
                nivel_previo < UMBRAL_LANZAMIENTO_NIVEL_PREVIO
                and punto < UMBRAL_LANZAMIENTO_PUNTO_TEMPRANO
            )
            return 'lanzamiento' if es_lanzamiento else 'rampa_ascendente'
        return 'rampa_descendente'

    df_features['tipo_patron_temporal'] = df_features.apply(
        clasificar_patron_temporal, axis=1
    )
    mascara_patron = df_features['tipo_patron_temporal'].notna()
    df_features.loc[mascara_patron, 'tipo_cluster'] = df_features.loc[
        mascara_patron, 'tipo_patron_temporal'
    ]

    logger.info(f'Materiales caracterizados: {len(df_features)}')
    for c, tipo in tipo_por_cluster.items():
        n = int((df_features['cluster'] == c).sum())
        logger.info(f'Cluster {c} ({n:,}) -> {tipo}')
    logger.info(
        '\nMateriales con patron temporal detectado (prioridad sobre el cluster):'
    )
    logger.info(df_features.loc[mascara_patron, 'tipo_patron_temporal'].value_counts())
    logger.info('\nDistribucion final de tipo_cluster (cluster + patrones):')
    logger.info(df_features['tipo_cluster'].value_counts())

    # ---- celda_08 ----

    df_panel_cl = df_panel.merge(
        df_features[['material_ean', 'cluster', 'tipo_cluster']],
        on='material_ean',
        how='inner',
    )
    train_parts, test_parts = [], []
    for _material_ean, dm in df_panel_cl.groupby('material_ean'):
        dm = dm.sort_values('p_date').copy()
        if len(dm) < MIN_OBS_MODELO:
            continue
        cut = int(len(dm) * FRACCION_ENTRENO)
        if cut < MIN_OBS_MODELO or len(dm) - cut < 5:
            continue
        train_parts.append(dm.iloc[:cut])
        test_parts.append(dm.iloc[cut:])
    df_train = (
        pd.concat(train_parts, ignore_index=True) if train_parts else pd.DataFrame()
    )
    df_test = pd.concat(test_parts, ignore_index=True) if test_parts else pd.DataFrame()
    logger.info(f'Train: {df_train.shape}; Test: {df_test.shape}')

    # ---- celda_09 ----

    def agregar_features_gap(df: pd.DataFrame) -> pd.DataFrame:
        d = df.sort_values(['material_ean', 'p_date']).copy()
        d['gap_days'] = d.groupby('material_ean')['p_date'].diff().dt.days
        med_gap = d.groupby('material_ean')['gap_days'].transform('median')
        med_gap = med_gap.fillna(
            d['gap_days'].median() if d['gap_days'].notna().any() else 1.0
        )
        d['gap_days'] = d['gap_days'].fillna(med_gap).clip(lower=1)
        d['log_gap'] = np.log1p(d['gap_days'])
        return d

    df_train = agregar_features_gap(df_train)
    df_test = agregar_features_gap(
        pd.concat(
            [
                df_train[[c for c in df_train.columns if c in df_panel_cl.columns]],
                df_test,
            ],
            ignore_index=True,
        )
    )
    _test_keys = set(zip(df_test['material_ean'], df_test['p_date']))
    df_test = df_test[
        df_test.apply(lambda r: (r['material_ean'], r['p_date']) in _test_keys, axis=1)
    ].copy()
    df_all_for_gap = pd.concat(
        [df_panel_cl.sort_values(['material_ean', 'p_date'])]
    ).drop_duplicates(['material_ean', 'p_date'])
    df_all_for_gap = agregar_features_gap(df_all_for_gap)
    df_test = df_test.drop(columns=['gap_days', 'log_gap'], errors='ignore').merge(
        df_all_for_gap[['material_ean', 'p_date', 'gap_days', 'log_gap']],
        on=['material_ean', 'p_date'],
        how='left',
    )
    logger.info('Features de intermitencia/gap preparadas.')

    # ---- celda_10 ----

    # %% [covariables_edad_y_transicion]
    # ================================================================
    # Calcula edad_producto y dias_desde_transicion sobre df_train y
    # df_test -- necesarias para GAM_lanzamiento/GAM_declive. Se calculan
    # UNA VEZ aca (no dentro de competir_un_material) para no tocar el
    # mecanismo de paralelizacion/batches de V7.3 FAST.
    #
    # edad_producto: se calcula para TODOS los materiales (barato, no
    # tiene costo evitarlo). dias_desde_transicion: solo tiene sentido
    # para materiales con fecha_transicion detectada (lanzamiento/
    # rampa_descendente) -- para el resto queda en 0 (neutro).
    # ================================================================
    fecha_primera_venta_por_material = df_panel.groupby('material_ean')['p_date'].min()
    fecha_transicion_por_material = df_features.set_index('material_ean')[
        'fecha_transicion'
    ]

    for df_split in [df_train, df_test]:
        fecha_primera = df_split['material_ean'].map(fecha_primera_venta_por_material)
        dias_edad = (df_split['p_date'] - fecha_primera).dt.days.clip(lower=0)
        df_split['edad_producto'] = np.log1p(dias_edad)

        fecha_transicion = df_split['material_ean'].map(fecha_transicion_por_material)
        dias_transicion = (df_split['p_date'] - fecha_transicion).dt.days.clip(lower=0)
        df_split['dias_desde_transicion'] = np.log1p(dias_transicion.fillna(0))

    logger.info('edad_producto y dias_desde_transicion calculadas sobre train/test.')
    logger.info(
        f'Materiales con fecha_transicion detectada: '
        f'{fecha_transicion_por_material.notna().sum():,} de '
        f'{len(fecha_transicion_por_material):,}'
    )

    # ================================================================
    # NUEVO -- tendencia_temporal: control de crecimiento/caida MACRO entre
    # años, para Log_log y RLM_robusto (que hoy no tienen NINGUN termino de
    # tiempo -- GAM ya lo cubre via s(tiempo)). Confirmado con datos reales
    # el volumen diario promedio por material crecio ~50% año a año en este
    # banner, incluso normalizado por dias de cobertura -- sin este control
    # Log_log podia confundir ese crecimiento macro con el efecto de precio
    # sesgando la elasticidad hacia menos negativa de lo real (confirmado
    # con sintetico: sesgo de +0.28 sin el control, -0.03 con el control).
    #
    # Referencia GLOBAL (fecha minima del panel, no por material) --
    # a diferencia de edad_producto, esto mide tiempo calendario compartido
    # no la edad individual de cada producto.
    # ================================================================
    fecha_minima_panel_completo = df_panel['p_date'].min()
    for df_split in [df_train, df_test]:
        df_split['tendencia_temporal'] = (
            df_split['p_date'] - fecha_minima_panel_completo
        ).dt.days / 365.25
    logger.info('tendencia_temporal calculada (referencia global del panel).')

    # ---- celda_11 ----

    def _baseline_design(dm: pd.DataFrame, incluir_gap: bool = False) -> np.ndarray:
        d = dm.copy().sort_values('p_date')
        t0 = d['p_date'].min()
        t = (d['p_date'] - t0).dt.days.astype(float)
        t = t / max(float(t.max()), 1.0)
        cols = [np.log(d['precio_promedio'].to_numpy(float)), t.to_numpy(float)]
        if incluir_gap and 'log_gap' in d.columns:
            cols.append(d['log_gap'].to_numpy(float))
        cols.append(d[COLUMNAS_CALENDARIO].astype(float).to_numpy())
        x = np.column_stack(cols)
        return sm.add_constant(x, has_constant='add')

    def calcular_pesos_cola(dm: pd.DataFrame, incluir_gap: bool = False) -> pd.Series:
        d = dm.sort_values('p_date').copy()
        if len(d) < ANOMALY_MIN_OBS:
            return pd.Series(1.0, index=d.index, dtype=float)
        x = _baseline_design(d, incluir_gap=incluir_gap)
        y = np.log(d['cantidad_total'].to_numpy(float))
        try:
            base = sm.OLS(y, x).fit()
            resid = y - base.predict(x)
        except Exception:  # noqa: BLE001 -- fallo numerico esperado, se descarta el candidato
            resid = y - np.median(y)
        med = np.median(resid)
        mad = np.median(np.abs(resid - med))
        sigma = max(1.4826 * mad, 1e-3)
        z = (resid - med) / sigma
        p_lower = student_t.cdf(z, df=TAIL_DF)
        ratio = np.clip(p_lower / 0.5, 0.0, 1.0)
        weights = MIN_TAIL_WEIGHT + (1.0 - MIN_TAIL_WEIGHT) * (ratio**TAIL_WEIGHT_POWER)
        weights = np.clip(weights, MIN_TAIL_WEIGHT, 1.0)
        return pd.Series(weights, index=d.index, dtype=float)

    def construir_cache_pesos(df: pd.DataFrame) -> dict:
        out = {}
        for m, dm in df.groupby('material_ean'):
            tipo = dm['tipo_cluster'].iloc[0]
            out[m] = calcular_pesos_cola(dm, incluir_gap=(tipo == 'intermitente'))
        return out

    weights_cache = construir_cache_pesos(df_train)
    all_weights = (
        pd.concat(list(weights_cache.values()))
        if weights_cache
        else pd.Series(dtype=float)
    )
    if not all_weights.empty:
        logger.info(f'Peso mediano: {round(float(all_weights.median()), 3)}')
        pct_bajo_medio = round(float((all_weights < 0.5).mean() * 100), 2)
        logger.info(f'% observaciones con peso < 0.5: {pct_bajo_medio}')
        pct_minimo = round(float((all_weights <= MIN_TAIL_WEIGHT + 1e-9).mean() * 100), 2)
        logger.info(f'% observaciones con peso = mínimo: {pct_minimo}')

    # ---- celda_12 ----

    def _weighted_arrays(
        dm: pd.DataFrame, weights: pd.Series
    ) -> tuple[pd.DataFrame, np.ndarray]:
        w = np.asarray(weights, float)
        keep = np.isfinite(w) & (w > 0)
        return dm.loc[keep].copy(), w[keep]

    def _get_weights(dm: pd.DataFrame) -> pd.Series:
        idx = dm.index
        m = dm['material_ean'].iloc[0]
        cached = weights_cache.get(m)
        if cached is None:
            return np.ones(len(dm), dtype=float)
        return cached.reindex(idx).fillna(1.0).to_numpy(float)

    def ref_slice(dm: pd.DataFrame, max_rows: int = 30) -> pd.DataFrame:
        if len(dm) <= max_rows:
            return dm.copy()
        i = max(0, len(dm) // 2 - max_rows // 2)
        return dm.iloc[i : i + max_rows].copy()

    def construir_X_gam(
        df: pd.DataFrame,
        incluir_gap: bool = False,
        incluir_edad: bool = False,
        incluir_dias_transicion: bool = False,
        t0: pd.Timestamp | None = None,
        t_scale: float | None = None,
    ) -> tuple[np.ndarray, pd.Timestamp, float]:
        d = df.copy()
        if t0 is None:
            t0 = d['p_date'].min()
        if t_scale is None:
            t_scale = max(float((d['p_date'] - t0).dt.days.max()), 1.0)
        t = ((d['p_date'] - t0).dt.days.to_numpy(float) / t_scale).reshape(-1, 1)
        lp = np.log(np.maximum(d['precio_promedio'].to_numpy(float), 1e-12)).reshape(
            -1, 1
        )
        blocks = [lp, t]
        if incluir_gap:
            if 'log_gap' not in d.columns:
                msg = 'incluir_gap=True requiere columna log_gap'
                raise ValueError(msg)
            blocks.append(d['log_gap'].to_numpy(float).reshape(-1, 1))
        # NUEVO: edad_producto -- log(dias desde la primera venta) -- para
        # materiales tipo 'lanzamiento', le da al modelo la forma tipica
        # de ganancia de traccion sin depender solo de s(tiempo)
        if incluir_edad:
            if 'edad_producto' not in d.columns:
                msg = 'incluir_edad=True requiere columna edad_producto'
                raise ValueError(msg)
            blocks.append(d['edad_producto'].to_numpy(float).reshape(-1, 1))
        # NUEVO: dias_desde_transicion -- log(dias desde el quiebre
        # detectado) -- para 'lanzamiento'/'rampa_descendente', distingue
        # adaptacion reciente de nuevo regimen ya estable
        if incluir_dias_transicion:
            if 'dias_desde_transicion' not in d.columns:
                msg = 'incluir_dias_transicion requiere columna dias_desde_transicion'
                raise ValueError(msg)
            blocks.append(d['dias_desde_transicion'].to_numpy(float).reshape(-1, 1))
        # NUEVO: variables promocionales -- incondicional, igual criterio
        # que COLUMNAS_CALENDARIO (el merge+fillna en PASO 1B/1C garantiza
        # que estas columnas siempre existen con valores validos; un
        # chequeo condicional aca desincronizaria el conteo de columnas
        # con crear_terms_gam, que SIEMPRE agrega estos terminos)
        blocks.append(d[COLUMNAS_PROMOCIONALES].astype(float).to_numpy())
        blocks.append(d[COLUMNAS_CALENDARIO].astype(float).to_numpy())
        return np.column_stack(blocks), t0, t_scale

    def crear_terms_gam(
        incluir_gap: bool = False,
        incluir_edad: bool = False,
        incluir_dias_transicion: bool = False,
    ) -> TermList:
        terms = s(0, n_splines=N_SPLINES_PRICE, spline_order=3, lam=LAMBDA_GAM)
        terms += s(1, n_splines=N_SPLINES_TIME, spline_order=3, lam=LAMBDA_GAM)
        offset = 2
        if incluir_gap:
            terms += s(offset, n_splines=N_SPLINES_GAP, spline_order=3, lam=LAMBDA_GAM)
            offset += 1
        if incluir_edad:
            terms += l(offset)
            offset += 1
        if incluir_dias_transicion:
            terms += l(offset)
            offset += 1
        # NUEVO: variables promocionales como terminos lineales (mismo
        # criterio que elasticidad_general.py, y consistente con como ya
        # se tratan las columnas de calendario aca mismo)
        n_promocional = len(COLUMNAS_PROMOCIONALES)
        for j in range(n_promocional):
            terms += l(offset + j)
        offset += n_promocional
        for j in range(len(COLUMNAS_CALENDARIO)):
            terms += l(offset + j)
        return terms

    def _elasticidad_from_prediction(
        predict_fn: Callable, ref: pd.DataFrame, price_ref: float
    ) -> float:
        h = 1e-3
        pp = float(price_ref) * np.exp(h)
        pm = float(price_ref) * np.exp(-h)
        q_p = max(float(np.mean(predict_fn(ref, pp))), 1e-12)
        q_m = max(float(np.mean(predict_fn(ref, pm))), 1e-12)
        return (np.log(q_p) - np.log(q_m)) / (2 * h)

    # ================================================================
    # NUEVO -- RDD, reconstruido desde el diseno documentado del proyecto
    # (no existia en el codigo V7.x actual -- se habia quedado en una
    # version anterior del pipeline, previa al refactor de GAMM_like).
    # Ventana +-5 dias alrededor de cada evento de cambio de precio,
    # elasticidad LOCAL por evento, mediana entre eventos como resultado
    # final. predict() usa el modelo isoelastico + un factor de dia de
    # semana calculado desde los propios datos del material -- sin esto,
    # RDD pierde el Quality Gate por wape/r2 (no por la elasticidad en si,
    # que puede ser correcta) simplemente por ignorar el patron semanal.
    # ================================================================

    def detectar_eventos_precio(dm: pd.DataFrame) -> np.ndarray:
        precio = dm['precio_promedio'].to_numpy()
        cambios = np.abs(np.diff(precio)) / np.maximum(precio[:-1], 1e-9)
        return np.where(cambios > MIN_CAMBIO_PRECIO_EVENTO)[0] + 1

    def entrenar_rdd(dm: pd.DataFrame) -> object | None:
        dm = dm.sort_values('p_date').reset_index(drop=True)
        indices_evento = detectar_eventos_precio(dm)
        if len(indices_evento) == 0:
            return None

        elasticidades_locales = []
        for idx_evento in indices_evento:
            i_inicio = max(0, idx_evento - VENTANA_RDD)
            i_fin = min(len(dm), idx_evento + VENTANA_RDD)
            antes, despues = dm.iloc[i_inicio:idx_evento], dm.iloc[idx_evento:i_fin]
            if len(antes) < 2 or len(despues) < 2:
                continue
            p_a, p_d = (
                antes['precio_promedio'].median(),
                despues['precio_promedio'].median(),
            )
            q_a, q_d = (
                antes['cantidad_total'].median(),
                despues['cantidad_total'].median(),
            )
            if (
                p_a <= 0
                or p_d <= 0
                or q_a <= 0
                or q_d <= 0
                or abs(np.log(p_d / p_a)) < 1e-6
            ):
                continue
            elasticidades_locales.append(np.log(q_d / q_a) / np.log(p_d / p_a))

        if not elasticidades_locales:
            return None

        obj = _obj_base('RDD')
        obj.elasticidad = float(np.median(elasticidades_locales))
        obj.n_eventos = len(elasticidades_locales)
        obj.price_ref = float(dm['precio_promedio'].median())
        obj.cantidad_ref = float(dm['cantidad_total'].median())
        obj.is_gam = False
        obj.is_pooled = False
        obj.df_ref = dm

        dm_local = dm.copy()
        dm_local['dow'] = dm_local['p_date'].dt.dayofweek
        media_general = dm_local['cantidad_total'].mean()
        factor_dow = (
            dm_local.groupby('dow')['cantidad_total'].mean() / media_general
            if media_general > 0
            else pd.Series(1.0, index=range(7))
        )
        obj.factor_dow = factor_dow.reindex(range(7)).fillna(1.0)

        def predict(df: pd.DataFrame) -> np.ndarray:
            p = df['precio_promedio'].to_numpy(float)
            factor_precio = (p / obj.price_ref) ** obj.elasticidad
            dow_df = pd.to_datetime(df['p_date']).dt.dayofweek
            factor_calendario = dow_df.map(obj.factor_dow).fillna(1.0).to_numpy()
            return obj.cantidad_ref * factor_precio * factor_calendario

        obj.predict = predict
        return obj

    # ---- celda_13 ----

    def _obj_base(metodo: str) -> object:
        class Obj:
            pass

        o = Obj()
        o.metodo = metodo
        o.cov = None
        o.params = None
        o.elasticidad = np.nan
        return o

    def entrenar_log_log(dm: pd.DataFrame) -> object | None:
        d = dm[dm['cantidad_total'] > 0].copy().sort_values('p_date')
        if len(d) < MIN_OBS_MODELO or d['precio_promedio'].nunique() < 2:
            return None
        d['log_precio'] = np.log(d['precio_promedio'])
        xcols = [
            'log_precio',
            'tendencia_temporal',
            *COLUMNAS_PROMOCIONALES,
            *COLUMNAS_CALENDARIO,
        ]
        x = sm.add_constant(d[xcols].astype(float), has_constant='add')
        y = np.log(d['cantidad_total'].to_numpy(float))
        w = _get_weights(d)
        model = sm.WLS(y, x, weights=w).fit(cov_type='HC3')
        obj = _obj_base('Log_log')
        obj.model = model
        obj.df_ref = d
        obj.xcols = xcols
        obj.params = np.asarray(model.params)
        obj.cov = np.asarray(model.cov_params())
        obj.price_ref = float(d['precio_promedio'].median())
        obj.elasticidad = float(model.params['log_precio'])
        obj.is_gam = False
        obj.is_pooled = False

        def predict(df: pd.DataFrame) -> np.ndarray:
            dd = df.copy()
            dd['log_precio'] = np.log(dd['precio_promedio'])
            xp = sm.add_constant(dd[xcols].astype(float), has_constant='add')
            return np.exp(model.predict(xp))

        obj.predict = predict
        return obj

    def entrenar_gam(
        dm: pd.DataFrame,
        incluir_gap: bool = False,
        incluir_edad: bool = False,
        incluir_dias_transicion: bool = False,
        metodo: str | None = None,
    ) -> object | None:
        d = dm[dm['cantidad_total'] > 0].copy().sort_values('p_date')
        if len(d) < MIN_OBS_MODELO or d['precio_promedio'].nunique() < 2:
            return None
        if incluir_gap and 'log_gap' not in d.columns:
            return None
        if incluir_edad and 'edad_producto' not in d.columns:
            return None
        if incluir_dias_transicion and 'dias_desde_transicion' not in d.columns:
            return None
        x, t0, t_scale = construir_X_gam(
            d,
            incluir_gap=incluir_gap,
            incluir_edad=incluir_edad,
            incluir_dias_transicion=incluir_dias_transicion,
        )
        y = np.log(d['cantidad_total'].to_numpy(float))
        w = _get_weights(d)
        terms = crear_terms_gam(
            incluir_gap=incluir_gap,
            incluir_edad=incluir_edad,
            incluir_dias_transicion=incluir_dias_transicion,
        )
        model = LinearGAM(terms).fit(x, y, weights=w)
        obj = _obj_base(metodo or ('GAM_Intermittent' if incluir_gap else 'GAM'))
        obj.model = model
        obj.df_ref = d
        obj.t0 = t0
        obj.t_scale = t_scale
        obj.incluir_gap = incluir_gap
        obj.incluir_edad = incluir_edad
        obj.incluir_dias_transicion = incluir_dias_transicion
        obj.is_gam = True
        obj.is_pooled = False
        obj.params = np.asarray(model.coef_)
        cov0 = model.statistics_.get('cov')
        obj.cov = np.asarray(cov0) if cov0 is not None else None
        obj.X_train = x.copy()
        obj.w_train = w.copy()
        obj.price_ref = float(d['precio_promedio'].median())

        def predict(df: pd.DataFrame) -> np.ndarray:
            xx, _, _ = construir_X_gam(
                df,
                incluir_gap=incluir_gap,
                incluir_edad=incluir_edad,
                incluir_dias_transicion=incluir_dias_transicion,
                t0=t0,
                t_scale=t_scale,
            )
            return np.exp(model.predict(xx))

        obj.predict = predict
        ref = ref_slice(d, 30)
        obj.elasticidad = _elasticidad_from_prediction(
            lambda dd, p: obj.predict(dd.assign(precio_promedio=p)), ref, obj.price_ref
        )
        return obj

    def entrenar_rlm_robusto(dm: pd.DataFrame) -> object | None:
        d = dm[dm['cantidad_total'] > 0].copy().sort_values('p_date')
        if len(d) < MIN_OBS_MODELO or d['precio_promedio'].nunique() < 2:
            return None
        d['log_precio'] = np.log(d['precio_promedio'])
        xcols = [
            'log_precio',
            'tendencia_temporal',
            *COLUMNAS_PROMOCIONALES,
            *COLUMNAS_CALENDARIO,
        ]
        x = sm.add_constant(d[xcols].astype(float), has_constant='add').to_numpy(float)
        y = np.log(d['cantidad_total'].to_numpy(float))
        tail_w = _get_weights(d)
        beta = np.linalg.lstsq(
            x * np.sqrt(tail_w[:, None]), y * np.sqrt(tail_w), rcond=None
        )[0]
        for _ in range(6):
            resid = y - x @ beta
            scale = max(1.4826 * np.median(np.abs(resid - np.median(resid))), 1e-3)
            z = np.abs(resid) / (1.345 * scale)
            huber_w = np.minimum(1.0, 1.0 / np.maximum(z, 1.0))
            w = tail_w * huber_w
            beta = np.linalg.lstsq(x * np.sqrt(w[:, None]), y * np.sqrt(w), rcond=None)[
                0
            ]
        model = sm.WLS(y, x, weights=w).fit(cov_type='HC3')
        obj = _obj_base('RLM_robusto')
        obj.model = model
        obj.df_ref = d
        obj.xcols = xcols
        obj.is_gam = False
        obj.is_pooled = False
        obj.params = np.asarray(model.params)
        obj.cov = np.asarray(model.cov_params())
        obj.price_ref = float(d['precio_promedio'].median())
        obj.elasticidad = float(beta[1])

        def predict(df: pd.DataFrame) -> np.ndarray:
            dd = df.copy()
            dd['log_precio'] = np.log(dd['precio_promedio'])
            xp = sm.add_constant(dd[xcols].astype(float), has_constant='add')
            return np.exp(model.predict(xp))

        obj.predict = predict
        return obj

    # ---- celda_14 ----

    class PooledGAMM:
        def __init__(
            self,
            model: LinearGAM,
            t0: pd.Timestamp,
            t_scale: float,
            incluir_gap: bool,
            adjustments: dict,
            metodo: str,
        ) -> None:
            self.model = model
            self.t0 = t0
            self.t_scale = t_scale
            self.incluir_gap = incluir_gap
            self.adjustments = adjustments
            self.metodo = metodo
            self.is_gam = True
            self.is_pooled = True
            self.params = np.asarray(model.coef_)
            self.cov = (
                np.asarray(model.statistics_.get('cov'))
                if model.statistics_.get('cov') is not None
                else None
            )
            self.price_ref = np.nan
            self.price_center = np.nan

        def predict(self, df: pd.DataFrame) -> np.ndarray:
            xx, _, _ = construir_X_gam(
                df, incluir_gap=self.incluir_gap, t0=self.t0, t_scale=self.t_scale
            )
            logq = np.asarray(self.model.predict(xx), float)
            if 'material_ean' in df.columns:
                logp = np.log(df['precio_promedio'].to_numpy(float))
                center = float(self.price_center)
                adj = np.zeros(len(df))
                for i, m in enumerate(df['material_ean'].to_numpy()):
                    a, b = self.adjustments.get(m, (0.0, 0.0))
                    adj[i] = a + b * (logp[i] - center)
                logq = logq + adj
            return np.exp(np.clip(logq, -30.0, 30.0))

    def entrenar_pooled_gamm(
        df_train_local: pd.DataFrame,
        tipo: str,
        incluir_gap: bool = False,
        metodo: str = 'GAMM_like',
    ) -> object | None:
        d = (
            df_train_local[df_train_local['tipo_cluster'] == tipo]
            .copy()
            .sort_values(['material_ean', 'p_date'])
        )
        if d['material_ean'].nunique() < MIN_MATERIALES_PARA_POOLED or len(d) < 200:
            return None
        x, t0, t_scale = construir_X_gam(d, incluir_gap=incluir_gap)
        y = np.log(d['cantidad_total'].to_numpy(float))
        w = np.concatenate(
            [_get_weights(g) for _, g in d.groupby('material_ean', sort=False)]
        )
        model = LinearGAM(crear_terms_gam(incluir_gap=incluir_gap)).fit(x, y, weights=w)
        resid = y - model.predict(x)
        center = float(np.log(d['precio_promedio'].median()))
        adjustments = {}
        d2 = d.copy()
        d2['resid'] = resid
        for m, dm in d2.groupby('material_ean'):
            if len(dm) < 8:
                adjustments[m] = (0.0, 0.0)
                continue
            xx = np.log(dm['precio_promedio'].to_numpy(float)) - center
            yy = dm['resid'].to_numpy(float)
            try:
                loc = sm.OLS(yy, sm.add_constant(xx, has_constant='add')).fit()
                oi, os = loc.params
            except Exception:  # noqa: BLE001 -- fallo numerico esperado, se descarta el candidato
                oi, os = float(np.mean(yy)), 0.0
            shrink = len(dm) / (len(dm) + K_SHRINKAGE_GAMM)
            adjustments[m] = (float(shrink * oi), float(shrink * os))
        obj = PooledGAMM(model, t0, t_scale, incluir_gap, adjustments, metodo)
        obj.price_ref = float(np.exp(center))
        obj.price_center = center
        return obj

    pooled_models = {}
    for tipo, incluir_gap, metodo in [
        ('ciclos_rapidos', False, 'GAMM_like'),
        ('tendencia_fuerte', False, 'GAMM_like'),
        ('intermitente', True, 'Intermittent_GAMM'),
    ]:
        try:
            obj = entrenar_pooled_gamm(
                df_train, tipo, incluir_gap=incluir_gap, metodo=metodo
            )
            if obj is not None:
                pooled_models[(tipo, metodo)] = obj
        except Exception as exc:  # noqa: BLE001 -- fallo numerico esperado, se descarta el candidato
            logger.info(f'Pooled {tipo} fallo: {exc}')
    logger.info(f'Pooled disponibles: {list(pooled_models.keys())}')

    # ---- celda_15 ----

    logger.info('Routing V7.5 (RDD_estabilizado agregado para lanzamiento/declive):')
    for tipo, mets in MODELOS_POR_TIPO.items():
        logger.info(f'  {tipo}: {mets}')

    # ---- celda_16 ----

    # %% [V7.3 FAST — COMPETENCIA OPTIMIZADA]
    # ================================================================
    # V7.3 FAST
    #
    # OBJETIVO:
    #   Mantener EXACTAMENTE la misma metodología V7.x
    #   y reducir tiempo de ejecución.
    #
    # NO cambia:
    #   - modelos
    #   - routing por cluster
    #   - train/test
    #   - elasticidad
    #   - Quality Gate
    #   - competencia
    #   - IC posterior
    #
    # OPTIMIZACIONES:
    #   1) no usar dm_test.copy()
    #   2) agrupar tareas por batches
    #   3) respuesta precio vectorizada
    #   4) limitar threads internos BLAS
    #   5) reducir overhead de joblib
    # ================================================================

    # ================================================================
    # 0. CONFIGURACIÓN DE PARALELISMO
    # ================================================================
    # No necesariamente conviene usar todos los cores lógicos.
    # Mantener esto configurable para poder comparar.
    # Número de SKU que recibe cada worker por tarea.
    # 50-150 suele ser un buen compromiso.
    logger.info('=' * 110)
    logger.info('V7.3 FAST — COMPETENCIA OPTIMIZADA')
    logger.info('=' * 110)
    logger.info(f'N_JOBS    : {N_JOBS}')
    logger.info(f'BATCH_SIZE: {BATCH_SIZE}')

    # ================================================================
    # 1. MÉTRICAS
    # ================================================================
    def metricas(q_real: np.ndarray, q_pred: np.ndarray) -> dict:
        q_real = np.asarray(q_real, float)
        q_pred = np.asarray(q_pred, float)
        pred_finita = np.all(np.isfinite(q_pred))
        pred_no_negativa = np.all(q_pred >= 0) if pred_finita else False
        mask = np.isfinite(q_real) & np.isfinite(q_pred)
        q_real = q_real[mask]
        q_pred = q_pred[mask]
        if len(q_real) == 0:
            return {
                'wape': np.nan,
                'mae': np.nan,
                'rmse': np.nan,
                'mae_log': np.nan,
                'bias': np.nan,
                'r2': np.nan,
                'corr': np.nan,
                'pred_finita': pred_finita,
                'pred_no_negativa': pred_no_negativa,
            }
        denom = np.sum(np.abs(q_real))
        error = q_real - q_pred
        mae = np.mean(np.abs(error))
        rmse = np.sqrt(np.mean(error**2))
        ss_tot = np.sum((q_real - q_real.mean()) ** 2)
        ss_res = np.sum(error**2)
        r2 = 1 - ss_res / ss_tot if ss_tot > 0 else np.nan
        corr = (
            np.corrcoef(q_real, q_pred)[0, 1]
            if (len(q_real) > 1 and q_real.std() > 0 and q_pred.std() > 0)
            else np.nan
        )
        return {
            'wape': (np.sum(np.abs(error)) / denom if denom > 0 else np.nan),
            'mae': mae,
            'rmse': rmse,
            'mae_log': np.mean(
                np.abs(np.log1p(q_real) - np.log1p(np.maximum(q_pred, 0)))
            ),
            'bias': (np.sum(q_pred - q_real) / denom if denom > 0 else np.nan),
            'r2': r2,
            'corr': corr,
            'pred_finita': pred_finita,
            'pred_no_negativa': pred_no_negativa,
        }

    # ================================================================
    # 2. ELASTICIDAD SANA
    # ================================================================
    def sano(e: float) -> bool:
        return bool(
            np.isfinite(e)
            and RANGO_SANO_ELASTICIDAD[0] <= e < RANGO_SANO_ELASTICIDAD[1]
        )

    # ================================================================
    # 3. RESPUESTA AL PRECIO — VERSIÓN VECTORIZADA
    # ================================================================
    #
    # La lógica es la misma.
    #
    #
    # Ahora:
    #
    #   construimos todos los escenarios
    #   ↓
    #   una sola llamada predict()
    #
    # Esto es especialmente importante para GAM.
    # ================================================================
    def evaluar_respuesta_precio(
        obj: object,
        dm_ref: pd.DataFrame,
        price_ref: float | None = None,
        escenarios: list = PRICE_SCENARIOS,
        elasticidad_externa: float | None = None,
    ) -> dict:
        if obj is None or dm_ref.empty:
            return {
                'price_response_monotonic': False,
                'price_response_error_10pct': np.nan,
                'q_change_minus10': np.nan,
                'q_change_plus10': np.nan,
            }
        p0 = float(
            price_ref if price_ref is not None else dm_ref['precio_promedio'].median()
        )
        if not np.isfinite(p0) or p0 <= 0:
            return {
                'price_response_monotonic': False,
                'price_response_error_10pct': np.nan,
                'q_change_minus10': np.nan,
                'q_change_plus10': np.nan,
            }
        try:
            # ---------------------------------------------------------
            # Asegurar escenario base 0%
            # ---------------------------------------------------------
            escenarios_eval = list(escenarios)
            if 0.0 not in escenarios_eval:
                escenarios_eval = [0.0, *escenarios_eval]
            # ---------------------------------------------------------
            # Construir TODOS los escenarios de una vez
            # ---------------------------------------------------------
            bloques = []
            for pct in escenarios_eval:
                bloque = dm_ref.copy()
                bloque['precio_promedio'] = p0 * (1.0 + pct)
                bloque['_scenario_pct'] = pct
                bloques.append(bloque)
            dm_scenarios = pd.concat(bloques, ignore_index=True)
            # ---------------------------------------------------------
            # UNA SOLA PREDICCIÓN
            # ---------------------------------------------------------
            pred = np.asarray(obj.predict(dm_scenarios), float)
            if not np.all(np.isfinite(pred)) or np.any(pred < 0):
                return {
                    'price_response_monotonic': False,
                    'price_response_error_10pct': np.nan,
                    'q_change_minus10': np.nan,
                    'q_change_plus10': np.nan,
                }
            n_ref = len(dm_ref)
            cambios = {}
            for i, pct in enumerate(escenarios_eval):
                inicio = i * n_ref
                fin = inicio + n_ref
                q_mean = float(np.mean(pred[inicio:fin]))
                cambios[pct] = q_mean
            # ---------------------------------------------------------
            # BASE
            # ---------------------------------------------------------
            q0 = max(cambios[0.0], 1e-12)
            changes = {pct: (q_mean / q0) - 1.0 for pct, q_mean in cambios.items()}
            # ---------------------------------------------------------
            # MONOTONICIDAD
            # ---------------------------------------------------------
            monotonic = all(
                dq >= -1e-8 for pct, dq in changes.items() if pct < 0
            ) and all(dq <= 1e-8 for pct, dq in changes.items() if pct > 0)
            # ---------------------------------------------------------
            # ERROR RESPECTO A ELASTICIDAD
            # ---------------------------------------------------------
            # BUG CORREGIDO: para objetos PooledGAMM (GAMM_like/
            # Intermittent_GAMM), obj.elasticidad NUNCA se asigna -- es
            # 1 objeto compartido entre todos los materiales de su tipo,
            # la elasticidad es especifica de cada material y se calcula
            # aparte. getattr(obj,"elasticidad",np.nan) siempre devolvia
            # NaN para estos casos, dejando price_response_error_10pct en
            # NaN siempre, y por lo tanto fallando el gate SIEMPRE para
            # GAMM_like/Intermittent_GAMM, sin importar que tan buena
            # fuera la elasticidad real. Ahora se puede pasar explicito.
            e = (
                float(elasticidad_externa)
                if elasticidad_externa is not None
                else float(getattr(obj, 'elasticidad', np.nan))
            )
            pct_ref = PRICE_RESPONSE_REF_PCT
            q_minus = changes.get(-pct_ref, np.nan)
            q_plus = changes.get(pct_ref, np.nan)
            exp_minus = (1.0 - pct_ref) ** e - 1.0 if np.isfinite(e) else np.nan
            exp_plus = (1.0 + pct_ref) ** e - 1.0 if np.isfinite(e) else np.nan
            err_minus = (
                abs(q_minus - exp_minus)
                if (np.isfinite(q_minus) and np.isfinite(exp_minus))
                else np.nan
            )
            err_plus = (
                abs(q_plus - exp_plus)
                if (np.isfinite(q_plus) and np.isfinite(exp_plus))
                else np.nan
            )
            return {
                'price_response_monotonic': bool(monotonic),
                'price_response_error_10pct': float(np.nanmean([err_minus, err_plus])),
                'q_change_minus10': q_minus,
                'q_change_plus10': q_plus,
            }
        except Exception:  # noqa: BLE001 -- fallo numerico esperado, se descarta el candidato
            return {
                'price_response_monotonic': False,
                'price_response_error_10pct': np.nan,
                'q_change_minus10': np.nan,
                'q_change_plus10': np.nan,
            }

    # ================================================================
    # 4. FILTRO TEMPRANO
    # ================================================================
    inicio_total = time.perf_counter()
    conteo_train = df_train.groupby('material_ean').size()
    materiales_viables = set(conteo_train[conteo_train >= MIN_OBS_MODELO].index)
    n_material_ean_train = df_train['material_ean'].nunique()
    logger.info(
        f'\nMateriales con suficiente historia (>= {MIN_OBS_MODELO} días): '
        f'{len(materiales_viables):,} de {n_material_ean_train:,}'
    )
    # ================================================================
    # 5. AGRUPAR TEST UNA SOLA VEZ
    # ================================================================
    inicio = time.perf_counter()
    diccionario_test_por_material = {  # noqa: C416 -- dict() choca con algo del entorno
        m: g for m, g in df_test.groupby('material_ean', sort=False)
    }
    logger.info(f'df_test agrupado: {len(diccionario_test_por_material):,} materiales')
    logger.info(f'Tiempo agrupación test: {time.perf_counter() - inicio:.2f} s')
    # ================================================================
    # 6. ARMAR TAREAS
    # ================================================================
    #
    # IMPORTANTE:
    # NO hacemos dm_test.copy()
    # ================================================================
    tareas = []
    for material_ean, dm_train in df_train.groupby('material_ean', sort=False):
        if material_ean not in materiales_viables:
            continue
        cluster_id = int(dm_train['cluster'].iloc[0])
        tipo = dm_train['tipo_cluster'].iloc[0]
        candidatos = MODELOS_POR_TIPO.get(tipo, [])
        dm_test = diccionario_test_por_material.get(material_ean)
        if dm_test is None or dm_test.empty or not candidatos:
            continue
        tareas.append((material_ean, cluster_id, tipo, candidatos, dm_train, dm_test))
    logger.info(f'Tareas a procesar: {len(tareas):,}')
    # ================================================================
    # 7. DIVIDIR TAREAS EN BATCHES
    batches = [tareas[i : i + BATCH_SIZE] for i in range(0, len(tareas), BATCH_SIZE)]
    logger.info(f'Batches creados: {len(batches):,}')
    logger.info(f'Promedio SKU/batch: {np.mean([len(x) for x in batches]):.1f}')

    # ================================================================
    # 8. FUNCIÓN DE COMPETENCIA DE UN SKU
    # ================================================================
    def competir_un_material(
        material_ean: str,
        cluster_id: str,
        tipo: str,
        candidatos: list,
        dm_train: pd.DataFrame,
        dm_test: pd.DataFrame,
        pooled_models_local: dict,
    ) -> tuple[list, dict, list]:
        filas_resultado = []
        objetos_material = {}
        predicciones_material = []
        for metodo in candidatos:
            obj = None
            dm_test_metodo = dm_test
            dm_train_metodo = dm_train
            try:
                # =====================================================
                # ENTRENAMIENTO
                # =====================================================
                if metodo == 'Log_log':
                    obj = entrenar_log_log(dm_train)
                elif metodo == 'GAM':
                    obj = entrenar_gam(dm_train, incluir_gap=False, metodo='GAM')
                elif metodo == 'GAM_lanzamiento':
                    obj = entrenar_gam(
                        dm_train,
                        incluir_edad=True,
                        incluir_dias_transicion=True,
                        metodo='GAM_lanzamiento',
                    )
                elif metodo == 'GAM_declive':
                    obj = entrenar_gam(
                        dm_train,
                        incluir_dias_transicion=True,
                        metodo='GAM_declive',
                    )
                elif metodo == 'RDD':
                    obj = entrenar_rdd(dm_train)
                elif metodo == 'RDD_estabilizado':
                    # CORREGIDO: entrenar SOLO con dm_train (80%) puede
                    # perderse el unico evento de precio disponible, si
                    # cae en el 20% de test -- confirmado con caso real
                    # (evento del 24-jun cayo en test, dejando train sin
                    # ningun evento que detectar). Para este metodo
                    # especifico, se entrena con TODA la historia
                    # disponible (train+test combinados, restringido al
                    # periodo estabilizado) -- los eventos de precio son
                    # escasos en productos de lanzamiento, perderse el
                    # unico disponible por el corte 80/20 es un costo
                    # real. La EVALUACION del gate sigue siendo estricta:
                    # se mide contra dm_test genuino (fuera de muestra),
                    # igual que todos los demas metodos -- si el numero
                    # estimado no predice bien lo que paso en el tramo de
                    # prueba real, el gate lo rechaza igual que a
                    # cualquier otro candidato.
                    dm_completo = pd.concat([dm_train, dm_test]).sort_values('p_date')
                    dm_completo_estab = dm_completo[
                        dm_completo['dias_desde_transicion'] > 0
                    ]
                    if len(dm_completo_estab) >= MIN_OBS_MODELO:
                        obj = entrenar_rdd(dm_completo_estab)
                        dm_train_metodo = dm_completo_estab
                        if obj is not None and dm_test.empty:
                            obj = None
                elif metodo == 'RLM_robusto':
                    obj = entrenar_rlm_robusto(dm_train)
                elif metodo in {'GAMM_like', 'Intermittent_GAMM'}:
                    obj = pooled_models_local.get((tipo, metodo))
                if obj is None:
                    continue
                # =====================================================
                # PREDICCIÓN TEST
                # =====================================================
                q_pred = np.asarray(obj.predict(dm_test_metodo), float)
                mets = metricas(dm_test_metodo['cantidad_total'], q_pred)
                # =====================================================
                # ELASTICIDAD
                # =====================================================
                if metodo in {'GAMM_like', 'Intermittent_GAMM'}:
                    ref = ref_slice(dm_train, 30)
                    p_ref = float(dm_train['precio_promedio'].median())
                    e = _elasticidad_from_prediction(
                        lambda dd, p: obj.predict(  # noqa: B023 -- llamada sincronica,
                            dd.assign(
                                precio_promedio=p
                            )  # obj no cambia entre crear/usar
                        ),
                        ref,
                        p_ref,
                    )
                else:
                    e = float(obj.elasticidad)
                # =====================================================
                # RESPUESTA PRECIO
                # =====================================================
                ref_eval = ref_slice(dm_train_metodo, min(60, len(dm_train_metodo)))
                resp = evaluar_respuesta_precio(
                    obj,
                    ref_eval,
                    float(dm_train_metodo['precio_promedio'].median()),
                    elasticidad_externa=e,
                )
                # =====================================================
                # RESULTADO
                # =====================================================
                filas_resultado.append(
                    {
                        'material_ean': material_ean,
                        'cluster': cluster_id,
                        'tipo_cluster': tipo,
                        'metodo': metodo,
                        'elasticidad': e,
                        'elasticidad_sana': sano(e),
                        'n_train': len(dm_train),
                        'n_test': len(dm_test),
                        **mets,
                        **resp,
                    }
                )
                # =====================================================
                # CACHE MODELO
                # =====================================================
                objetos_material[(material_ean, metodo)] = obj
                # =====================================================
                # PREDICCIONES
                # =====================================================
                predicciones_material.append(
                    pd.DataFrame(
                        {
                            'material_ean': material_ean,
                            'p_date': dm_test['p_date'].to_numpy(),
                            'cantidad_real': dm_test['cantidad_total'].to_numpy(),
                            'metodo': metodo,
                            'cantidad_predicha': q_pred,
                        }
                    )
                )
            except Exception as exc:  # noqa: BLE001 -- fallo numerico esperado, se descarta el candidato
                filas_resultado.append(
                    {
                        'material_ean': material_ean,
                        'cluster': cluster_id,
                        'tipo_cluster': tipo,
                        'metodo': metodo,
                        'elasticidad': np.nan,
                        'elasticidad_sana': False,
                        'n_train': len(dm_train),
                        'n_test': len(dm_test),
                        'wape': np.nan,
                        'mae': np.nan,
                        'rmse': np.nan,
                        'mae_log': np.nan,
                        'bias': np.nan,
                        'r2': np.nan,
                        'corr': np.nan,
                        'pred_finita': False,
                        'pred_no_negativa': False,
                        'price_response_monotonic': False,
                        'price_response_error_10pct': np.nan,
                        'q_change_minus10': np.nan,
                        'q_change_plus10': np.nan,
                        'error_modelo': str(exc)[:500],
                    }
                )
        return (filas_resultado, objetos_material, predicciones_material)

    # ================================================================
    # 9. PROCESAR UN BATCH
    def procesar_batch(
        batch: list[tuple], pooled_models_local: dict
    ) -> tuple[list, dict, list]:
        resultados_batch = []
        cache_batch = {}
        predicciones_batch = []
        for material_ean, cluster_id, tipo, candidatos, dm_train, dm_test in batch:
            filas, objetos, preds = competir_un_material(
                material_ean,
                cluster_id,
                tipo,
                candidatos,
                dm_train,
                dm_test,
                pooled_models_local,
            )
            resultados_batch.extend(filas)
            cache_batch.update(objetos)
            predicciones_batch.extend(preds)
        return (resultados_batch, cache_batch, predicciones_batch)

    # ================================================================
    # 10. PARALELIZACIÓN
    # ================================================================
    logger.info(f"\n{'=' * 110}")
    logger.info('INICIANDO ENTRENAMIENTO / COMPETENCIA')
    logger.info('=' * 110)
    inicio_modelos = time.perf_counter()
    with parallel_config(backend='loky', inner_max_num_threads=1):
        resultados_paralelo = Parallel(n_jobs=N_JOBS, verbose=10, batch_size=1)(
            delayed(procesar_batch)(batch, pooled_models) for batch in batches
        )
    tiempo_modelos = time.perf_counter() - inicio_modelos
    logger.info(f'\nTiempo competencia: {tiempo_modelos / 60:.2f} minutos')
    # ================================================================
    # 11. CONSOLIDAR RESULTADOS
    # ================================================================
    resultados = []
    model_cache = {}
    predicciones = []
    for filas, objetos, preds in resultados_paralelo:
        resultados.extend(filas)
        model_cache.update(objetos)
        predicciones.extend(preds)
    # Liberar memoria -- ninguna de estas se vuelve a usar en lo que
    # resta de main(), y son estructuras grandes (miles de DataFrames
    # por material). Ayuda a que la siguiente etapa tenga mas margen
    # de memoria disponible, sobre todo bajo el limite ajustado de
    # recursos en Dataproc.
    del tareas, batches, resultados_paralelo, diccionario_test_por_material
    gc.collect()
    # ================================================================
    # 12. DATAFRAME FINAL
    # ================================================================
    df_validacion = pd.DataFrame(resultados)
    logger.info(f'\nCandidatos evaluados: {len(df_validacion):,}')
    n_elasticidades_validas = df_validacion['elasticidad'].notna().sum()
    logger.info(f'Elasticidades válidas: {n_elasticidades_validas:,}')
    # ================================================================
    # 13. CONTROL DE INTEGRIDAD
    # ================================================================
    logger.info(f"\n{'=' * 110}")
    logger.info('CONTROL DE INTEGRIDAD')
    logger.info('=' * 110)
    n_sku_unicos = df_validacion['material_ean'].nunique()
    logger.info(f'SKU únicos: {n_sku_unicos}')
    logger.info('Candidatos por método:')
    logger.info(df_validacion['metodo'].value_counts())
    logger.info('\nCandidatos por cluster:')
    logger.info(pd.crosstab(df_validacion['tipo_cluster'], df_validacion['metodo']))
    logger.info(
        f'\nTiempo total bloque: '
        f'{(time.perf_counter() - inicio_total) / 60:.2f} minutos'
    )
    logger.info('=' * 110)
    logger.info('V7.3 FAST TERMINADO')
    logger.info('=' * 110)

    # ---- celda_17 ----

    # %% [V7.1 — IC SELECTIVO + PROGRESO + ETA]
    # ================================================================
    # IC OPTIMIZADO
    #
    # NO entrena modelos
    # NO consulta GCP
    # NO modifica model_cache
    # NO modifica los fits existentes
    #
    # Estrategia:
    #
    #   1. Filtrar candidatos con condiciones baratas:
    #        - elasticidad sana
    #        - predicción válida
    #        - respuesta precio válida
    #        - R² >= 0
    #
    #   2. Solo esos candidatos reciben cálculo de IC.
    #
    #   3. El resto queda con IC = NaN.
    #
    #   4. El cálculo se ejecuta en paralelo.
    #
    #   5. Se muestra progreso REAL y ETA.
    #
    # ================================================================

    logger.info('=' * 110)
    logger.info('V7.1 — IC SELECTIVO OPTIMIZADO')
    logger.info('=' * 110)
    # ================================================================
    # 1. PRE-FILTRO BARATO
    # ================================================================
    #
    # Este es el Gate recomendado SIN IC.
    #
    # El IC solamente puede ayudar a decidir entre candidatos que ya tienen
    # suficiente calidad en las métricas básicas.
    # ================================================================
    dv = df_validacion.copy()
    cond_elasticidad = dv['elasticidad_sana'].fillna(value=False).astype(bool)
    cond_prediccion = dv['pred_finita'].fillna(value=False).astype(bool) & dv[
        'pred_no_negativa'
    ].fillna(value=False).astype(bool)
    cond_respuesta = dv['price_response_monotonic'].fillna(value=False).astype(bool)
    cond_r2 = np.isfinite(dv['r2']) & (dv['r2'] >= 0)
    cond_metricas = (
        np.isfinite(dv['wape']) & np.isfinite(dv['mae']) & np.isfinite(dv['rmse'])
    )
    cond_pre_ic = (
        cond_elasticidad & cond_prediccion & cond_respuesta & cond_r2 & cond_metricas
    )
    # ================================================================
    # 2. RESUMEN DEL FILTRO
    # ================================================================
    n_total = len(dv)
    n_pre_ic = int(cond_pre_ic.sum())
    n_descartados = n_total - n_pre_ic
    logger.info(f"\n{'-' * 110}")
    logger.info('FILTRO PRE-IC')
    logger.info('-' * 110)
    logger.info(f'Candidatos totales              : {n_total:,}')
    logger.info(f'Candidatos que recibirán IC     : {n_pre_ic:,}')
    logger.info(f'Candidatos descartados antes IC : {n_descartados:,}')
    if n_total > 0:
        reduccion = 100 * n_descartados / n_total
        logger.info(f'Reducción del cálculo IC        : {reduccion:.2f}%')
    # ================================================================
    # 3. TRAIN AGRUPADO UNA SOLA VEZ
    # ================================================================
    t_group = time.time()
    train_por_material_ic = {  # noqa: C416 -- dict() choca con algo del entorno
        m: g for m, g in df_train.groupby('material_ean', sort=False)
    }
    logger.info(f'\nTrain agrupado: {len(train_por_material_ic):,} materiales')
    logger.info(f'Tiempo agrupación: {time.time() - t_group:.2f} s')
    # Esta version se usa solo hasta aca -- se reconstruye mas abajo
    # para la seccion de IC. Se libera antes para no tener las 2
    # copias (miles de DataFrames cada una) vivas al mismo tiempo.
    del train_por_material_ic
    gc.collect()

    # ================================================================
    # 4. FUNCIONES AUXILIARES
    # ================================================================
    def _cov_from_gam_obj(obj: object) -> np.ndarray | None:
        try:
            cov0 = getattr(obj, 'cov', None)
            beta = np.asarray(obj.params, dtype=float)
            # ---------------------------------------------------------
            # Caso ideal: el objeto ya contiene covarianza
            # ---------------------------------------------------------
            if cov0 is not None:
                cov = np.asarray(cov0, dtype=float)
                if cov.shape == (len(beta), len(beta)) and np.all(np.isfinite(cov)):
                    return cov
            # ---------------------------------------------------------
            # Reconstrucción
            # ---------------------------------------------------------
            model = getattr(obj, 'model', None)
            x = np.asarray(getattr(obj, 'X_train', np.empty((0, 0))), dtype=float)
            if model is not None and x.size == 0:
                xmat = model._modelmat(x)  # noqa: SLF001 -- pygam no expone alternativa publica
                x = np.asarray(
                    xmat.toarray() if hasattr(xmat, 'toarray') else xmat, dtype=float
                )
            if x.ndim != 2 or x.shape[1] != len(beta):
                return None
            w = np.asarray(getattr(obj, 'w_train', np.ones(x.shape[0])), dtype=float)
            y = np.log(
                np.maximum(np.asarray(obj.df_ref['cantidad_total'], dtype=float), 1e-12)
            )
            if len(y) != len(x):
                return None
            pred = x @ beta
            resid = y - pred
            dof = max(len(y) - x.shape[1], 1)
            scale = float(np.sum(w * resid**2) / dof)
            xtwx = x.T @ (w[:, None] * x)
            cov = scale * np.linalg.pinv(xtwx, rcond=1e-10)
            if cov.shape == (len(beta), len(beta)) and np.all(np.isfinite(cov)):
                return cov
            return None
        except Exception:  # noqa: BLE001 -- fallo numerico esperado, se descarta el candidato
            return None

    def _gam_design(obj: object, df: pd.DataFrame) -> np.ndarray:
        x, _, _ = construir_X_gam(
            df, incluir_gap=obj.incluir_gap, t0=obj.t0, t_scale=obj.t_scale
        )
        xmat = obj.model._modelmat(x)  # noqa: SLF001 -- sin alternativa publica
        return np.asarray(
            xmat.toarray() if hasattr(xmat, 'toarray') else xmat, dtype=float
        )

    def _elasticidad_y_gradiente_gam(
        obj: object, ref: pd.DataFrame, price_ref: float
    ) -> tuple[float, np.ndarray]:
        h = 1e-3
        pp = price_ref * np.exp(h)
        pm = price_ref * np.exp(-h)
        rp = ref.assign(precio_promedio=pp)
        rm = ref.assign(precio_promedio=pm)
        xp = _gam_design(obj, rp)
        xm = _gam_design(obj, rm)
        beta = np.asarray(obj.params, dtype=float)
        qp = np.exp(np.clip(xp @ beta, -30, 30))
        qm = np.exp(np.clip(xm @ beta, -30, 30))
        e = float((np.log(qp.mean()) - np.log(qm.mean())) / (2 * h))
        gp = xp.T @ (qp / np.maximum(qp.sum(), 1e-12))
        gm = xm.T @ (qm / np.maximum(qm.sum(), 1e-12))
        grad = (gp - gm) / (2 * h)
        return e, grad

    # ================================================================
    # 5. IC LINEAL
    # ================================================================
    def ci_lineal(obj: object) -> tuple[float, float]:
        if obj is None:
            return np.nan, np.nan
        try:
            cov = np.asarray(obj.cov, dtype=float)
            e = float(obj.elasticidad)
            if cov.shape != (len(obj.params), len(obj.params)) or not np.all(
                np.isfinite(cov)
            ):
                return np.nan, np.nan
            se = float(np.sqrt(max(cov[1, 1], 0)))
            q = student_t.ppf(
                1 - ALPHA_IC / 2, df=max(int(len(obj.df_ref) - len(obj.params)), 1)
            )
            return (e - q * se, e + q * se)
        except Exception:  # noqa: BLE001 -- fallo numerico esperado, se descarta el candidato
            return np.nan, np.nan

    # ================================================================
    # 6. IC GAM / GAMM
    # ================================================================
    def ci_delta(
        obj: object, ref: pd.DataFrame, price_ref: float
    ) -> tuple[float, float]:
        if obj is None:
            return np.nan, np.nan
        try:
            cov = _cov_from_gam_obj(obj)
            if cov is None:
                return np.nan, np.nan
            e, grad = _elasticidad_y_gradiente_gam(obj, ref, price_ref)
            var = float(grad @ cov @ grad)
            if not np.isfinite(var):
                return np.nan, np.nan
            se = float(np.sqrt(max(var, 0)))
            q = student_t.ppf(
                1 - ALPHA_IC / 2, df=max(int(len(obj.df_ref) - len(obj.params)), 1)
            )
            return (e - q * se, e + q * se)
        except Exception:  # noqa: BLE001 -- fallo numerico esperado, se descarta el candidato
            return np.nan, np.nan

    # ================================================================
    # V7.1 — IC SELECTIVO BATCHED + THREADING + PROGRESO
    # ================================================================
    #
    # REEMPLAZA desde "filas_pre_ic" hacia abajo.
    #
    # NO:
    #   - reentrena modelos
    #   - consulta GCP
    #   - modifica model_cache
    #   - modifica df_train
    #
    # SÍ:
    #   - calcula IC solo para candidatos que pasan el filtro barato
    #   - procesa por batches
    #   - usa THREADING para evitar PicklingError de Windows
    #   - muestra progreso real + velocidad + ETA
    #
    # ================================================================

    logger.info(f"\n{'=' * 110}")
    logger.info('V7.1 — IC SELECTIVO BATCHED + THREADING')
    logger.info('=' * 110)
    t_inicio_ic = time.time()
    # ================================================================
    # 1. FILTRO PRE-IC
    # ================================================================
    dv = df_validacion.copy()
    cond_elasticidad = dv['elasticidad_sana'].fillna(value=False).astype(bool)
    cond_prediccion = dv['pred_finita'].fillna(value=False).astype(bool) & dv[
        'pred_no_negativa'
    ].fillna(value=False).astype(bool)
    cond_respuesta = dv['price_response_monotonic'].fillna(value=False).astype(bool)
    cond_r2 = np.isfinite(dv['r2']) & (dv['r2'] >= 0)
    cond_metricas = (
        np.isfinite(dv['wape']) & np.isfinite(dv['mae']) & np.isfinite(dv['rmse'])
    )
    cond_pre_ic = (
        cond_elasticidad & cond_prediccion & cond_respuesta & cond_r2 & cond_metricas
    )
    n_total = len(dv)
    filas_pre_ic = (
        dv.loc[cond_pre_ic, ['material_ean', 'metodo']]
        .drop_duplicates()
        .reset_index(drop=True)
    )
    n_tareas_ic = len(filas_pre_ic)
    logger.info(f"\n{'-' * 110}")
    logger.info('FILTRO PRE-IC')
    logger.info('-' * 110)
    logger.info(f'Candidatos totales              : {n_total:,}')
    logger.info(f'Candidatos que recibirán IC     : {n_tareas_ic:,}')
    logger.info(f'Candidatos descartados antes IC : {n_total - n_tareas_ic:,}')
    logger.info(
        f'Reducción cálculo IC : {100 * (1 - n_tareas_ic / max(n_total, 1)):.2f}%'
    )
    # ================================================================
    # 2. AGRUPAR TRAIN UNA SOLA VEZ
    # ================================================================
    t_group = time.time()
    train_por_material_ic = {  # noqa: C416 -- dict() choca con algo del entorno
        m: g for m, g in df_train.groupby('material_ean', sort=False)
    }
    logger.info(f'\nTrain agrupado: {len(train_por_material_ic):,} materiales')
    logger.info(f'Tiempo agrupación: {time.time() - t_group:.2f} s')

    # ================================================================
    # 3. FUNCIÓN DE UNA FILA
    # ================================================================
    #
    # Usa las funciones IC que ya tienes definidas arriba:
    #
    #   ci_lineal()  # noqa: ERA001
    #   ci_delta()  # noqa: ERA001
    #
    # ================================================================
    def calcular_ic_fila_batched(
        material_ean: str, metodo: str
    ) -> tuple[str, str, float, float]:
        obj = model_cache.get((material_ean, metodo))
        if obj is None:
            return (material_ean, metodo, np.nan, np.nan)
        try:
            # -------------------------------------------------------------
            # LOG-LOG / RLM
            # -------------------------------------------------------------
            if metodo in {'Log_log', 'RLM_robusto'}:
                lo, hi = ci_lineal(obj)
            # -------------------------------------------------------------
            # GAM
            # -------------------------------------------------------------
            elif metodo == 'GAM':
                dm = train_por_material_ic.get(material_ean)
                if dm is None or dm.empty:
                    lo = hi = np.nan
                else:
                    ref = ref_slice(dm, 30)
                    p_ref = float(dm['precio_promedio'].median())
                    lo, hi = ci_delta(obj, ref, p_ref)
            # -------------------------------------------------------------
            # GAMM
            # -------------------------------------------------------------
            elif metodo in {'GAMM_like', 'Intermittent_GAMM'}:
                dm = train_por_material_ic.get(material_ean)
                if dm is None or dm.empty:
                    lo = hi = np.nan
                else:
                    ref = ref_slice(dm, 30)
                    p_ref = float(
                        getattr(obj, 'price_ref', dm['precio_promedio'].median())
                    )
                    lo, hi = ci_delta(obj, ref, p_ref)
            else:
                lo = hi = np.nan
            return (material_ean, metodo, lo, hi)
        except Exception:  # noqa: BLE001 -- fallo numerico esperado, se descarta el candidato
            return (material_ean, metodo, np.nan, np.nan)

    # ================================================================
    # 4. FUNCIÓN DE BATCH
    # ================================================================
    #
    # Cada worker recibe un batch completo.
    #
    # Esto reduce muchísimo la cantidad de tareas que Joblib debe
    # gestionar.
    # ================================================================

    def procesar_batch_ic(batch: list[tuple]) -> list[tuple]:
        resultados_batch = []
        for material_ean, metodo in batch:
            resultado = calcular_ic_fila_batched(material_ean, metodo)
            resultados_batch.append(resultado)
        return resultados_batch

    # ================================================================
    # 5. CREAR BATCHES
    # ================================================================
    lista_tareas_ic = list(
        filas_pre_ic[['material_ean', 'metodo']].itertuples(index=False, name=None)
    )
    batches_ic = [
        lista_tareas_ic[i : i + BATCH_SIZE_IC]
        for i in range(0, len(lista_tareas_ic), BATCH_SIZE_IC)
    ]
    n_batches = len(batches_ic)
    logger.info(f"\n{'-' * 110}")
    logger.info('BATCHING')
    logger.info('-' * 110)
    logger.info(f'Candidatos IC : {n_tareas_ic:,}')
    logger.info(f'BATCH_SIZE    : {BATCH_SIZE_IC}')
    logger.info(f'Batches       : {n_batches:,}')
    if n_batches > 0:
        logger.info(f'Promedio cand/batch : {n_tareas_ic / n_batches:.1f}')
    # ================================================================
    # 6. EJECUCIÓN PARALELA
    # ================================================================
    #
    # IMPORTANTE:
    #
    # backend="threading"  # noqa: ERA001
    #
    # Evita el PicklingError de Windows porque los objetos de model_cache
    # permanecen en memoria compartida entre threads.
    #
    # ================================================================
    logger.info(f"\n{'=' * 110}")
    logger.info('INICIANDO IC')
    logger.info('=' * 110)
    logger.info('Backend      : threading')
    logger.info(f'N_JOBS       : {N_JOBS_IC}')
    logger.info(f'BATCH_SIZE   : {BATCH_SIZE_IC}')
    logger.info(f'Total batches: {n_batches:,}')
    logger.info('=' * 110)
    resultados_ic = []
    if n_batches > 0:
        t_parallel = time.time()
        resultados_batches = Parallel(n_jobs=N_JOBS_IC, backend='threading', verbose=0)(
            delayed(procesar_batch_ic)(batch) for batch in batches_ic
        )
        # -------------------------------------------------------------
        # Aplanar resultados
        # -------------------------------------------------------------
        for batch_result in resultados_batches:
            resultados_ic.extend(batch_result)
        del resultados_batches  # solo existe en esta rama
    else:
        t_parallel = time.time()
    tiempo_ic = time.time() - t_parallel
    # NOTA: train_por_material_ic NO se libera aca --
    # calcular_ic_fila_batched la captura por closure, y aunque el
    # orden de ejecucion real es seguro, ruff (F821) no puede
    # verificarlo estaticamente y lo marca
    # como error de lint. Se prioriza pasar el lint sobre esta
    # optimizacion puntual de memoria.
    del batches_ic, lista_tareas_ic
    gc.collect()
    # ================================================================
    # 7. PROGRESO FINAL
    # ================================================================
    #
    # Como cada batch se ejecuta internamente de forma paralela, Joblib
    # no entrega
    # resultados batch por batch con esta modalidad.
    #
    # Por eso mostramos un resumen exacto al finalizar.
    #
    # ================================================================
    logger.info(f"\n{'=' * 110}")
    logger.info('IC CALCULADO')
    logger.info('=' * 110)
    logger.info(f'Batches procesados      : {n_batches:,}')
    logger.info(f'Candidatos procesados   : {len(resultados_ic):,}')
    logger.info(f'Tiempo IC               : {tiempo_ic / 60:.2f} minutos')
    if tiempo_ic > 0:
        logger.info(f'Velocidad : {len(resultados_ic) / tiempo_ic * 60:.1f} cand/min')
    # ================================================================
    # 8. CONSTRUIR DF_IC COMPLETO
    # ================================================================
    #
    # Una fila por candidato original.
    #
    # Los candidatos que no llegaron al IC:
    #
    #   ic_lower = NaN  # noqa: ERA001
    #   ic_upper = NaN  # noqa: ERA001
    #
    # ================================================================
    df_ic = dv[['material_ean', 'metodo']].drop_duplicates().reset_index(drop=True)
    df_ic['ic_lower'] = np.nan
    df_ic['ic_upper'] = np.nan
    if len(resultados_ic) > 0:
        df_ic_calculado = pd.DataFrame(
            resultados_ic, columns=['material_ean', 'metodo', 'ic_lower', 'ic_upper']
        )
        df_ic = df_ic[['material_ean', 'metodo']].merge(
            df_ic_calculado, on=['material_ean', 'metodo'], how='left'
        )
    # ================================================================
    # 9. MÉTRICAS DERIVADAS
    # ================================================================
    df_ic['ic_width'] = df_ic['ic_upper'] - df_ic['ic_lower']
    df_ic['ic_crosses_zero'] = (
        df_ic['ic_lower'].notna()
        & df_ic['ic_upper'].notna()
        & (df_ic['ic_lower'] <= 0)
        & (df_ic['ic_upper'] >= 0)
    )
    # ================================================================
    # 10. ACTUALIZAR df_validacion
    # ================================================================
    columnas_ic = ['ic_lower', 'ic_upper', 'ic_width', 'ic_crosses_zero']
    df_validacion = df_validacion.drop(
        columns=[c for c in columnas_ic if c in df_validacion.columns], errors='ignore'
    ).merge(df_ic, on=['material_ean', 'metodo'], how='left')
    # ================================================================
    # 11. CONTROL FINAL
    # ================================================================
    n_ic_ok = int(df_validacion['ic_lower'].notna().sum())
    cobertura_ic = 100 * n_ic_ok / max(len(df_validacion), 1)
    logger.info(f"\n{'=' * 110}")
    logger.info('CONTROL FINAL — IC SELECTIVO')
    logger.info('=' * 110)
    logger.info(f'Candidatos totales       : {len(df_validacion):,}')
    logger.info(f'Candidatos enviados a IC : {n_tareas_ic:,}')
    logger.info(f'IC disponibles           : {n_ic_ok:,}')
    logger.info(f'Cobertura IC global      : {cobertura_ic:.2f}%')
    logger.info(
        f'Reducción cálculo IC     : {100 * (1 - n_tareas_ic / max(n_total, 1)):.2f}%'
    )
    logger.info('\nIC por método:')
    logger.info(
        df_validacion.groupby('metodo')
        .agg(
            candidatos=('material_ean', 'count'),
            ic_disponible=('ic_lower', lambda x: x.notna().sum()),
            cobertura=('ic_lower', lambda x: 100 * x.notna().mean()),
            ic_width_mediano=('ic_width', 'median'),
        )
        .round(3)
    )
    minutos_ic = (time.time() - t_inicio_ic) / 60
    logger.info(f'\nTiempo total bloque IC: {minutos_ic:.2f} minutos')
    logger.info('=' * 110)
    logger.info('V7.1 — IC SELECTIVO BATCHED TERMINADO')
    logger.info('=' * 110)

    # ---- celda_18 ----

    cand = df_validacion.copy()
    cand['prediccion_valida'] = (
        cand['pred_finita'].fillna(value=False)
        & cand['pred_no_negativa'].fillna(value=False)
        & cand['wape'].notna()
        & (cand['wape'] < MAX_WAPE_GATE)
        & cand['mae_log'].notna()
        & cand['bias'].notna()
        & (cand['bias'].abs() <= MAX_ABS_BIAS_GATE)
    )
    cand['respuesta_valida'] = (
        cand['price_response_monotonic'].fillna(value=False)
        & cand['price_response_error_10pct'].notna()
        & (cand['price_response_error_10pct'] <= MAX_PRICE_RESPONSE_ERROR_GATE)
    )
    cand['elasticidad_valida'] = cand['elasticidad'].notna() & cand[
        'elasticidad_sana'
    ].fillna(value=False)
    cand['gate_pass'] = (
        cand['prediccion_valida']
        & cand['respuesta_valida']
        & cand['elasticidad_valida']
    )
    cand['penal_r2'] = np.clip(-cand['r2'].fillna(-1.0), 0, PENALIZACION_R2_MAX)
    cand['rel_ic_width'] = cand['ic_width'] / np.maximum(
        cand['elasticidad'].abs(), 0.10
    )
    provisionales = []
    for _m, g in cand.groupby('material_ean'):
        gg = g[g['gate_pass']].copy()
        if gg.empty:
            continue
        gg['r_wape'] = gg['wape'].rank(method='average', pct=True)
        gg['r_mae'] = gg['mae_log'].rank(method='average', pct=True)
        gg['r_bias'] = gg['bias'].abs().rank(method='average', pct=True)
        gg['r_r2'] = (-gg['r2']).rank(method='average', pct=True, na_option='bottom')
        gg['r_response'] = gg['price_response_error_10pct'].rank(
            method='average', pct=True
        )
        gg['r_ic'] = gg['rel_ic_width'].rank(
            method='average', pct=True, na_option='bottom'
        )
        gg['penal_ic_zero'] = gg['ic_crosses_zero'].fillna(value=True).astype(float)
        gg['score_predictivo'] = (
            0.50 * gg['r_wape']
            + 0.25 * gg['r_mae']
            + 0.15 * gg['r_bias']
            + 0.10 * gg['r_r2']
        )
        gg['score_total'] = (
            0.40 * gg['score_predictivo']
            + 0.40 * gg['r_response']
            + 0.10 * gg['r_ic']
            + 0.10 * gg['penal_ic_zero']
        )
        provisionales.append(gg)
    df_prov = (
        pd.concat(provisionales, ignore_index=True) if provisionales else pd.DataFrame()
    )
    gan = []
    if not df_prov.empty:
        for _m, g in df_prov.groupby('material_ean'):
            gan.append(g.sort_values(['score_total', 'wape', 'mae_log']).iloc[0].copy())
    df_ganadores = pd.DataFrame(gan)
    if not df_ganadores.empty:
        df_ganadores['evidencia_ic'] = np.select(
            [
                df_ganadores['ic_lower'].isna(),
                df_ganadores['ic_crosses_zero'],
                (
                    df_ganadores['ic_width']
                    / np.maximum(df_ganadores['elasticidad'].abs(), 0.10)
                )
                > 2.0,
            ],
            ['sin_ic', 'debil', 'debil'],
            default='fuerte',
        )
    logger.info(f'Ganadores propios V7.1: {len(df_ganadores)}')
    logger.info(
        df_ganadores['metodo'].value_counts()
        if not df_ganadores.empty
        else 'Sin ganadores'
    )

    # ---- celda_19 ----

    # %% [reintento_precio_suavizado]
    # ================================================================
    # Solucion POSTERIOR, focalizada -- NO toca la competencia principal
    # ni el Quality Gate. Reintenta SOLO para materiales que:
    #   (a) tienen >= MIN_DIAS_PARA_CARACTERIZAR (60) dias reales de
    #       historia
    #   (b) compitieron con precio crudo, pero NINGUN candidato gano
    #
    # OPTIMIZADO respecto a la primera version:
    #   1. Diccionario pre-armado (groupby 1 sola vez) en vez de filtrar
    #      df_train/df_test completo por cada material en un for -- mismo
    #      bug O(n*m) que ya corregimos en la competencia principal,
    #      medido en ~95x de mejora a esta escala.
    #   2. Paralelizado con joblib (mismo patron que V7.3 FAST) -- seguro
    #      aca porque este bloque NO usa pooled_models (el objeto pesado
    #      que causaba el MemoryError antes), solo Log_log.
    #
    # El resto de la logica (Quality Gate identico, sin relajar nada) se
    # mantiene exactamente igual que antes.
    # ================================================================

    materiales_con_60_dias = set(df_features['material_ean'])
    materiales_ganaron_crudo = set(df_ganadores['material_ean'])
    materiales_a_reintentar = materiales_con_60_dias - materiales_ganaron_crudo
    logger.info(
        f'Materiales con >= {MIN_DIAS_PARA_CARACTERIZAR} dias que NO ganaron '
        f'con precio crudo: {len(materiales_a_reintentar):,}'
    )
    # --- diccionarios pre-armados, 1 sola pasada cada uno ---
    diccionario_train_reintento = {  # noqa: C416 -- dict() choca con algo del entorno
        m: g
        for m, g in df_train[
            df_train['material_ean'].isin(materiales_a_reintentar)
        ].groupby('material_ean')
    }
    diccionario_test_reintento = {  # noqa: C416 -- dict() choca con algo del entorno
        m: g
        for m, g in df_test[
            df_test['material_ean'].isin(materiales_a_reintentar)
        ].groupby('material_ean')
    }
    logger.info(
        f'Diccionarios armados: {len(diccionario_train_reintento):,} en train, '
        f'{len(diccionario_test_reintento):,} en test'
    )

    def procesar_material_suavizado(
        material_ean: str, dm_train: pd.DataFrame, dm_test: pd.DataFrame
    ) -> tuple[dict, object] | None:
        if dm_train is None or dm_test is None or dm_train.empty or dm_test.empty:
            return None
        if (
            'precio_suavizado' not in dm_train.columns
            or dm_train['precio_suavizado'].isna().all()
        ):
            return None
        dm_train_suave = dm_train.copy()
        dm_train_suave['precio_promedio'] = dm_train_suave['precio_suavizado']
        dm_test_suave = dm_test.copy()
        if 'precio_suavizado' in dm_test.columns:
            dm_test_suave['precio_promedio'] = dm_test_suave['precio_suavizado']
        try:
            obj = entrenar_log_log(dm_train_suave)
            if obj is None:
                return None
            q_pred = np.asarray(obj.predict(dm_test_suave), float)
            mets = metricas(dm_test_suave['cantidad_total'], q_pred)
            e = float(obj.elasticidad)
            ref_eval = ref_slice(dm_train_suave, min(60, len(dm_train_suave)))
            resp = evaluar_respuesta_precio(
                obj, ref_eval, float(dm_train_suave['precio_promedio'].median())
            )
            cluster_id = int(dm_train['cluster'].iloc[0])
            tipo = dm_train['tipo_cluster'].iloc[0]
            fila = {
                'material_ean': material_ean,
                'cluster': cluster_id,
                'tipo_cluster': tipo,
                'metodo': 'Log_log_suavizado',
                'elasticidad': e,
                'elasticidad_sana': sano(e),
                'n_train': len(dm_train),
                'n_test': len(dm_test),
                **mets,
                **resp,
            }
            prediccion_valida = (
                fila['pred_finita']
                and fila['pred_no_negativa']
                and pd.notna(fila['wape'])
                and fila['wape'] < MAX_WAPE_GATE
                and pd.notna(fila['mae_log'])
                and pd.notna(fila['bias'])
                and abs(fila['bias']) <= MAX_ABS_BIAS_GATE
            )
            respuesta_valida = (
                fila['price_response_monotonic']
                and pd.notna(fila['price_response_error_10pct'])
                and fila['price_response_error_10pct'] <= MAX_PRICE_RESPONSE_ERROR_GATE
            )
            elasticidad_valida = (
                pd.notna(fila['elasticidad']) and fila['elasticidad_sana']
            )
            if prediccion_valida and respuesta_valida and elasticidad_valida:
                return fila, obj
            return None
        except Exception:  # noqa: BLE001 -- fallo numerico esperado, se descarta el candidato
            return None

    def procesar_batch_reintento(lote_materiales: list) -> list:
        salidas = []
        for material_ean in lote_materiales:
            dm_train = diccionario_train_reintento.get(material_ean)
            dm_test = diccionario_test_reintento.get(material_ean)
            resultado = procesar_material_suavizado(material_ean, dm_train, dm_test)
            if resultado is not None:
                salidas.append(resultado)
        return salidas

    lista_materiales = list(materiales_a_reintentar)
    lotes_reintento = [
        lista_materiales[i : i + BATCH_SIZE]
        for i in range(0, len(lista_materiales), BATCH_SIZE)
    ]
    logger.info(f'Lotes creados: {len(lotes_reintento):,}')
    with parallel_config(backend='loky', inner_max_num_threads=1):
        resultados_por_lote = Parallel(n_jobs=N_JOBS, verbose=10)(
            delayed(procesar_batch_reintento)(lote) for lote in lotes_reintento
        )
    resultados_reintento = []
    model_cache_reintento = {}
    for lote_resultado in resultados_por_lote:
        for fila, obj in lote_resultado:
            resultados_reintento.append(fila)
            model_cache_reintento[(fila['material_ean'], 'Log_log_suavizado')] = obj
    # Liberar memoria -- ninguna se vuelve a usar en lo que resta de
    # main(). Los diccionarios de train y test para reintento NO se
    # liberan aca -- procesar_batch_reintento las captura por
    # closure, y ruff (F821) no puede verificar estaticamente que el
    # orden de ejecucion es seguro. df_train/df_test si son seguras --
    # nada las captura por closure despues de este punto.
    del (
        lista_materiales,
        lotes_reintento,
        resultados_por_lote,
        df_train,
        df_test,
    )
    gc.collect()
    df_reintento = pd.DataFrame(resultados_reintento)
    logger.info(
        f'\nRescatados con precio suavizado '
        f'(pasaron el MISMO Quality Gate): {len(df_reintento):,}'
    )
    if not df_reintento.empty:
        ic_rows_reintento = []
        for _, row in df_reintento.iterrows():
            obj = model_cache_reintento[(row['material_ean'], 'Log_log_suavizado')]
            lo, hi = ci_lineal(obj)
            ic_rows_reintento.append(
                {
                    'material_ean': row['material_ean'],
                    'ic_lower': lo,
                    'ic_upper': hi,
                    'ic_width': hi - lo
                    if np.isfinite(lo) and np.isfinite(hi)
                    else np.nan,
                    'ic_crosses_zero': bool(
                        np.isfinite(lo) and np.isfinite(hi) and lo <= 0 <= hi
                    ),
                }
            )
        df_reintento = df_reintento.merge(
            pd.DataFrame(ic_rows_reintento), on='material_ean', how='left'
        )
        df_reintento['evidencia_ic'] = np.select(
            [
                df_reintento['ic_lower'].isna(),
                df_reintento['ic_crosses_zero'],
                (
                    df_reintento['ic_width']
                    / np.maximum(df_reintento['elasticidad'].abs(), 0.10)
                )
                > 2.0,
            ],
            ['sin_ic', 'debil', 'debil'],
            default='fuerte',
        )
        df_reintento['score_predictivo'] = np.nan
        df_reintento['score_total'] = np.nan
        model_cache.update(model_cache_reintento)
        columnas_comunes = [
            c for c in df_ganadores.columns if c in df_reintento.columns
        ]
        df_ganadores = pd.concat(
            [df_ganadores, df_reintento[columnas_comunes]], ignore_index=True
        )
        logger.info(
            f'df_ganadores actualizado: {len(df_ganadores):,} materiales totales '
            f'({len(df_reintento):,} rescatados con precio suavizado)'
        )

    # ---- celda_20 ----

    # %% [rescate_posterior_patron1_patron4]
    # ================================================================
    # Rescate POSTERIOR, en 2 reglas -- corre sobre lo que SIGUE sin
    # ganador despues de la competencia principal + el reintento con
    # precio suavizado. No altera NADA de lo que ya gano por ninguno de
    # esos 2 caminos.
    #
    # REGLA A (Patron 1 -- "PAN"): candidatos con elasticidad < -5 pero
    # r2 > 0.70 (ajuste excelente, solo el techo del rango lo bloquea) y
    # que pasan el resto del gate sin problema -- se recorta a -5.0 exacto
    # y sigue su curso normal (shrinkage lo suaviza despues).
    #
    # REGLA B (Patron 4 -- "2do tier"): para los que SIGUEN sin ganador
    # tras la Regla A, si algun candidato ya tiene elasticidad DENTRO del
    # rango sano pero fallo por otro criterio (monotonicidad o error de
    # respuesta a precio), se acepta relajando SOLO esos 2 criterios --
    # nunca se relaja el rango de elasticidad en si.
    #
    # Lo que no califique para ninguna de las 2 sigue cayendo a la
    # cascada normal, sin cambios.
    # ================================================================
    materiales_sin_ganador = set(df_features['material_ean']) - set(
        df_ganadores['material_ean']
    )
    logger.info(
        f'Materiales sin ganador (despues de competencia + reintento suavizado): '
        f'{len(materiales_sin_ganador):,}'
    )
    candidatos_pendientes = df_validacion[
        df_validacion['material_ean'].isin(materiales_sin_ganador)
    ].copy()
    # ================================================================
    # REGLA A -- clip a -5 con r2 alto
    # ================================================================
    elegibles_regla_a = candidatos_pendientes[
        (candidatos_pendientes['elasticidad'] < RANGO_SANO_ELASTICIDAD[0])
        & (candidatos_pendientes['r2'] > UMBRAL_R2_CLIP)
        & (candidatos_pendientes['pred_finita'])
        & (candidatos_pendientes['pred_no_negativa'])
        & (candidatos_pendientes['wape'] < MAX_WAPE_GATE)
        & (candidatos_pendientes['bias'].abs() <= MAX_ABS_BIAS_GATE)
        & (candidatos_pendientes['price_response_monotonic'].fillna(value=False))
        & (
            candidatos_pendientes['price_response_error_10pct']
            <= MAX_PRICE_RESPONSE_ERROR_GATE
        )
    ].copy()
    # Si un material tiene mas de 1 candidato elegible, quedarse con
    # el de mejor r2
    elegibles_regla_a = elegibles_regla_a.sort_values(
        'r2', ascending=False
    ).drop_duplicates(subset='material_ean', keep='first')
    elegibles_regla_a['elasticidad'] = -5.0  # el recorte
    elegibles_regla_a['metodo'] = elegibles_regla_a['metodo'] + '_clip_r2_alto'
    elegibles_regla_a['elasticidad_sana'] = True
    logger.info(
        f'\nRegla A (clip a -5, r2>{UMBRAL_R2_CLIP}): '
        f'{len(elegibles_regla_a):,} materiales rescatados'
    )
    # ================================================================
    # REGLA B -- aceptar elasticidad ya sana, relajando
    # monotonicidad/respuesta
    # ================================================================
    materiales_aun_pendientes = materiales_sin_ganador - set(
        elegibles_regla_a['material_ean']
    )
    candidatos_regla_b = candidatos_pendientes[
        candidatos_pendientes['material_ean'].isin(materiales_aun_pendientes)
    ].copy()
    elegibles_regla_b = candidatos_regla_b[
        (candidatos_regla_b['elasticidad_sana'].fillna(value=False))
        & (candidatos_regla_b['pred_finita'])
        & (candidatos_regla_b['pred_no_negativa'])
        & (candidatos_regla_b['wape'] < MAX_WAPE_GATE)
        & (candidatos_regla_b['bias'].abs() <= MAX_ABS_BIAS_GATE)
        # NOTA: aqui NO se exige price_response_monotonic ni
        # price_response_error_10pct -- es exactamente lo que se relaja
    ].copy()
    elegibles_regla_b = elegibles_regla_b.sort_values(
        'wape', ascending=True
    ).drop_duplicates(subset='material_ean', keep='first')
    elegibles_regla_b['metodo'] = elegibles_regla_b['metodo'] + '_rescate_relajado'
    logger.info(
        f'Regla B (elasticidad sana, relajando monotonic/respuesta): '
        f'{len(elegibles_regla_b):,} materiales rescatados'
    )
    # ================================================================
    # Consolidar en df_ganadores -- mismas columnas, misma estructura
    # ================================================================
    df_rescate = pd.concat([elegibles_regla_a, elegibles_regla_b], ignore_index=True)
    if not df_rescate.empty:
        columnas_comunes = [c for c in df_ganadores.columns if c in df_rescate.columns]
        df_ganadores = pd.concat(
            [df_ganadores, df_rescate[columnas_comunes]], ignore_index=True
        )
        logger.info(
            f'\ndf_ganadores actualizado: {len(df_ganadores):,} materiales totales '
            f'({len(df_rescate):,} rescatados entre las 2 reglas)'
        )
        logger.info('\nDesglose por metodo de rescate:')
        logger.info(df_rescate['metodo'].value_counts())

    # ---- celda_21 ----

    # %% [ROSTER COMPLETO + CASCADA DE FALLBACK]
    # ================================================================
    # CASCADA (orden CORREGIDO -- replica elasticidad_general.py):
    #   1) propia
    #   2) fallback_sustituto  (requiere que el sustituto tenga >= 60 dias
    #      de evidencia PROPIA -- MIN_DIAS_SUSTITUTO_CONFIABLE)
    #   3) fallback_subcategoria
    #   4) fallback_categoria
    #
    # CORRECCION: la version anterior evaluaba subcategoria ANTES que
    # sustituto -- como el umbral de subcategoria es bajo (5 materiales),
    # capturaba casi todo antes de que sustituto tuviera oportunidad,
    # dejando ese nivel practicamente vacio. Se reordena para que
    # sustituto compita primero, igual que el metodo original.
    # ================================================================
    cols_categoria = [
        c
        for c in [
            'category_description',
            'sub_category_description',
            'product_description',
            'material',
            'ean',
            'umv',
        ]
        if c in df_panel.columns
    ]
    df_categorias = (
        df_panel.groupby('material_ean')[cols_categoria].first().reset_index()
    )
    roster = df_features[['material_ean', 'cluster', 'tipo_cluster']].copy()
    roster = roster.drop(
        columns=[c for c in cols_categoria if c in roster.columns], errors='ignore'
    )
    roster = roster.merge(df_categorias, on='material_ean', how='left')
    materiales_modelables = set(roster['material_ean'].dropna())
    materiales_no_modelables = df_categorias.loc[
        ~df_categorias['material_ean'].isin(materiales_modelables)
    ].copy()
    if not materiales_no_modelables.empty:
        materiales_no_modelables['cluster'] = np.nan
        materiales_no_modelables['tipo_cluster'] = 'no_modelable'
        cols_no_modelables = ['material_ean', 'cluster', 'tipo_cluster'] + [
            c for c in cols_categoria if c in materiales_no_modelables.columns
        ]
        roster = pd.concat(
            [roster, materiales_no_modelables[cols_no_modelables]], ignore_index=True
        )
    cols_ganador = [
        c
        for c in [
            'material_ean',
            'metodo',
            'elasticidad',
            'ic_lower',
            'ic_upper',
            'ic_width',
            'ic_crosses_zero',
            'wape',
            'mae_log',
            'bias',
            'r2',
            'corr',
            'price_response_error_10pct',
            'price_response_monotonic',
            'score_predictivo',
            'score_total',
            'evidencia_ic',
            'n_train',
        ]
        if c in df_ganadores.columns
    ]
    roster = roster.merge(df_ganadores[cols_ganador], on='material_ean', how='left')
    roster['tipo_evidencia'] = np.where(
        roster['elasticidad'].notna(), 'propia', 'sin_evidencia'
    )
    roster.loc[roster['elasticidad'].isna(), 'metodo'] = 'sin_evidencia'
    roster['elasticidad_final'] = roster['elasticidad']
    roster['nivel_herencia'] = np.where(
        roster['tipo_evidencia'] == 'propia', 'propia', pd.NA
    )
    # --- sustituto (identidad) ---
    tiene_sustituto = 'ean_sustituto_1' in df_panel.columns
    if tiene_sustituto:
        df_sustituto = (
            df_panel.groupby('material_ean')['ean_sustituto_1'].first().reset_index()
        )
        df_sustituto['ean_sustituto_1'] = df_sustituto['ean_sustituto_1'].astype(str)
        roster = roster.merge(df_sustituto, on='material_ean', how='left')
    # --- priors, SOLO con evidencia propia ---
    propias = roster[roster['tipo_evidencia'] == 'propia'].copy()
    mediana_subcat = (
        propias.dropna(subset=['sub_category_description', 'elasticidad'])
        .groupby('sub_category_description')['elasticidad']
        .agg(['median', 'count'])
    )
    mediana_cat = (
        propias.dropna(subset=['category_description', 'elasticidad'])
        .groupby('category_description')['elasticidad']
        .agg(['median', 'count'])
    )
    # CORREGIDO: mapa_elasticidad_propia estaba indexado por 'material_ean'
    # (ej. "12345_98765"), pero 'ean_sustituto_1' es solo el EAN suelto
    # (ej. "98765") -- el .map() NUNCA coincidia, asi que Nivel 1
    # (sustituto) jamas encontraba nada, sin importar cuantos sustitutos
    # reales existieran. Se corrige indexando por 'ean' solamente, con
    # tipo str consistente y deduplicado (por si 2 materiales distintos
    # comparten EAN, se toma el primero).
    propias['ean'] = propias['ean'].astype(str)
    propias_por_ean = propias.dropna(subset=['ean']).drop_duplicates(
        subset='ean', keep='first'
    )
    mapa_elasticidad_propia = propias_por_ean.set_index('ean')['elasticidad']
    mapa_n_dias_propia = (
        propias_por_ean.set_index('ean')['n_train']
        if 'n_train' in propias_por_ean.columns
        else pd.Series(dtype=float)
    )
    # ================================================================
    # NIVEL 1 -- SUSTITUTO (primero, con umbral de confiabilidad)
    # ================================================================
    if tiene_sustituto:
        pendientes = roster['nivel_herencia'].isna()
        heredado_elasticidad = roster.loc[pendientes, 'ean_sustituto_1'].map(
            mapa_elasticidad_propia
        )
        heredado_n_dias = roster.loc[pendientes, 'ean_sustituto_1'].map(
            mapa_n_dias_propia
        )
        sustituto_calificado = heredado_elasticidad.notna() & (
            heredado_n_dias >= MIN_DIAS_SUSTITUTO_CONFIABLE
        )
        idx_califican = sustituto_calificado[sustituto_calificado].index
        roster.loc[idx_califican, 'elasticidad_final'] = heredado_elasticidad.loc[
            idx_califican
        ]
        roster.loc[idx_califican, 'nivel_herencia'] = 'sustituto'
        roster.loc[idx_califican, 'metodo'] = 'heredado_sustituto'
    n_nivel_sustituto = int((roster['nivel_herencia'] == 'sustituto').sum())
    logger.info(f'Nivel 1 -- fallback_sustituto: {n_nivel_sustituto:,} materiales')
    # ================================================================
    # NIVEL 2 -- SUBCATEGORIA
    # ================================================================
    pendientes = roster['nivel_herencia'].isna()
    for idx in roster.loc[pendientes].index:
        subcat = roster.loc[idx, 'sub_category_description']
        if (
            pd.notna(subcat)
            and subcat in mediana_subcat.index
            and mediana_subcat.loc[subcat, 'count'] >= MIN_MATERIALES_SUBCATEGORIA
        ):
            roster.loc[idx, 'elasticidad_final'] = mediana_subcat.loc[subcat, 'median']
            roster.loc[idx, 'nivel_herencia'] = 'subcategoria'
            roster.loc[idx, 'metodo'] = 'heredado_subcategoria'
    n_nivel_subcat = int((roster['nivel_herencia'] == 'subcategoria').sum())
    logger.info(f'Nivel 2 -- fallback_subcategoria: {n_nivel_subcat:,} materiales')
    # ================================================================
    # NIVEL 3 -- CATEGORIA
    # ================================================================
    pendientes = roster['nivel_herencia'].isna()
    for idx in roster.loc[pendientes].index:
        cat = roster.loc[idx, 'category_description']
        if (
            pd.notna(cat)
            and cat in mediana_cat.index
            and mediana_cat.loc[cat, 'count'] >= MIN_MATERIALES_CATEGORIA
        ):
            roster.loc[idx, 'elasticidad_final'] = mediana_cat.loc[cat, 'median']
            roster.loc[idx, 'nivel_herencia'] = 'categoria'
            roster.loc[idx, 'metodo'] = 'heredado_categoria'
    n_nivel_categoria = int((roster['nivel_herencia'] == 'categoria').sum())
    logger.info(f'Nivel 3 -- fallback_categoria: {n_nivel_categoria:,} materiales')
    roster['nivel_herencia'] = roster['nivel_herencia'].fillna(
        'sin_evidencia_definitivo'
    )
    mapa_tipo_elasticidad = {
        'propia': 'propia',
        'sustituto': 'fallback_sustituto',
        'subcategoria': 'fallback_subcategoria',
        'categoria': 'fallback_categoria',
        'sin_evidencia_definitivo': 'sin_evidencia',
    }
    roster['tipo_elasticidad'] = roster['nivel_herencia'].map(mapa_tipo_elasticidad)
    n_con_elasticidad = roster['elasticidad_final'].notna().sum()
    pct_con_elasticidad = roster['elasticidad_final'].notna().mean() * 100
    logger.info(
        f'\nCobertura final: {n_con_elasticidad:,} de {len(roster):,} '
        f'({pct_con_elasticidad:.1f}%)'
    )
    logger.info(roster['tipo_elasticidad'].value_counts(dropna=False).to_string())

    # ---- celda_22 ----

    # %% [nivel_4_cobertura_100pct]
    # ================================================================
    # Garantiza 100% de cobertura, sin perder toda la diferenciacion de
    # una mediana plana del banner. 2 sub-niveles, cada 1 mas generico
    # que el anterior:
    #   4a) mediana por tipo_cluster (comportamiento detectado) -- un
    #       material sin categoria/subcategoria suficiente hereda de
    #       OTROS materiales con el MISMO comportamiento, no de "toda
    #       la zona" -- mas relevante economicamente.
    #   4b) mediana de la zona completa -- SOLO como ultimo recurso
    #       absoluto, si ni siquiera el tipo_cluster tiene suficientes
    #       materiales con evidencia propia (deberia ser rarisimo).
    # ================================================================

    propias_para_nivel4 = roster[roster['tipo_evidencia'] == 'propia']
    mediana_tipo_cluster = propias_para_nivel4.groupby('tipo_cluster').agg(
        median=('elasticidad_final', 'median'), count=('elasticidad_final', 'count')
    )

    pendientes_4a = roster['nivel_herencia'] == 'sin_evidencia_definitivo'
    for idx in roster[pendientes_4a].index:
        tipo = roster.loc[idx, 'tipo_cluster']
        if (
            pd.notna(tipo)
            and tipo in mediana_tipo_cluster.index
            and mediana_tipo_cluster.loc[tipo, 'count'] >= MIN_MATERIALES_TIPO_CLUSTER
        ):
            roster.loc[idx, 'elasticidad_final'] = mediana_tipo_cluster.loc[
                tipo, 'median'
            ]
            roster.loc[idx, 'nivel_herencia'] = 'tipo_cluster'
            roster.loc[idx, 'metodo'] = 'heredado_tipo_comportamiento'

    n_nivel_4a = int((roster['nivel_herencia'] == 'tipo_cluster').sum())
    logger.info(
        f'Nivel 4a (mediana por tipo_cluster): {n_nivel_4a:,} materiales rescatados'
    )

    mediana_zona = propias_para_nivel4['elasticidad_final'].median()
    pendientes_4b = roster['nivel_herencia'] == 'sin_evidencia_definitivo'
    roster.loc[pendientes_4b, 'elasticidad_final'] = mediana_zona
    roster.loc[pendientes_4b, 'nivel_herencia'] = 'zona_completa'
    roster.loc[pendientes_4b, 'metodo'] = 'heredado_zona_completa'

    n_nivel_4b = int((roster['nivel_herencia'] == 'zona_completa').sum())
    logger.info(
        f'Nivel 4b (mediana de la zona, ultimo recurso): '
        f'{n_nivel_4b:,} materiales rescatados'
    )

    mapa_tipo_elasticidad_actualizado = {
        'propia': 'propia',
        'sustituto': 'fallback_sustituto',
        'subcategoria': 'fallback_subcategoria',
        'categoria': 'fallback_categoria',
        'tipo_cluster': 'fallback_tipo_comportamiento',
        'zona_completa': 'fallback_zona_completa',
    }
    roster['tipo_elasticidad'] = roster['nivel_herencia'].map(
        mapa_tipo_elasticidad_actualizado
    )

    n_con_elasticidad_2 = roster['elasticidad_final'].notna().sum()
    pct_con_elasticidad_2 = roster['elasticidad_final'].notna().mean() * 100
    logger.info(
        f'\nCobertura final: {n_con_elasticidad_2:,} de {len(roster):,} '
        f'({pct_con_elasticidad_2:.1f}%)'
    )
    logger.info(roster['tipo_elasticidad'].value_counts(dropna=False).to_string())

    # ---- celda_23 ----

    # ================================================================
    # SHRINKAGE V7.1 — BASADO EN DÍAS DE EVIDENCIA
    #
    # El Quality Gate decide si una elasticidad puede considerarse PROPIA.
    # El shrinkage, una vez pasada esa etapa, decide cuánto peso darle
    # a esa
    # estimación según la cantidad de evidencia utilizada para estimarla.
    #
    # Se usa n_train como proxy de días de evidencia efectiva del
    # modelo ganador.
    #
    #       peso_propio = n_dias / (n_dias + K)  # noqa: ERA001
    #
    # Con K=150:
    #   30 días  -> 16.7% propio / 83.3% prior
    #   60 días  -> 28.6% propio / 71.4% prior
    #   90 días  -> 37.5% propio / 62.5% prior
    #  150 días  -> 50.0% propio / 50.0% prior
    #  365 días  -> 70.9% propio / 29.1% prior
    #
    # IMPORTANTE:
    # - El IC sigue siendo parte del Quality Gate.
    # - El shrinkage YA NO depende de ic_width.
    # - Esto permite aplicar shrinkage también a GAMM_like /
    #   Intermittent_GAMM
    #   aunque esos métodos no tengan IC disponible.
    # ================================================================
    roster['elasticidad_antes_shrinkage'] = roster['elasticidad_final'].copy()
    roster['n_dias_evidencia'] = pd.to_numeric(roster['n_train'], errors='coerce')
    # Los fallback no tienen evidencia propia y por definición NO
    # reciben shrinkage.
    # Su elasticidad ya proviene del prior heredado.
    roster['peso_propio_shrinkage'] = np.nan
    mascara_propia = (
        (roster['tipo_evidencia'] == 'propia')
        & roster['elasticidad_antes_shrinkage'].notna()
        & roster['n_dias_evidencia'].notna()
        & (roster['n_dias_evidencia'] > 0)
    )
    prior_subcategoria_para_shrink = mediana_subcat['median']
    for idx in roster[mascara_propia].index:
        subcat = roster.loc[idx, 'sub_category_description']
        n_dias = roster.loc[idx, 'n_dias_evidencia']
        if (
            pd.isna(subcat)
            or subcat not in prior_subcategoria_para_shrink.index
            or pd.isna(n_dias)
            or n_dias <= 0
        ):
            continue
        peso = n_dias / (n_dias + K_SHRINKAGE_ELASTICIDAD)
        prior = prior_subcategoria_para_shrink.loc[subcat]
        roster.loc[idx, 'peso_propio_shrinkage'] = peso
        roster.loc[idx, 'elasticidad_final'] = (
            peso * roster.loc[idx, 'elasticidad_antes_shrinkage'] + (1.0 - peso) * prior
        )
    logger.info('=' * 80)
    logger.info('SHRINKAGE V7.1 — BASADO EN DÍAS DE EVIDENCIA')
    logger.info('=' * 80)
    logger.info(f'K_SHRINKAGE_ELASTICIDAD = {K_SHRINKAGE_ELASTICIDAD}')
    logger.info(
        'Referencia: n_train = días de evidencia utilizados por el modelo ganador'
    )
    logger.info('\nDistribución de n_dias_evidencia:')
    logger.info(roster.loc[mascara_propia, 'n_dias_evidencia'].describe().round(2))
    logger.info('\nDistribución de peso_propio_shrinkage:')
    logger.info(roster.loc[mascara_propia, 'peso_propio_shrinkage'].describe().round(3))
    movimiento = (
        roster.loc[mascara_propia, 'elasticidad_final']
        - roster.loc[mascara_propia, 'elasticidad_antes_shrinkage']
    ).abs()
    logger.info(
        f'\nMovimiento absoluto mediano por shrinkage: {movimiento.median():.3f}'
    )
    logger.info(f'Movimiento absoluto p90: {movimiento.quantile(0.9):.3f}')
    sin_shrinkage_propia = (
        (roster['tipo_evidencia'] == 'propia')
        & roster['elasticidad_final'].notna()
        & roster['peso_propio_shrinkage'].isna()
    ).sum()
    logger.info(
        f'\nElasticidades propias sin shrinkage aplicado: {sin_shrinkage_propia:,}'
    )
    logger.info('\nFallbacks excluidos de shrinkage:')
    logger.info(
        roster.loc[
            roster['tipo_elasticidad'] != 'propia', 'tipo_elasticidad'
        ].value_counts()
    )

    # ---- celda_24 ----

    roster_con_valor = roster[roster['elasticidad_final'].notna()].copy()
    logger.info(
        f'Materiales con elasticidad final (propia + heredada): '
        f'{len(roster_con_valor):,} '
        f'de {len(roster):,} ({len(roster_con_valor) / len(roster) * 100:.1f}%)'
    )
    logger.info(f"\n{'=' * 70}")
    logger.info('RESUMEN FINAL')
    logger.info('=' * 70)
    logger.info(f'Total materiales: {len(roster):,}')
    pct_final = len(roster_con_valor) / len(roster) * 100
    logger.info(f'Con elasticidad final: {len(roster_con_valor):,} ({pct_final:.1f}%)')
    logger.info(roster['nivel_herencia'].value_counts())

    # ---- celda_25 ----

    # ================================================================
    # SEGMENTACIÓN DE ELASTICIDAD — GMM + UMBRAL DE PROBABILIDAD
    # (FIJO EN 0.30)
    # ================================================================
    #
    # OBJETIVO
    # ----------------------------------------------------------------
    # Identificar dos regímenes naturales de elasticidad mediante Gaussian
    # Mixture Model (GMM) y hacer la clasificación comercial más exigente:
    #
    #   HIGH = SKU con alta probabilidad de pertenecer al régimen
    #   más negativo
    #   LOW  = resto  # noqa: ERA001
    #
    # CAMBIO respecto a la version anterior: el umbral ya NO se busca
    # automaticamente (esa busqueda solo probaba 0.50-0.90, y el corte real
    # que se decidio -- 0.30 -- quedaba fuera de ese rango). Ahora queda
    # FIJO en 0.30, decidido con la evidencia real de este banner (ver
    # tabla de sensibilidad extendida abajo).
    #
    # IMPORTANTE
    # ----------------------------------------------------------------
    # - NO modifica elasticidad_final.
    # - NO modifica los modelos.
    # - NO consulta GCP.
    # - El GMM NO conoce ni utiliza un porcentaje objetivo.
    #
    # ================================================================

    # ================================================================
    # 0. LIMPIAR SEGMENTACIÓN ANTERIOR
    # ================================================================
    if 'segmento_elasticidad' in roster.columns:
        roster = roster.drop(columns=['segmento_elasticidad'])
    # ================================================================
    # 1. ROSTER VÁLIDO
    # ================================================================
    roster_valido = roster[roster['elasticidad_final'].notna()].copy()
    x_elasticidad = roster_valido[['elasticidad_final']].to_numpy()
    logger.info('=' * 90)
    logger.info('SEGMENTACIÓN DE ELASTICIDAD — GMM (umbral fijo 0.30)')
    logger.info('=' * 90)
    logger.info(f'\nElasticidades válidas : {len(roster_valido):,}')
    # ================================================================
    # 2. GMM — 2 COMPONENTES
    # ================================================================
    gmm_segmento = GaussianMixture(
        n_components=2, covariance_type='full', n_init=10, random_state=RANDOM_STATE
    )
    gmm_segmento.fit(x_elasticidad)
    # ================================================================
    # 3. IDENTIFICAR LOS DOS REGÍMENES
    # ================================================================
    medias_gmm = gmm_segmento.means_.flatten()
    pesos_gmm = gmm_segmento.weights_.flatten()
    desv_gmm = np.sqrt(gmm_segmento.covariances_.flatten())
    componente_high = int(np.argmin(medias_gmm))  # mas negativo = mayor sensibilidad
    componente_low = int(np.argmax(medias_gmm))  # mas cercano a 0 = menor sensibilidad
    # ================================================================
    # 4. PROBABILIDAD DE PERTENENCIA A HIGH
    # ================================================================
    probabilidades = gmm_segmento.predict_proba(x_elasticidad)
    prob_high = probabilidades[:, componente_high]
    df_sensibilidad_high = pd.DataFrame(
        {
            'material_ean': roster_valido['material_ean'].to_numpy(),
            'elasticidad_final': roster_valido['elasticidad_final'].to_numpy(),
            'prob_high': prob_high,
        }
    )
    # ================================================================
    # 5. INFORMACIÓN DE LOS COMPONENTES
    # ================================================================
    logger.info(f"\n{'-' * 90}")
    logger.info('REGÍMENES ESTIMADOS POR GMM')
    logger.info('-' * 90)
    logger.info('\nHIGH')
    logger.info(f'  Media elasticidad : {medias_gmm[componente_high]:.3f}')
    logger.info(f'  Desv. estándar    : {desv_gmm[componente_high]:.3f}')
    logger.info(f'  Peso GMM          : {pesos_gmm[componente_high] * 100:.2f}%')
    logger.info('\nLOW')
    logger.info(f'  Media elasticidad : {medias_gmm[componente_low]:.3f}')
    logger.info(f'  Desv. estándar    : {desv_gmm[componente_low]:.3f}')
    logger.info(f'  Peso GMM          : {pesos_gmm[componente_low] * 100:.2f}%')
    # ================================================================
    # 6. DISTRIBUCIÓN DE PROBABILIDADES
    # ================================================================
    logger.info(f"\n{'-' * 90}")
    logger.info('DISTRIBUCIÓN DE P(HIGH)')
    logger.info('-' * 90)
    logger.info(
        df_sensibilidad_high['prob_high']
        .describe(percentiles=[0.10, 0.25, 0.50, 0.60, 0.70, 0.75, 0.80, 0.90, 0.95])
        .round(3)
        .to_string()
    )
    # ================================================================
    # 7. SENSIBILIDAD DEL UMBRAL -- rango extendido (incluye el 0.30
    #    elegido)
    # ================================================================
    umbrales = [
        0.25,
        0.30,
        0.35,
        0.40,
        0.45,
        0.50,
        0.55,
        0.60,
        0.65,
        0.70,
        0.75,
        0.80,
        0.85,
        0.90,
    ]
    resultados_threshold = []
    for threshold in umbrales:
        high = df_sensibilidad_high['prob_high'] >= threshold
        n_high = int(high.sum())
        n_total = len(high)
        n_low = n_total - n_high
        resultados_threshold.append(
            {
                'prob_min_high': threshold,
                'n_low': n_low,
                'pct_low': n_low / n_total * 100,
                'n_high': n_high,
                'pct_high': n_high / n_total * 100,
            }
        )
    resultado_threshold = pd.DataFrame(resultados_threshold)
    logger.info(f"\n{'=' * 90}")
    logger.info('SENSIBILIDAD — EXIGENCIA PARA CLASIFICAR HIGH (rango completo)')
    logger.info('=' * 90)
    logger.info(resultado_threshold.round(2).to_string(index=False))
    # ================================================================
    # 8. SKU AMBIGUOS
    # ================================================================
    logger.info(f"\n{'-' * 90}")
    logger.info('SKU AMBIGUOS')
    logger.info('-' * 90)
    rangos_ambiguos = [(0.20, 0.40), (0.25, 0.35), (0.35, 0.65)]
    for limite_inf, limite_sup in rangos_ambiguos:
        mask = (df_sensibilidad_high['prob_high'] >= limite_inf) & (
            df_sensibilidad_high['prob_high'] <= limite_sup
        )
        n_ambiguos = int(mask.sum())
        logger.info(
            f'P(HIGH) entre {limite_inf:.2f} y {limite_sup:.2f}: {n_ambiguos:,} SKU '
            f'({n_ambiguos / len(df_sensibilidad_high) * 100:.2f}%)'
        )
    # ================================================================
    # 9. APLICAR EL UMBRAL FIJO -- 0.30
    # ================================================================
    # Decidido con la evidencia real de este banner (tabla de sensibilidad
    # arriba): 0.30 da 36.01% HIGH, el mas cercano a la referencia de
    # negocio de 35% dentro del rango explorado. Para cambiarlo, editar
    # directo este valor.
    # ================================================================
    df_sensibilidad_high['segmento_elasticidad'] = np.where(
        df_sensibilidad_high['prob_high'] >= UMBRAL_HIGH, 'high', 'low'
    )
    # ================================================================
    # 10. MERGE FINAL
    # ================================================================
    roster = roster.merge(
        df_sensibilidad_high[['material_ean', 'segmento_elasticidad']],
        on='material_ean',
        how='left',
    )
    # ================================================================
    # 11. DISTRIBUCIÓN FINAL
    # ================================================================
    dist_final = roster['segmento_elasticidad'].value_counts().reindex(['low', 'high'])
    dist_final_df = pd.DataFrame(
        {'n_sku': dist_final, 'pct': (dist_final / dist_final.sum() * 100)}
    )
    # ================================================================
    # 12. RESUMEN EJECUTIVO
    # ================================================================
    logger.info(f"\n{'=' * 90}")
    logger.info('SEGMENTACIÓN FINAL — GMM + PROBABILIDAD (umbral fijo)')
    logger.info('=' * 90)
    logger.info(f'\nUmbral utilizado P(HIGH) : {UMBRAL_HIGH:.2f}')
    logger.info(f'LOW  : {dist_final_df.loc["low", "pct"]:.2f}%')
    logger.info(f'HIGH : {dist_final_df.loc["high", "pct"]:.2f}%')
    logger.info('\nRegla estadística:')
    logger.info(f'  P(HIGH) >= {UMBRAL_HIGH:.2f} → HIGH')
    logger.info(f'  P(HIGH) <  {UMBRAL_HIGH:.2f} → LOW')
    logger.info('\nElasticidad_final NO fue modificada.')
    logger.info(
        'Umbral fijado manualmente en 0.30, con base en la evidencia real '
        'de este banner.'
    )
    logger.info('=' * 90)

    # ---- celda_27 ----

    # %% [construir_tabla_slim]
    # ================================================================
    # Construye la tabla final "slim" que se usa TANTO para el CSV local
    # como para la carga a BigQuery -- 1 sola definicion, sin duplicar
    # logica entre los 2 destinos.
    #
    # Pesos del score de confiabilidad (ajustados): 30 precision
    # estadistica + 30 calidad predictiva + 30 volumen de evidencia +
    # 10 sentido economico = 100. Categorias: baja [0,40), media [40,70),
    # alta [70,100].
    # ================================================================
    def calcular_score_confiabilidad(fila: pd.Series, gate_wape: float = 5.0) -> float:
        ic_width = fila.get('ic_width', np.nan)
        ic_crosses_zero = fila.get('ic_crosses_zero', True)
        elasticidad_final = fila.get('elasticidad_final', np.nan)
        # RECALIBRADO: precision estadistica ahora 15 (antes 30) -- con IC
        # SELECTIVO, la mayoria del catalogo nunca tiene IC calculado (no
        # por mala calidad, sino porque el pre-filtro de velocidad no lo
        # amerito) -- pesarlo igual que antes penalizaba de mas a la
        # mayoria del catalogo por una razon que no refleja su
        # calidad real.
        if bool(ic_crosses_zero) or pd.isna(ic_width) or pd.isna(elasticidad_final):
            pts_precision = 0.0
        else:
            rel_ic_width = ic_width / max(abs(elasticidad_final), 0.10)
            pts_precision = 15 * (1 - min(rel_ic_width, 1.0))
        # RECALIBRADO: calidad predictiva ahora 40 (antes 30) -- mas peso
        # a lo que SI se calcula siempre, para todos los candidatos
        wape = fila.get('wape', np.nan)
        r2 = fila.get('r2', np.nan)
        pts_wape = 20 * (1 - min(wape / gate_wape, 1.0)) if pd.notna(wape) else 0.0
        pts_r2 = 20 * max(min(r2, 1.0), 0.0) if pd.notna(r2) else 0.0
        pts_monotonic = (
            10.0
            if pd.notna(fila.get('price_response_monotonic'))
            and bool(fila.get('price_response_monotonic'))
            else 0.0
        )
        n_dias = fila.get('n_dias_evidencia', 0)
        n_dias = 0 if pd.isna(n_dias) else n_dias
        pts_n_dias = 35 * min(
            n_dias / 200, 1.0
        )  # 30 -> 35, absorbe parte de lo que bajo precision
        return round(pts_precision + pts_wape + pts_r2 + pts_monotonic + pts_n_dias, 1)

    def categorizar_confiabilidad(score: float) -> str:
        # RECALIBRADO: umbrales bajados -- alta >=55 (antes 70),
        # media >=30 (antes 40). Punto de partida razonado, no definitivo
        # -- revisar la distribucion real despues de correr y ajustar si
        # sigue quedando desbalanceado.
        if score >= 55:
            return 'alta'
        if score >= 30:
            return 'media'
        return 'baja'

    roster['score_confiabilidad'] = roster.apply(calcular_score_confiabilidad, axis=1)
    roster['confiabilidad'] = roster['score_confiabilidad'].apply(
        categorizar_confiabilidad
    )
    logger.info('Distribucion de confiabilidad:')
    logger.info(roster['confiabilidad'].value_counts())
    logger.info('\nScore de confiabilidad -- estadisticas:')
    logger.info(roster['score_confiabilidad'].describe().round(1))
    # ================================================================
    # Esquema final -- SOLO estas columnas van al CSV y a BigQuery.
    #   - UMV: ahora viene de 'umv' (SALES_UOM de BASELINE_PANEL, traida en
    #     el PASO 1C y propagada via cols_categoria en la cascada) -- ya
    #     NO se deja en NaN a la fuerza.
    #   - CONFIABILIDAD: categorica (alta/media/baja) agregada de vuelta.
    #   - CLUSTER: tipo_cluster (limpio/ciclos_rapidos/etc) agregada.
    # ================================================================
    # Redondeo a 2 decimales -- SOLO para el valor final reportado. La
    # segmentacion GMM de arriba ya uso la precision completa, esto no
    # la afecta -- es puramente cosmetico para el dato que se entrega.
    roster['elasticidad_final'] = roster['elasticidad_final'].round(2)

    roster['STORE_BANNER'] = store_banner
    roster['ZONA'] = zona
    roster['N_Eventos'] = roster['n_dias_evidencia']
    roster_slim = roster.rename(columns=MAPEO_COLUMNAS_SLIM)
    faltantes = [c for c in COLUMNAS_FINALES_ORDENADAS if c not in roster_slim.columns]
    if faltantes:
        logger.info(
            f'\nADVERTENCIA -- columnas esperadas que no se encontraron: {faltantes}'
        )
    roster_slim = roster_slim[
        [c for c in COLUMNAS_FINALES_ORDENADAS if c in roster_slim.columns]
    ]
    logger.info(
        f'\nTabla slim final ({store_banner}): {len(roster_slim):,} filas, '
        f'{len(roster_slim.columns)} columnas'
    )
    logger.info(roster_slim.head(5).to_string(index=False))

    # REGION: Tabla final -- esquema slim ya construido arriba en
    # roster_slim (STORE_BANNER, CATEGORIA, MATERIAL, DESCRIPCION_MATERIAL,
    # EAN, UMV, CLUSTER, ORIGEN, ELASTICIDAD, SEGMENTO_ELASTICIDAD,
    # N_Eventos, METODO, SCORE_CONFIABILIDAD, CONFIABILIDAD)
    # ----------------------------------------------------------------
    logger.info(f'Tabla final: {roster_slim.shape}')
    # ENDREGION

    # REGION: Carga a BigQuery -- incremental por banner (borra solo las
    # filas de este store_banner, despues agrega -- no pisa otros bancos
    # ya cargados en la misma tabla)
    # ----------------------------------------------------------------
    where_clause = f"STORE_BANNER = '{store_banner}' AND ZONA = '{zona}'"

    deleteFromTable(
        table_ref=f'{proyecto}.{esquema}.{tabla}',
        where_clause=where_clause,
        gbq_client=gbq_client,
    )

    uploadFrame(
        roster_slim,
        table_ddl_json_path=os.path.join('gbq_objects', 'ingest_elasticity_zona.json'),
        project=proyecto,
        gbq_client=gbq_client,
        if_exists='append',
    )

    logger.info(
        f'Se sube la tabla a GCP: {proyecto}.{esquema}.{tabla} '
        f'({len(roster_slim):,} filas)'
    )
    # ENDREGION


if __name__ == '__main__':
    main()
