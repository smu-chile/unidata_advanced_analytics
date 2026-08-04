# Default
from __future__ import annotations

import os
import logging
import argparse
from logging import config

# Pip
import numpy as np
import pandas as pd
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
# Niveles de baseline considerados confiables (excluye sin_baseline, que
# no tiene cantidad_esperada_baseline usable)
NIVELES_CONFIABLES = ['blend']

# Intensidades promocionales (para clasificar Promo->Promo). Los valores
# DEBEN calzar exactamente con la columna 'estado' real de BASELINE_PANEL.
ORDEN_INTENSIDAD = [
    '0_10', '10_15', '15_20', '20_25',
    '25_30', '30_40', '40_mas',
]
RANGO_INTENSIDAD = {intensidad: i for i, intensidad in enumerate(ORDEN_INTENSIDAD)}

# Reglas de soporte: deteccion de segmentos y transiciones
UMBRAL_CAMBIO_PRECIO_PCT = 3.0
MIN_DIAS_SEGMENTO = 3
MAX_DIAS_HUECO = 5
VENTANA_DIAS_TRANSICION = 7
MIN_EVENTOS_CONFIANZA_MEDIA = 5
MIN_EVENTOS_CONFIANZA_ALTA = 10

# Partial pooling (empirical Bayes) sobre elasticidad/lift
K_SHRINKAGE_TRANSICION = 10

# Prior de 2 niveles (subcategoria primero, categoria como respaldo)
MIN_SKU_PRIOR_SUBCATEGORIA = 5

# Contagio de 3 niveles: sustituto mas cercano antes de subcategoria
# /categoria
MIN_EVENTOS_TOTAL_SUSTITUTO_CONFIABLE = 5

# Cambio de precio minimo para considerar una transicion MEDIBLE
MIN_PCT_CAMBIO_PRECIO_TRANSICION = 5.0

# Honestidad a NIVEL MATERIAL: si mas de este % de las transiciones
# PROPIAS de un material tienen signo economicamente incorrecto
# (elasticidad positiva), el material se EXCLUYE por completo.
UMBRAL_PCT_SIGNO_RARO_MATERIAL = 50.0
MIN_TRANSICIONES_PARA_EVALUAR_MATERIAL = 3

# Baseline "mal estimado" a nivel de SEGMENTO Regular completo
UMBRAL_INDICE_SEGMENTO_REGULAR_ALTO = 5.0

# Segundo criterio: ratio vs. nivel tipico del propio SKU
UMBRAL_RATIO_ESPERADO_BAJO = 0.2

# Tercer criterio: cambio estructural (descontinuacion / lanzamiento)
MIN_DIAS_PARA_EVALUAR_CAMBIO_ESTRUCTURAL = 90
UMBRAL_RATIO_CAMBIO_ESTRUCTURAL_BAJO = 0.2
UMBRAL_RATIO_CAMBIO_ESTRUCTURAL_ALTO = 5.0

# Cuarto criterio: duracion maxima de un segmento medible
MAX_DIAS_SEGMENTO_PARA_MEDIR = 120

# Quinto criterio: correccion local en el tiempo (deriva gradual)
VENTANA_CORRECCION_LOCAL_DIAS = 90
MIN_DIAS_REGULARES_PARA_CORRECCION_LOCAL = 15

TIPOS_TRANSICION_FIJOS = [
    'Regular_a_Regular', 'Regular_a_Promocional',
    'Promocional_a_Regular', 'Promocional_a_Promocional',
]

RANGO_ELASTICIDAD_MIN, RANGO_ELASTICIDAD_MAX = -5.0, 0.0

# Ecommerce tiene su PROPIA tabla productiva de regresion.
MAPA_BANNER_REGRESSION_ECOMMERCE = {
    'Ecommerce Unimarc': 'Unimarc',
    'Ecommerce Alvi': 'Alvi',
}


# -------------------------------------------------------------------------
#  SQL Queries
# -------------------------------------------------------------------------
SQL_QUERIES = QueryDict({  # Region: Explicacion de query

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
        cantidad_total,
        cantidad_esperada_baseline,
        indice_venta,
        nivel_baseline
    FROM `${table}`
    WHERE store_banner = '${store_banner}'
    ORDER BY material, p_date
    """,

    # Se trae variacion_top1_sustituto (para el flag de contaminacion) y
    # ean_sustituto_1 (identidad del sustituto mas cercano, para el
    # contagio de elasticidad en cascada).
    'query_sustituto_contexto':
    """
    SELECT
        material,
        p_date,
        variacion_top1_sustituto,
        ean_sustituto_1
    FROM `${table}`
    WHERE store_banner = '${store_banner}'
    ORDER BY material, p_date
    """,
})


# -------------------------------------------------------------------------
#  Deteccion de cambio estructural
# -------------------------------------------------------------------------
def detectar_cambio_estructural(
        df_panel: pd.DataFrame,
        min_dias: int = MIN_DIAS_PARA_EVALUAR_CAMBIO_ESTRUCTURAL) -> pd.DataFrame:
    """Compara el nivel de venta del primer tercio vs. el ultimo tercio
    del historial de cada material (usando TODOS los dias, Regular y
    Promocional, ya que se busca el nivel real de actividad del
    producto, no solo su comportamiento sin oferta).
    """
    resultados = []
    for material, df_material in df_panel.groupby('material'):
        df_material = df_material.sort_values('p_date')
        n = len(df_material)
        if n < min_dias:
            continue
        tercio = n // 3
        nivel_temprano = df_material['cantidad_total'].iloc[:tercio].median()
        nivel_tardio = df_material['cantidad_total'].iloc[-tercio:].median()
        if nivel_temprano <= 0:
            continue
        resultados.append({
            'material': material, 'nivel_temprano': nivel_temprano,
            'nivel_tardio': nivel_tardio, 'ratio_cambio': nivel_tardio / nivel_temprano,
        })
    return pd.DataFrame(resultados)


# -------------------------------------------------------------------------
#  Correccion local en el tiempo
# -------------------------------------------------------------------------
def calcular_correccion_local(
        df_panel: pd.DataFrame,
        ventana_dias: int = VENTANA_CORRECCION_LOCAL_DIAS,
        min_dias_regulares: int = MIN_DIAS_REGULARES_PARA_CORRECCION_LOCAL,
        ) -> pd.DataFrame:
    """Para cada material y fecha, calcula un factor de correccion local
    comparando la venta REAL de dias Regulares cercanos (ventana +-N
    dias) contra lo que el baseline YA predijo para esos mismos dias.
    """
    resultados = []
    for material, df_material in df_panel.groupby('material'):
        df_material = (
            df_material.sort_values('p_date')
            .drop_duplicates(subset=['p_date'], keep='last')
            .set_index('p_date')
        )
        rango_fechas = pd.date_range(
            df_material.index.min(), df_material.index.max(), freq='D')

        es_regular = df_material['estado'] == 'Regular'
        obs_regular = df_material['cantidad_total'].where(
            es_regular).reindex(rango_fechas)
        esp_regular = df_material['cantidad_esperada_baseline'].where(
            es_regular).reindex(rango_fechas)

        ventana = ventana_dias * 2 + 1
        obs_local = obs_regular.rolling(
            ventana, center=True, min_periods=min_dias_regulares).median()
        esp_local = esp_regular.rolling(
            ventana, center=True, min_periods=min_dias_regulares).median()

        factor_local = (obs_local / esp_local).reindex(df_material.index)
        factor_local = factor_local.fillna(1.0)

        resultados.append(pd.DataFrame({
            'material': material, 'p_date': df_material.index,
            'factor_correccion_local': factor_local.to_numpy(),
        }))

    return pd.concat(resultados, ignore_index=True)


# -------------------------------------------------------------------------
#  Deteccion de segmentos y clasificacion de transiciones
# -------------------------------------------------------------------------
def agregar_segmentos_vectorizado(df_panel: pd.DataFrame) -> pd.DataFrame:
    """Calcula 'segmento_id' para TODOS los materiales de una sola pasada,
    usando operaciones vectorizadas (shift/cumsum por grupo).

    Un segmento nuevo empieza cuando: cambia el 'estado', o (estando en
    'Regular' tanto hoy como ayer) el precio cambia >= UMBRAL_CAMBIO_
    PRECIO_PCT respecto al DIA ANTERIOR, o hay un hueco de datos >
    MAX_DIAS_HUECO.
    """
    df = df_panel.sort_values(['material', 'p_date']).copy()  # noqa: PD901

    precio_prev = df.groupby('material')['precio_promedio'].shift(1)
    estado_prev = df.groupby('material')['estado'].shift(1)
    fecha_prev = df.groupby('material')['p_date'].shift(1)

    es_primero_del_material = estado_prev.isna()
    gap_dias = (df['p_date'] - fecha_prev).dt.days
    cambia_estado = df['estado'] != estado_prev
    ambos_regular = (df['estado'] == 'Regular') & (estado_prev == 'Regular')
    cambio_pct = (
        (df['precio_promedio'] - precio_prev).abs()
        / precio_prev.replace(0, np.nan) * 100
    )
    cambia_precio_regular = (
        ambos_regular & (cambio_pct >= UMBRAL_CAMBIO_PRECIO_PCT)
    ).fillna(value=False)
    hueco_grande = gap_dias > MAX_DIAS_HUECO

    nuevo_segmento = (
        es_primero_del_material | cambia_estado | cambia_precio_regular | hueco_grande
    )

    df['segmento_id'] = nuevo_segmento.groupby(df['material']).cumsum() - 1

    return df


def _resumen_segmentos(df_material_con_segmento: pd.DataFrame) -> pd.DataFrame:
    """Arma el resumen por segmento (uno por fila) para UN material que
    ya tiene la columna 'segmento_id' calculada.
    """
    resumen = (
        df_material_con_segmento.groupby('segmento_id')
        .agg(
            estado=('estado', 'first'),
            fecha_inicio=('p_date', 'min'),
            fecha_fin=('p_date', 'max'),
            dias=('p_date', 'count'),
            precio_promedio=('precio_promedio', 'mean'),
            indice_venta_promedio=('indice_venta', 'mean'),
        )
        .reset_index()
    )
    resumen['material'] = df_material_con_segmento['material'].iloc[0]

    return resumen


def _clasificar_transicion(estado_antes: str, estado_despues: str) -> str:
    antes_regular = estado_antes == 'Regular'
    despues_regular = estado_despues == 'Regular'
    if antes_regular and despues_regular:
        return 'Regular_a_Regular'
    if antes_regular and not despues_regular:
        return 'Regular_a_Promocional'
    if not antes_regular and despues_regular:
        return 'Promocional_a_Regular'
    return 'Promocional_a_Promocional'


def _direccion_promo_a_promo(estado_antes: str, estado_despues: str,
                            precio_antes: float, precio_despues: float) -> str:
    rango_antes = RANGO_INTENSIDAD.get(estado_antes)
    rango_despues = RANGO_INTENSIDAD.get(estado_despues)
    if (rango_antes is not None and rango_despues is not None
            and rango_antes != rango_despues):
        return 'Profundiza' if rango_despues > rango_antes else 'Alivia'
    if precio_despues < precio_antes:
        return 'Profundiza'
    if precio_despues > precio_antes:
        return 'Alivia'
    return 'Sin_cambio_neto'


def _ventana_promedio_indice(df_material: pd.DataFrame, segmento_id: int,
                            lado: str, dias: int) -> tuple[float, float, float]:
    """Precio, indice de venta y cantidad_esperada_baseline promedio de
    los primeros/ultimos `dias` dias de un segmento (capado por el largo
    real del segmento).
    """
    sub = df_material[df_material['segmento_id'] == segmento_id].sort_values('p_date')
    sub = sub.head(dias) if lado == 'inicio' else sub.tail(dias)
    return (sub['precio_promedio'].mean(), sub['indice_venta'].mean(),
            sub['cantidad_esperada_baseline'].mean())


# -------------------------------------------------------------------------
#  Calculo de transiciones controladas (indice_venta antes/despues)
# -------------------------------------------------------------------------
def calcular_transiciones_material(df_material: pd.DataFrame,
                                    segmentos_regular_mal_estimados: set,
                                    nivel_tipico_material: pd.Series) -> pd.DataFrame:
    """Calcula transiciones entre segmentos consecutivos, usando el indice
    de venta (ya controlado por el baseline) en vez de la cantidad cruda.
    Asume que df_material YA tiene 'segmento_id' (calculado una sola vez
    para todo el panel via agregar_segmentos_vectorizado).
    """
    segmentos = _resumen_segmentos(df_material)

    filas = []
    for i in range(len(segmentos) - 1):
        seg_antes = segmentos.iloc[i]
        seg_despues = segmentos.iloc[i + 1]

        if (seg_antes['dias'] < MIN_DIAS_SEGMENTO
                or seg_despues['dias'] < MIN_DIAS_SEGMENTO):
            continue

        # Descarta la transicion si alguno de los dos segmentos supera
        # la duracion maxima medible.
        if (seg_antes['dias'] > MAX_DIAS_SEGMENTO_PARA_MEDIR
                or seg_despues['dias'] > MAX_DIAS_SEGMENTO_PARA_MEDIR):
            continue

        # Descarta la transicion si alguno de sus dos segmentos es
        # 'Regular' Y esta marcado como mal estimado.
        material_actual = seg_antes['material']
        seg_antes_key = (material_actual, seg_antes['segmento_id'])
        seg_despues_key = (material_actual, seg_despues['segmento_id'])
        if (seg_antes['estado'] == 'Regular'
                and seg_antes_key in segmentos_regular_mal_estimados):
            continue
        if (seg_despues['estado'] == 'Regular'
                and seg_despues_key in segmentos_regular_mal_estimados):
            continue

        gap_dias = (seg_despues['fecha_inicio'] - seg_antes['fecha_fin']).days
        if gap_dias > MAX_DIAS_HUECO:
            continue

        precio_antes, indice_antes, esperada_antes = _ventana_promedio_indice(
            df_material, seg_antes['segmento_id'], 'fin', VENTANA_DIAS_TRANSICION)
        precio_despues, indice_despues, esperada_despues = _ventana_promedio_indice(
            df_material, seg_despues['segmento_id'], 'inicio', VENTANA_DIAS_TRANSICION)

        if (precio_antes <= 0 or pd.isna(indice_antes)
                or pd.isna(indice_despues) or indice_antes <= 0):
            continue

        # Segundo criterio de calidad: ratio vs. nivel tipico del propio
        # SKU.
        nivel_tipico = nivel_tipico_material.get(seg_antes['material'], np.nan)
        if pd.notna(nivel_tipico) and nivel_tipico > 0:
            ratio_antes_bajo = (
                pd.notna(esperada_antes)
                and esperada_antes / nivel_tipico < UMBRAL_RATIO_ESPERADO_BAJO
            )
            if ratio_antes_bajo:
                continue
            ratio_despues_bajo = (
                pd.notna(esperada_despues)
                and esperada_despues / nivel_tipico < UMBRAL_RATIO_ESPERADO_BAJO
            )
            if ratio_despues_bajo:
                continue

        pct_cambio_precio = (precio_despues - precio_antes) / precio_antes * 100
        pct_cambio_indice = (indice_despues - indice_antes) / indice_antes * 100

        if abs(pct_cambio_precio) < MIN_PCT_CAMBIO_PRECIO_TRANSICION:
            continue

        elasticidad_controlada = pct_cambio_indice / pct_cambio_precio

        # Elasticidad de ARCO (Marshall/Allen): usa el PUNTO MEDIO entre
        # antes/despues como base del % de cambio, en vez del punto
        # inicial -- se reporta AMBAS, nunca se reemplaza una por la otra.
        pct_precio_arco = (
            (precio_despues - precio_antes)
            / ((precio_despues + precio_antes) / 2) * 100
        )
        pct_indice_arco = (
            (indice_despues - indice_antes)
            / ((indice_despues + indice_antes) / 2) * 100
        )
        elasticidad_arco = (
            pct_indice_arco / pct_precio_arco if pct_precio_arco != 0 else np.nan
        )

        tipo_transicion = _clasificar_transicion(
            seg_antes['estado'], seg_despues['estado'])
        direccion_promo_promo = None
        if tipo_transicion == 'Promocional_a_Promocional':
            direccion_promo_promo = _direccion_promo_a_promo(
                seg_antes['estado'], seg_despues['estado'],
                precio_antes, precio_despues)

        filas.append({
            'material': seg_antes['material'],
            'fecha_transicion': seg_despues['fecha_inicio'],
            'estado_antes': seg_antes['estado'],
            'estado_despues': seg_despues['estado'],
            'tipo_transicion': tipo_transicion,
            'direccion_promo_a_promo': direccion_promo_promo,
            'precio_antes': round(precio_antes, 1),
            'precio_despues': round(precio_despues, 1),
            'pct_cambio_precio': round(pct_cambio_precio, 2),
            'indice_venta_antes': round(indice_antes, 3),
            'indice_venta_despues': round(indice_despues, 3),
            'pct_cambio_indice_controlado': round(pct_cambio_indice, 2),
            'elasticidad_controlada': round(elasticidad_controlada, 3),
            'elasticidad_arco': (
                round(elasticidad_arco, 3) if pd.notna(elasticidad_arco) else np.nan
            ),
            'dias_segmento_antes': int(seg_antes['dias']),
            'dias_segmento_despues': int(seg_despues['dias']),
        })

    return pd.DataFrame(filas)


def _calcular_confianza(n_eventos: int) -> str:
    if n_eventos >= MIN_EVENTOS_CONFIANZA_ALTA:
        return 'Alta'
    if n_eventos >= MIN_EVENTOS_CONFIANZA_MEDIA:
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
    usuario = 'elasticidad_transiciones'
    esquema = 'PRECIO_PROMOCIONES'
    tabla = 'ELASTICITY_TRANSITIONS'
    dataset_regression = 'TMP'

    if store_banner in MAPA_BANNER_REGRESSION_ECOMMERCE:
        table_regression = (
            f'{proyecto}.{dataset_regression}.'
            'TMP_ECOMMERCE_REGRESSION_PROCESSED_DATA_ELASTICITY'
        )
        store_banner_regression = MAPA_BANNER_REGRESSION_ECOMMERCE[store_banner]
    else:
        table_regression = (
            f'{proyecto}.{dataset_regression}.TMP_REGRESSION_PROCESSED_DATA_ELASTICITY'
        )
        store_banner_regression = store_banner

    table_baseline_panel = f'{proyecto}.{esquema}.BASELINE_PANEL'
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

    query_sustituto = SQL_QUERIES['query_sustituto_contexto'].substitute(
        table=table_regression, store_banner=store_banner_regression)
    df_sustituto = readBigQuery(
        query=query_sustituto, user=usuario, gbq_client=gbq_client)
    # ENDREGION

    # REGION: Preparacion del panel: tipos, merge de sustituto,
    # filtro de confiabilidad
    # -------------------------------------------------------------------
    df_panel['material'] = df_panel['material'].astype(str)
    df_panel['p_date'] = pd.to_datetime(df_panel['p_date'])

    # Deduplicar por (material, p_date): BASELINE_PANEL puede tener filas
    # repetidas -- sin esto, operaciones que usan p_date como indice
    # unico (correccion local) fallan.
    n_antes_dedup = len(df_panel)
    df_panel = df_panel.drop_duplicates(subset=['material', 'p_date'], keep='last')
    n_despues_dedup = len(df_panel)
    if n_antes_dedup != n_despues_dedup:
        n_dup = n_antes_dedup - n_despues_dedup
        logging.info(f'Se removieron {n_dup:,} filas duplicadas '
                    f'(material, p_date) de BASELINE_PANEL.')

    df_panel['precio_promedio'] = df_panel['precio_promedio'].astype(float)
    df_panel['cantidad_total'] = df_panel['cantidad_total'].astype(float)
    df_panel['cantidad_esperada_baseline'] = (
        df_panel['cantidad_esperada_baseline'].astype(float))
    df_panel['indice_venta'] = df_panel['indice_venta'].astype(float)

    df_sustituto['material'] = df_sustituto['material'].astype(str)
    df_sustituto['p_date'] = pd.to_datetime(df_sustituto['p_date'])
    df_sustituto = df_sustituto.drop_duplicates(subset=['material', 'p_date'])

    df_panel = df_panel.merge(df_sustituto, on=['material', 'p_date'], how='left')

    df_panel_confiable = df_panel[
        df_panel['nivel_baseline'].isin(NIVELES_CONFIABLES)].copy()
    df_panel_confiable = df_panel_confiable.sort_values(
        ['material', 'p_date']).reset_index(drop=True)

    n_confiables = df_panel_confiable['material'].nunique()
    n_total = df_panel['material'].nunique()
    logging.info(f'SKU con baseline confiable (usados en Transiciones): '
                f'{n_confiables:,} de {n_total:,}')
    # ENDREGION

    # REGION: Mapeo material_sustituto_1 (identidad, para el contagio
    # de 3 niveles)
    # -------------------------------------------------------------------
    lookup_ean_material = (
        df_panel[['ean', 'material']].dropna().drop_duplicates(subset=['ean'])
        .set_index('ean')['material']
    )

    ean_sustituto_por_material = (
        df_panel_confiable.dropna(subset=['ean_sustituto_1'])
        .groupby('material')['ean_sustituto_1']
        .agg(lambda s: s.mode().iloc[0] if not s.mode().empty else np.nan)
    )

    material_sustituto_1 = ean_sustituto_por_material.map(lookup_ean_material)
    n_con_sustituto = material_sustituto_1.notna().sum()
    n_material_total = len(material_sustituto_1)
    logging.info(f'Materiales con un sustituto identificado y mapeado a otro material: '
                f'{n_con_sustituto:,} de {n_material_total:,}')
    # ENDREGION

    # REGION: Detector de cambio estructural
    # -------------------------------------------------------------------
    df_cambio_estructural = detectar_cambio_estructural(df_panel_confiable)

    n_evaluados = len(df_cambio_estructural)
    logging.info(f'Materiales evaluados por cambio estructural: {n_evaluados:,}')

    ratio_bajo = (
        df_cambio_estructural['ratio_cambio'] < UMBRAL_RATIO_CAMBIO_ESTRUCTURAL_BAJO)
    ratio_alto = (
        df_cambio_estructural['ratio_cambio'] > UMBRAL_RATIO_CAMBIO_ESTRUCTURAL_ALTO)
    materiales_cambio_estructural = set(
        df_cambio_estructural.loc[ratio_bajo | ratio_alto, 'material']
    ) if not df_cambio_estructural.empty else set()

    n_excluidos_estructural = len(materiales_cambio_estructural)
    logging.info(
        f'Materiales excluidos por cambio estructural fuerte: '
        f'{n_excluidos_estructural:,}')

    df_panel_confiable = df_panel_confiable[
        ~df_panel_confiable['material'].isin(materiales_cambio_estructural)
    ].copy()
    # ENDREGION

    # REGION: Correccion local en el tiempo
    # -------------------------------------------------------------------
    df_correccion_local = calcular_correccion_local(df_panel_confiable)

    df_panel_confiable = df_panel_confiable.merge(
        df_correccion_local, on=['material', 'p_date'], how='left')
    df_panel_confiable['factor_correccion_local'] = (
        df_panel_confiable['factor_correccion_local'].fillna(1.0))

    # Se sobrescriben cantidad_esperada_baseline / indice_venta con la
    # version corregida.
    df_panel_confiable['cantidad_esperada_baseline'] = (
        df_panel_confiable['cantidad_esperada_baseline']
        * df_panel_confiable['factor_correccion_local']
    )
    df_panel_confiable['indice_venta'] = (
        df_panel_confiable['cantidad_total']
        / df_panel_confiable['cantidad_esperada_baseline']
    )
    # ENDREGION

    # REGION: Calculo de segmentos (una sola vez,
    # para todo el panel confiable)
    # -------------------------------------------------------------------
    df_panel_confiable = agregar_segmentos_vectorizado(df_panel_confiable)

    n_segmentos = df_panel_confiable.groupby('material')['segmento_id'].nunique().sum()
    logging.info(f'Total de segmentos detectados: {n_segmentos:,}')
    # ENDREGION

    # REGION: mediana del indice por SEGMENTO REGULAR completo
    # -------------------------------------------------------------------
    segmentos_regulares_completos = (
        df_panel_confiable[df_panel_confiable['estado'] == 'Regular']
        .groupby(['material', 'segmento_id'])
        .agg(
            mediana_indice_segmento=('indice_venta', 'median'),
            dias_segmento=('indice_venta', 'count'),
        )
        .reset_index()
    )

    segmentos_regular_mal_estimados = set(
        segmentos_regulares_completos.loc[
            segmentos_regulares_completos['mediana_indice_segmento']
            > UMBRAL_INDICE_SEGMENTO_REGULAR_ALTO,
            ['material', 'segmento_id'],
        ].itertuples(index=False, name=None)
    )

    n_materiales_afectados = len({m for m, _ in segmentos_regular_mal_estimados})
    n_segmentos_mal_estimados = len(segmentos_regular_mal_estimados)
    logging.info(f'Con umbral={UMBRAL_INDICE_SEGMENTO_REGULAR_ALTO}: '
                f'{n_segmentos_mal_estimados:,} segmentos marcados como mal estimados '
                f'({n_materiales_afectados:,} materiales distintos afectados)')
    # ENDREGION

    # REGION: nivel tipico de cantidad_esperada_baseline por material
    # -------------------------------------------------------------------
    nivel_tipico_material = (
        df_panel_confiable[df_panel_confiable['estado'] == 'Regular']
        .groupby('material')['cantidad_esperada_baseline']
        .median()
    )
    # ENDREGION

    # REGION: Calculo de transiciones controladas
    # (indice_venta antes/despues)
    # -------------------------------------------------------------------
    resultados_transiciones = []

    for _material, df_material in df_panel_confiable.groupby('material'):
        df_trans = calcular_transiciones_material(
            df_material, segmentos_regular_mal_estimados, nivel_tipico_material)
        if not df_trans.empty:
            resultados_transiciones.append(df_trans)

    df_transiciones = (
        pd.concat(resultados_transiciones, ignore_index=True)
        if resultados_transiciones else pd.DataFrame()
    )

    logging.info(f'Transiciones detectadas (detalle): {len(df_transiciones):,}')
    # ENDREGION

    # REGION: Honestidad a nivel material
    # -------------------------------------------------------------------
    if not df_transiciones.empty:
        df_transiciones['signo_raro'] = df_transiciones['elasticidad_controlada'] > 0

        calidad_por_material = df_transiciones.groupby('material').agg(
            n_transiciones=('elasticidad_controlada', 'count'),
            pct_signo_raro=('signo_raro', 'mean'),
        )
        calidad_por_material['pct_signo_raro'] *= 100

        n_transiciones = calidad_por_material['n_transiciones']
        pct_raro = calidad_por_material['pct_signo_raro']
        n_transiciones_min = n_transiciones >= MIN_TRANSICIONES_PARA_EVALUAR_MATERIAL
        pct_signo_alto = pct_raro > UMBRAL_PCT_SIGNO_RARO_MATERIAL
        materiales_no_confiables = calidad_por_material[
            n_transiciones_min & pct_signo_alto]

        n_no_confiables = len(materiales_no_confiables)
        logging.info(f'Materiales EXCLUIDOS por mayoria de signo raro '
                    f'(>{UMBRAL_PCT_SIGNO_RARO_MATERIAL}%): {n_no_confiables:,}')

        df_transiciones = df_transiciones[
            ~df_transiciones['material'].isin(materiales_no_confiables.index)
        ].drop(columns=['signo_raro']).copy()

        n_transiciones_finales = len(df_transiciones)
        logging.info(
            f'Transiciones que quedan tras el filtro: {n_transiciones_finales:,}')
    # ENDREGION

    # REGION: Agregacion SKU x tipo_transicion -- cobertura completa
    # -------------------------------------------------------------------
    info_material = (
        df_panel_confiable[
            ['material', 'ean', 'product_description',
            'category_description', 'sub_category_description', 'sales_uom']]
        .drop_duplicates(subset=['material'])
    )
    df_transiciones = df_transiciones.merge(info_material, on='material', how='left')

    # --- Cross-join: TODOS los SKU confiables x los 4 tipos de transicion
    tipos_transicion_df = pd.DataFrame(
        {'tipo_transicion': TIPOS_TRANSICION_FIJOS, '_key': 1})
    agregado_transiciones = (
        info_material.assign(_key=1)
        .merge(tipos_transicion_df, on='_key')
        .drop(columns='_key')
    )

    # --- Estadisticas PROPIAS ---
    stats_propias = (
        df_transiciones
        .groupby(['material', 'tipo_transicion'])
        .agg(
            n_eventos=('elasticidad_controlada', 'count'),
            elasticidad_mediana=('elasticidad_controlada', 'median'),
            elasticidad_p25=('elasticidad_controlada', lambda s: s.quantile(0.25)),
            elasticidad_p75=('elasticidad_controlada', lambda s: s.quantile(0.75)),
            elasticidad_arco_mediana=('elasticidad_arco', 'median'),
            elasticidad_arco_p25=('elasticidad_arco', lambda s: s.quantile(0.25)),
            elasticidad_arco_p75=('elasticidad_arco', lambda s: s.quantile(0.75)),
            pct_cambio_precio_promedio=('pct_cambio_precio', 'mean'),
            pct_cambio_indice_promedio=('pct_cambio_indice_controlado', 'mean'),
            fecha_primer_evento=('fecha_transicion', 'min'),
            fecha_ultimo_evento=('fecha_transicion', 'max'),
        )
        .reset_index()
    )

    agregado_transiciones = agregado_transiciones.merge(
        stats_propias, on=['material', 'tipo_transicion'], how='left'
    )
    agregado_transiciones['n_eventos'] = (
        agregado_transiciones['n_eventos'].fillna(0).astype(int))
    agregado_transiciones['confianza'] = (
        agregado_transiciones['n_eventos'].apply(_calcular_confianza))
    agregado_transiciones['store_banner'] = store_banner

    # --- Priors de 2 niveles (subcategoria / categoria) ---
    prior_subcat_transicion = (
        df_transiciones.groupby(['sub_category_description', 'tipo_transicion'])
        .agg(
            elasticidad_arco_subcat_prior=('elasticidad_arco', 'median'),
            n_sku_subcat_prior=('material', 'nunique'),
        )
        .reset_index()
    )

    prior_categoria_transicion = (
        df_transiciones
        .groupby(['category_description', 'tipo_transicion'])['elasticidad_arco']
        .median()
        .rename('elasticidad_arco_categoria_prior')
        .reset_index()
    )

    agregado_transiciones = agregado_transiciones.merge(
        prior_subcat_transicion,
        on=['sub_category_description', 'tipo_transicion'], how='left'
    )
    agregado_transiciones = agregado_transiciones.merge(
        prior_categoria_transicion,
        on=['category_description', 'tipo_transicion'], how='left'
    )

    # --- Nivel 1 de la cascada: el SUSTITUTO MAS CERCANO del propio SKU
    agregado_transiciones['material_sustituto_1'] = (
        agregado_transiciones['material'].map(material_sustituto_1)
    )

    lookup_propio_por_tipo = (
        agregado_transiciones.set_index(['material', 'tipo_transicion'])
        [['elasticidad_arco_mediana', 'n_eventos']]
        .rename(columns={'elasticidad_arco_mediana': 'elasticidad_arco_sustituto_prior',
                        'n_eventos': 'n_eventos_sustituto'})
    )

    agregado_transiciones = agregado_transiciones.merge(
        lookup_propio_por_tipo,
        left_on=['material_sustituto_1', 'tipo_transicion'],
        right_index=True, how='left',
    )

    # Confiabilidad GENERAL del sustituto: evidencia TOTAL sumando los 4
    # tipos de transicion.
    total_eventos_por_material = (
        agregado_transiciones.groupby('material')['n_eventos'].transform('sum'))
    agregado_transiciones['total_eventos_material'] = total_eventos_por_material
    total_eventos_por_material_unico = (
        agregado_transiciones.drop_duplicates('material')
        .set_index('material')['total_eventos_material']
    )
    agregado_transiciones['total_eventos_sustituto'] = (
        agregado_transiciones['material_sustituto_1']
        .map(total_eventos_por_material_unico)
    )

    sustituto_confiable_general = (
        agregado_transiciones['material_sustituto_1'].notna()
        & (agregado_transiciones['total_eventos_sustituto']
        >= MIN_EVENTOS_TOTAL_SUSTITUTO_CONFIABLE)
    )
    sustituto_tiene_dato_especifico = agregado_transiciones['n_eventos_sustituto'] > 0

    sustituto_calificado = sustituto_confiable_general & sustituto_tiene_dato_especifico
    subcat_calificada = (
        agregado_transiciones['n_sku_subcat_prior'] >= MIN_SKU_PRIOR_SUBCATEGORIA)

    agregado_transiciones['elasticidad_arco_prior_final'] = np.select(
        [sustituto_calificado, subcat_calificada],
        [agregado_transiciones['elasticidad_arco_sustituto_prior'],
        agregado_transiciones['elasticidad_arco_subcat_prior']],
        default=agregado_transiciones['elasticidad_arco_categoria_prior'],
    )

    # --- Columna EXPLICITA de origen (nunca se mezcla en silencio) ---
    tiene_evidencia_propia = agregado_transiciones['n_eventos'] > 0
    tiene_categoria_prior = agregado_transiciones[
        'elasticidad_arco_categoria_prior'].notna()
    agregado_transiciones['origen_elasticidad'] = np.select(
        [tiene_evidencia_propia, sustituto_calificado, subcat_calificada,
        tiene_categoria_prior],
        ['propia', 'heredada_sustituto', 'heredada_subcategoria', 'heredada_categoria'],
        default='sin_evidencia',
    )

    agregado_transiciones['peso_propio_transicion'] = (
        agregado_transiciones['n_eventos']
        / (agregado_transiciones['n_eventos'] + K_SHRINKAGE_TRANSICION)
    )

    elasticidad_arco_segura = (
        agregado_transiciones['elasticidad_arco_mediana'].fillna(0))

    agregado_transiciones['elasticidad_arco_final'] = np.where(
        agregado_transiciones['elasticidad_arco_prior_final'].notna(),
        (agregado_transiciones['peso_propio_transicion'] * elasticidad_arco_segura
        + (1 - agregado_transiciones['peso_propio_transicion'])
        * agregado_transiciones['elasticidad_arco_prior_final']),
        np.where(agregado_transiciones['n_eventos'] > 0,
                agregado_transiciones['elasticidad_arco_mediana'], np.nan),
    )

    logging.info('Distribucion de origen de la elasticidad (cobertura completa):')
    logging.info(agregado_transiciones['origen_elasticidad'].value_counts().to_dict())
    # ENDREGION

    # REGION: Acotar elasticidad_arco_final al rango [-5, 0]
    # -------------------------------------------------------------------
    agregado_transiciones['elasticidad_arco_final_sin_acotar'] = (
        agregado_transiciones['elasticidad_arco_final'])
    agregado_transiciones['elasticidad_arco_final'] = agregado_transiciones[
        'elasticidad_arco_final'].clip(
        lower=RANGO_ELASTICIDAD_MIN, upper=RANGO_ELASTICIDAD_MAX
    )

    sin_acotar = agregado_transiciones['elasticidad_arco_final_sin_acotar']
    con_acotar = agregado_transiciones['elasticidad_arco_final']
    n_acotados = (sin_acotar != con_acotar).sum()
    logging.info(
        f'Filas acotadas al rango [{RANGO_ELASTICIDAD_MIN}, {RANGO_ELASTICIDAD_MAX}]: '
        f'{n_acotados:,} de {len(agregado_transiciones):,}')
    # ENDREGION

    # REGION: Carga a BigQuery
    # -------------------------------------------------------------------
    # Tabla reducida: descripcion/granularidad del material + origen +
    # valor final de elasticidad (ya acotado a [-5,0]) + evidencia.
    columnas_salida_transiciones = [
        'material', 'ean', 'product_description', 'category_description',
        'sub_category_description', 'sales_uom', 'tipo_transicion', 'store_banner',
        'origen_elasticidad', 'elasticidad_arco_final', 'n_eventos',
    ]
    df_gcp = agregado_transiciones[columnas_salida_transiciones].copy()

    where_clause = f"store_banner = '{store_banner}'"

    deleteFromTable(table_ref=f'{proyecto}.{esquema}.{tabla}',
                    where_clause=where_clause,
                    gbq_client=gbq_client)

    uploadFrame(
        df_gcp,
        table_ddl_json_path=os.path.join(
            'gbq_objects', 'ingest_elasticity_transitions.json'),
        project=proyecto,
        gbq_client=gbq_client,
        if_exists='append'
    )

    logging.info(
        f'Se sube la tabla a GCP: {proyecto}.{esquema}.{tabla} ({len(df_gcp):,} filas)')
    # ENDREGION


if __name__ == '__main__':
    main()
