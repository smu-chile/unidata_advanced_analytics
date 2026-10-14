# Default
"""Elasticidad cruzada de EAN seleccionados (sustitutos/complementarios).

Para un store_banner: (1) selecciona los EAN focales representativos
(Pareto, dias activos y variacion de precio), (2) toma su top 10 de
sustitutos y de complementarios del modelo de SKU (pasado a EAN),
(3) estima la elasticidad cruzada con un log-log y errores HAC,
(4) clasifica la confiabilidad (FDR, robustez, estabilidad) y (5) sube
a PRECIO_PROMOCIONES.ELASTICITY_CROSS solo los pares cuyo signo coincide
con el del modelo de SKU. Sin cascada ni imputacion.
"""
from __future__ import annotations  # noqa: I001

import os
import time
import logging
import argparse
import warnings
from logging import config
from dataclasses import dataclass

# Pip
import numpy as np  # noqa: I001
import pandas as pd
import statsmodels.api as sm
from joblib import Parallel, delayed
from statsmodels.stats.multitest import multipletests
from google.cloud.bigquery import Client, DatasetReference

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
    '--project_id', type=str,
    help='GCP project in which the script will be executed'
)
parser.add_argument(
    '--execution_date', type=str,
    help='DAG execution date (YYYY-MM-DD): ultimo dia de datos a usar'
)
parser.add_argument(
    '--store_banner', type=str,
    help='Store banner'
)
parser.add_argument(
    '--categorias', type=str, default='',
    help='Categorias focales separadas por coma (vacio = todas)'
)

# -------------------------------------------------------------------------
#  Parametros: seleccion de EAN focales
# -------------------------------------------------------------------------
SEL_VENTANA_MESES = 12  # ventana de venta del Pareto (hasta la ultima fecha)
SEL_PARETO = 0.80  # venta acumulada por (categoria, subcategoria)
SEL_MIN_DIAS_ACTIVOS = 60  # dias con venta del EAN focal
# desviacion minima de ln(precio) del focal, sin tendencia lineal
SEL_MIN_STD_SIN_TENDENCIA = 0.02
SEL_MIN_EAN_POR_SUBCATEGORIA = 1
SEL_MAX_EAN_POR_SUBCATEGORIA = 10  # None = sin tope

# -------------------------------------------------------------------------
# -------------------------------------------------------------------------
CANDIDATOS_POR_FOCAL = 10  # top del modelo de SKU por tipo de relacion
MAX_RELACIONADOS = 10  # maximo a estimar tras filtrar los no viables
MIN_DIAS_RELACIONADO = 60  # dias con venta del relacionado
MIN_STD_PRECIO_RELACIONADO = 0.01  # desviacion minima de ln P del relacionado

# -------------------------------------------------------------------------
# -------------------------------------------------------------------------
# 'otras_categorias' | 'resto_subcategoria' | 'ninguno'
CONTROL_DEMANDA = 'otras_categorias'
CONTROLES = 'reducido'  # 'reducido' o 'completo'
INCLUIR_PROMO = True
INCLUIR_APO = False
FECHA_DESDE = None  # 'YYYY-MM-DD' o None (toda la historia)
LAGS_HAC = 7
MIN_OBS = 60
MIN_OBS_AJUSTE = 30
MIN_OBS_BLOQUE = 40
MIN_STD_PRECIO = 0.01  # desviacion minima de ln P del focal
P_VALOR_UMBRAL = 0.05
Q_UMBRAL = 0.05  # umbral del q-value (FDR)
ELASTICIDAD_MAX = 5.0
VIF_MAX = 10.0
P_BLOQUE_SEVERA = 0.10
VENTANA_FERIADO_DIAS = 3
ROBUSTEZ_SOLO_SIGNIFICATIVOS = True

# Evento = racha de dias con el precio del focal al menos
# EVENTO_CAIDA_PRECIO bajo su mediana movil, o en promocion.
EVENTO_CAIDA_PRECIO = 0.03
EVENTO_VENTANA_DIAS = 28
MIN_EVENTOS_MUY_ALTA = 8
# 'completa': mismo signo y significativo con los 3 controles
# 'solo_signo': basta con el mismo signo con los 3 controles
ROBUSTEZ_MUY_ALTA = 'completa'

# -------------------------------------------------------------------------
#  Parametros: ejecucion y salida
# -------------------------------------------------------------------------
N_JOBS = 6  # procesos (loky); cada uno recibe solo los datos de su lote
LOTE_FOCALES = 20  # focales por lote de estimacion
TAMANO_LOTE_SQL = 2000  # EAN por consulta de descarga
NIVELES = ('MUY ALTA', 'ALTA', 'MEDIA', 'BAJA')
NIVELES_A_SUBIR = ('MUY ALTA', 'ALTA', 'MEDIA', 'BAJA')
DECIMALES_ELASTICIDAD = 3

SIGNO_ESPERADO = {'SUSTITUTO': 1.0, 'COMPLEMENTARIO': -1.0}
COLUMNA_CONTROL = {
    'otras_categorias': 'LN_V_OTRAS_CAT',
    'resto_subcategoria': 'LN_V_RESTO_SUBCAT',
    'ninguno': None,
}
VARIANTES_CONTROL = (
    ('otras_categorias', 'OTRAS_CAT'),
    ('resto_subcategoria', 'RESTO_SUBCAT'),
    ('ninguno', 'SIN_CONTROL'),
)
FERIADOS = pd.to_datetime([
    '2023-01-01', '2023-04-07', '2023-05-01', '2023-05-21', '2023-06-26',
    '2023-07-16', '2023-08-15', '2023-09-18', '2023-09-19', '2023-10-09',
    '2023-10-27', '2023-11-01', '2023-12-08', '2023-12-25',
    '2024-01-01', '2024-03-29', '2024-05-01', '2024-05-21', '2024-06-20',
    '2024-07-16', '2024-08-15', '2024-09-18', '2024-09-19', '2024-10-12',
    '2024-10-31', '2024-11-01', '2024-12-08', '2024-12-25',
    '2025-01-01', '2025-04-18', '2025-05-01', '2025-05-21', '2025-06-20',
    '2025-07-16', '2025-08-15', '2025-09-18', '2025-09-19', '2025-10-12',
    '2025-10-31', '2025-11-01', '2025-12-08', '2025-12-25',
    '2026-01-01', '2026-04-03', '2026-05-01', '2026-05-21', '2026-06-21',
    '2026-06-29', '2026-07-16', '2026-08-15', '2026-09-18', '2026-09-19',
    '2026-10-12', '2026-10-31', '2026-11-01', '2026-12-08', '2026-12-25',
])
COLUMNAS_MODELO = (
    'N_OBSERVATIONS', 'N_EVENTOS_FOCAL', 'STD_LN_P_A', 'STD_LN_P_B',
    'CORR_LN_PRECIO_A_B', 'PCT_DIAS_PROMO_A', 'PCT_DIAS_PROMO_B',
    'CORR_PROMO_A_B', 'ELASTICIDAD_CRUZADA', 'STD_ERROR', 'T_STAT', 'P_VALUE',
    'IC95_LOWER', 'IC95_UPPER', 'R2', 'VIF_MAXIMO', 'BETA_PERIODO_1',
    'P_PERIODO_1', 'BETA_PERIODO_2', 'P_PERIODO_2', 'ELAST_OTRAS_CAT',
    'P_OTRAS_CAT', 'ELAST_RESTO_SUBCAT', 'P_RESTO_SUBCAT', 'ELAST_SIN_CONTROL',
    'P_SIN_CONTROL',
)
COLUMNAS_FINALES_ORDENADAS = [
    'STORE_BANNER', 'MES_EJECUCION', 'CATEGORIA_FOCAL', 'SUBCATEGORIA_FOCAL', 'EAN_FOCAL',
    'PRODUCTO_FOCAL', 'SUB_CATEGORIA_RELACIONADO', 'EAN_RELACIONADO',
    'PRODUCTO_RELACIONADO', 'ELASTICIDAD_CRUZADA', 'CONFIABILIDAD',
]

# ====================================================================
#  Queries -- QueryDict (solo SELECT)
# ====================================================================
SQL_QUERIES = QueryDict({  # Region: Explicacion de query

    # Ultima fecha con datos del banner (hasta la fecha de ejecucion)
    'query_fecha_max':
    """
    SELECT MAX(p_date) AS fecha_max
    FROM `${table}`
    WHERE store_banner = '${store_banner}'
      AND p_date <= DATE('${fecha_hasta}')
    """,

    # Estadisticas por EAN para elegir los focales
    'query_estadisticas':
    """
    SELECT
      CAST(ean AS STRING) AS ean,
      category_description,
      sub_category_description,
      MAX(product_description) AS product_description,
      COUNT(DISTINCT p_date) AS dias_activos,
      SUM(IF(p_date > DATE('${corte}'), ventas_totales_producto, 0)) AS ventas_ventana,
      STDDEV_POP(LN(precio_promedio)) AS std_ln_precio,
      CORR(LN(precio_promedio), UNIX_DATE(p_date)) AS corr_ln_precio_tiempo
    FROM `${table}`
    WHERE store_banner = '${store_banner}'
      AND precio_promedio > 0
      AND p_date <= DATE('${fecha_hasta}')
    GROUP BY ean, category_description, sub_category_description
    """,

    # Top de sustitutos y complementarios del modelo de SKU, a nivel EAN
    'query_pares':
    """
    WITH MAPA AS (
      SELECT STORE_BANNER,
             SAFE_CAST(MATERIAL AS INT64) AS MATERIAL,
             CAST(EAN AS STRING) AS EAN
      FROM `${table}`
      WHERE STORE_BANNER = '${store_banner}'
      GROUP BY 1, 2, 3
    ),
    NOMBRE AS (
      SELECT CAST(EAN AS STRING) AS EAN, ANY_VALUE(PRODUCT_DESCRIPTION) AS PRODUCT_DESCRIPTION
      FROM `${table}`
      WHERE STORE_BANNER = '${store_banner}'
      GROUP BY 1
    ),
    REL AS (
      SELECT STORE_BANNER,
             SAFE_CAST(sku AS INT64) AS SKU_FOCAL,
             SAFE_CAST(substitute AS INT64) AS SKU_RELACIONADO,
             substitution_score AS SCORE,
             substitution_rank AS RANKING,
             'SUSTITUTO' AS TIPO_RELACION
      FROM `${table_sustitutos}`
      WHERE date = (SELECT MAX(date) FROM `${table_sustitutos}`
                    WHERE STORE_BANNER = '${store_banner}')
        AND STORE_BANNER = '${store_banner}'
      UNION ALL
      SELECT store_banner,
             SAFE_CAST(sku AS INT64),
             SAFE_CAST(complementary_sku AS INT64),
             score,
             relevance,
             'COMPLEMENTARIO'
      FROM `${table_complementarios}`
      WHERE date = (SELECT MAX(date) FROM `${table_complementarios}`
                    WHERE store_banner = '${store_banner}')
        AND store_banner = '${store_banner}'
    ),
    PARES AS (
      SELECT REL.TIPO_RELACION,
             F.EAN AS EAN_FOCAL,
             R.EAN AS EAN_RELACIONADO,
             MIN(REL.RANKING) AS RANKING,
             MAX(REL.SCORE)   AS SCORE
      FROM REL
      JOIN MAPA F ON F.MATERIAL = REL.SKU_FOCAL AND F.STORE_BANNER = REL.STORE_BANNER
      JOIN MAPA R ON R.MATERIAL = REL.SKU_RELACIONADO AND R.STORE_BANNER = REL.STORE_BANNER
      WHERE F.EAN <> R.EAN
        AND F.EAN IN (${focales})
      GROUP BY 1, 2, 3
    ),
    TOPN AS (
      SELECT *,
             ROW_NUMBER() OVER (PARTITION BY TIPO_RELACION, EAN_FOCAL
                                ORDER BY RANKING ASC, SCORE DESC) AS POSICION
      FROM PARES
    )
    SELECT T.TIPO_RELACION,
           T.EAN_FOCAL,
           NF.PRODUCT_DESCRIPTION AS PRODUCTO_FOCAL,
           T.POSICION,
           T.EAN_RELACIONADO,
           NR.PRODUCT_DESCRIPTION AS PRODUCTO_RELACIONADO,
           T.RANKING,
           T.SCORE
    FROM TOPN T
    LEFT JOIN NOMBRE NF ON NF.EAN = T.EAN_FOCAL
    LEFT JOIN NOMBRE NR ON NR.EAN = T.EAN_RELACIONADO
    WHERE T.POSICION <= ${candidatos}
    """,

    # Serie diaria de los EAN involucrados (con marca de promocion)
    'query_ean':
    """
    WITH promo AS (
      SELECT DISTINCT SAFE_CAST(material AS INT64) AS material, p_date
      FROM `${table_promo}`
      WHERE store_banner = '${store_banner}'
    )
    SELECT
      r.p_date,
      CAST(r.ean AS STRING) AS ean,
      r.material,
      r.category_description,
      r.sub_category_description,
      r.product_description,
      r.ventas_totales_producto,
      r.cantidad_total,
      r.precio_promedio,
      r.apo,
      IF(p.material IS NULL, 0, 1) AS en_promo
    FROM `${table}` r
    LEFT JOIN promo p
      ON SAFE_CAST(r.material AS INT64) = p.material AND r.p_date = p.p_date
    WHERE r.store_banner = '${store_banner}'
      AND CAST(r.ean AS STRING) IN (${eans})
      ${fechas}
    """,

    # Controles de demanda: venta diaria de banner, categoria, subcat.
    'query_venta_banner':
    """
    SELECT p_date, SUM(ventas_totales_producto) AS v_banner
    FROM `${table}`
    WHERE store_banner = '${store_banner}'
      ${fechas}
    GROUP BY p_date
    """,

    'query_venta_categoria':
    """
    SELECT category_description, p_date, SUM(ventas_totales_producto) AS v_categoria
    FROM `${table}`
    WHERE store_banner = '${store_banner}'
      AND category_description IN (${categorias})
      ${fechas}
    GROUP BY category_description, p_date
    """,

    'query_venta_subcategoria':
    """
    WITH promo AS (
      SELECT DISTINCT SAFE_CAST(material AS INT64) AS material, p_date
      FROM `${table_promo}`
      WHERE store_banner = '${store_banner}'
    )
    SELECT
      r.category_description,
      r.sub_category_description,
      r.p_date,
      SUM(r.ventas_totales_producto) AS v_sub,
      SUM(IF(p.material IS NULL, 0, r.ventas_totales_producto)) AS v_sub_promo
    FROM `${table}` r
    LEFT JOIN promo p
      ON SAFE_CAST(r.material AS INT64) = p.material AND r.p_date = p.p_date
    WHERE r.store_banner = '${store_banner}'
      AND CONCAT(r.category_description, '|', r.sub_category_description) IN (${pares})
      ${fechas}
    GROUP BY r.category_description, r.sub_category_description, r.p_date
    """,

})  # ENDREGION


@dataclass(frozen=True)
class Entorno:
    """Banner, fecha tope, categorias y tablas fuente de la corrida."""

    proyecto: str
    store_banner: str
    fecha_hasta: str
    categorias: tuple
    tabla_regresion: str
    tabla_promo: str
    tabla_sustitutos: str
    tabla_complementarios: str


# ====================================================================
#  Utilidades de descarga
# ====================================================================
def literal_sql(valores: list[str]) -> str:
    """Lista de textos como literal SQL, escapando comillas simples."""
    return ', '.join("'" + v.replace("'", "\\'") + "'" for v in valores)


def filtro_fechas(ent: Entorno, alias: str = '') -> str:
    """Condiciones AND sobre p_date segun FECHA_DESDE y la fecha tope."""
    prefijo = f'{alias}.' if alias else ''
    condiciones = [f"AND {prefijo}p_date <= DATE('{ent.fecha_hasta}')"]
    if FECHA_DESDE:
        condiciones.append(f"AND {prefijo}p_date >= DATE('{FECHA_DESDE}')")
    return '\n      '.join(condiciones)


def a_numerico(df: pd.DataFrame, columnas: list[str]) -> pd.DataFrame:
    """Convierte a float columnas que BigQuery entrega como Decimal."""
    for columna in columnas:
        df[columna] = pd.to_numeric(df[columna], errors='coerce').astype(float)
    return df


# ====================================================================
#  PASO 1: seleccion de EAN focales
# ====================================================================
CLAVE = ['category_description', 'sub_category_description']


def cargar_estadisticas(ent: Entorno, leer: object) -> pd.DataFrame:
    """Estadisticas por EAN (una fila por categoria, subcategoria, EAN)."""
    consulta = SQL_QUERIES['query_fecha_max'].substitute(
        table=ent.tabla_regresion, store_banner=ent.store_banner,
        fecha_hasta=ent.fecha_hasta)
    fecha_max = pd.to_datetime(leer(query=consulta)['fecha_max'].iloc[0])
    if pd.isna(fecha_max):
        msg = f'Sin datos en {ent.tabla_regresion} para {ent.store_banner}.'
        raise ValueError(msg)
    corte = fecha_max - pd.DateOffset(months=SEL_VENTANA_MESES)
    logger.info(
        f'Ultima fecha de datos: {fecha_max:%Y-%m-%d} - ventana de venta: '
        f'despues de {corte:%Y-%m-%d} ({SEL_VENTANA_MESES} meses)'
    )
    consulta = SQL_QUERIES['query_estadisticas'].substitute(
        table=ent.tabla_regresion, store_banner=ent.store_banner,
        fecha_hasta=ent.fecha_hasta, corte=f'{corte:%Y-%m-%d}')
    crudo = leer(query=consulta).rename(columns=str.lower)
    crudo = a_numerico(
        crudo,
        ['dias_activos', 'ventas_ventana', 'std_ln_precio', 'corr_ln_precio_tiempo'],
    )
    crudo['ean'] = crudo['ean'].astype(str)
    return crudo


def agregar_metricas(df: pd.DataFrame) -> pd.DataFrame:
    """Agrega desviacion sin tendencia, ranking y participacion."""
    df = df.copy()  # noqa: PD901
    corr = df['corr_ln_precio_tiempo'].fillna(0.0).clip(-1.0, 1.0)
    df['std_ln_sin_tendencia'] = df['std_ln_precio'] * np.sqrt(1.0 - corr**2)
    con_venta = df[df['ventas_ventana'] > 0].copy()
    con_venta = con_venta.sort_values(
        [*CLAVE, 'ventas_ventana', 'ean'], ascending=[True, True, False, True]
    )
    total = con_venta.groupby(CLAVE)['ventas_ventana'].transform('sum')
    con_venta['participacion'] = con_venta['ventas_ventana'] / total
    con_venta['participacion_acum'] = con_venta.groupby(CLAVE)['participacion'].cumsum()
    con_venta['ranking'] = con_venta.groupby(CLAVE).cumcount() + 1
    columnas = ['participacion', 'participacion_acum', 'ranking']
    return df.merge(con_venta[['ean', *CLAVE, *columnas]], on=['ean', *CLAVE], how='left')


def seleccionar(df: pd.DataFrame) -> pd.DataFrame:
    """Aplica Pareto, dias, precio y tope; agrega una marca por etapa."""
    df = df.copy()  # noqa: PD901
    antes = df['participacion_acum'] - df['participacion']
    df['pasa_pareto'] = df['participacion'].notna() & (
        (antes < SEL_PARETO) | (df['ranking'] == 1)
    )
    df['pasa_dias'] = df['pasa_pareto'] & (df['dias_activos'] >= SEL_MIN_DIAS_ACTIVOS)
    df['pasa_precio'] = df['pasa_dias'] & (
        df['std_ln_sin_tendencia'] >= SEL_MIN_STD_SIN_TENDENCIA
    )
    viables = df['pasa_precio']
    n_viables = viables.groupby([df[c] for c in CLAVE]).transform('sum')
    df['pasa_min_ean'] = viables & (n_viables >= SEL_MIN_EAN_POR_SUBCATEGORIA)
    orden = (
        df[df['pasa_min_ean']]
        .groupby(CLAVE)['ventas_ventana'].rank(method='first', ascending=False)
    )
    df['seleccionado'] = df['pasa_min_ean']
    if SEL_MAX_EAN_POR_SUBCATEGORIA is not None:
        df['seleccionado'] = df['pasa_min_ean'] & (
            orden.reindex(df.index) <= SEL_MAX_EAN_POR_SUBCATEGORIA
        )
    return df


def filtrar_categorias(df: pd.DataFrame, categorias: tuple) -> pd.DataFrame:
    """Deja las categorias que contienen alguno de los textos pedidos."""
    if not categorias:
        return df
    nombre = df['category_description'].astype(str).str.upper().str.strip()
    mascara = nombre.apply(lambda x: any(c in x for c in categorias))
    for categoria in categorias:
        if not nombre.str.contains(categoria, regex=False).any():
            logger.info(f'ATENCION: ninguna categoria coincide con "{categoria}".')
    return df[mascara]


def seleccionar_focales(ent: Entorno, leer: object) -> pd.DataFrame:
    """EAN focales del banner (tabla con descripcion y subcategoria)."""
    df = seleccionar(agregar_metricas(cargar_estadisticas(ent, leer)))  # noqa: PD901
    etapas = [
        ('EAN en la tabla', pd.Series(True, index=df.index)),  # noqa: FBT003
        ('Con venta en la ventana', df['ventas_ventana'] > 0),
        (f'Pareto {SEL_PARETO:.0%}', df['pasa_pareto']),
        (f'Dias activos >= {SEL_MIN_DIAS_ACTIVOS}', df['pasa_dias']),
        (f'Variacion de precio >= {SEL_MIN_STD_SIN_TENDENCIA}', df['pasa_precio']),
        (f'Tope {SEL_MAX_EAN_POR_SUBCATEGORIA} por subcategoria', df['seleccionado']),
    ]
    logger.info('Embudo de seleccion (EAN):')
    for nombre, mascara in etapas:
        logger.info(f'  {nombre}: {int(mascara.sum()):,}')
    sel = filtrar_categorias(df[df['seleccionado']], ent.categorias)
    if sel.empty:
        msg = f'Sin EAN focales seleccionados (categorias: {list(ent.categorias)}).'
        raise ValueError(msg)
    logger.info(f'EAN focales seleccionados: {len(sel):,}')
    return sel.drop_duplicates('ean').reset_index(drop=True)


# ====================================================================
#  PASO 2: pares focal -> relacionado y datos diarios
# ====================================================================
def cargar_pares(ent: Entorno, leer: object, focales: list[str]) -> pd.DataFrame:
    """Top de sustitutos y complementarios por EAN focal."""
    partes = []
    for inicio in range(0, len(focales), TAMANO_LOTE_SQL):
        consulta = SQL_QUERIES['query_pares'].substitute(
            table=ent.tabla_regresion, table_sustitutos=ent.tabla_sustitutos,
            table_complementarios=ent.tabla_complementarios,
            store_banner=ent.store_banner, fecha_hasta=ent.fecha_hasta,
            focales=literal_sql(focales[inicio:inicio + TAMANO_LOTE_SQL]),
            candidatos=CANDIDATOS_POR_FOCAL)
        partes.append(leer(query=consulta))
    pares = pd.concat(partes, ignore_index=True).rename(columns=str.upper)
    pares['EAN_FOCAL'] = pares['EAN_FOCAL'].astype(str)
    pares['EAN_RELACIONADO'] = pares['EAN_RELACIONADO'].astype(str)
    pares['TIPO_RELACION'] = pares['TIPO_RELACION'].astype(str).str.upper().str.strip()
    pares = a_numerico(pares, ['POSICION', 'RANKING', 'SCORE'])
    pares['POSICION'] = pares['POSICION'].astype(int)
    pares = pares.drop_duplicates(['TIPO_RELACION', 'EAN_FOCAL', 'EAN_RELACIONADO'])
    return pares.reset_index(drop=True)


def cargar_datos(ent: Entorno, leer: object, eans: list[str]) -> dict:
    """Datos diarios de los EAN involucrados y sus controles de demanda."""
    partes = []
    for inicio in range(0, len(eans), TAMANO_LOTE_SQL):
        consulta = SQL_QUERIES['query_ean'].substitute(
            table=ent.tabla_regresion, table_promo=ent.tabla_promo,
            store_banner=ent.store_banner, fechas=filtro_fechas(ent, 'r'),
            eans=literal_sql(eans[inicio:inicio + TAMANO_LOTE_SQL]))
        partes.append(leer(query=consulta))
    crudo = pd.concat(partes, ignore_index=True).rename(columns=str.lower)
    crudo['p_date'] = pd.to_datetime(crudo['p_date'])
    crudo['ean'] = crudo['ean'].astype(str)
    crudo = a_numerico(
        crudo,
        ['ventas_totales_producto', 'cantidad_total', 'precio_promedio', 'apo', 'en_promo'],
    ).sort_values('p_date')

    info = crudo.groupby('ean').agg(
        categoria=('category_description', 'last'),
        subcategoria=('sub_category_description', 'last'),
        descripcion=('product_description', 'last'),
    ).to_dict('index')
    crudo['_pq'] = crudo['precio_promedio'] * crudo['cantidad_total']
    dia = crudo.groupby(['ean', 'p_date']).agg(
        v=('ventas_totales_producto', 'sum'), q=('cantidad_total', 'sum'),
        pq=('_pq', 'sum'), promo=('en_promo', 'max'), apo=('apo', 'max'),
    )
    dia['p'] = dia['pq'] / dia['q']
    series = {
        ean: g.droplevel(0).drop(columns='pq') for ean, g in dia.groupby(level=0)
    }

    categorias = sorted({d['categoria'] for d in info.values()})
    pares_sub = sorted({f"{d['categoria']}|{d['subcategoria']}" for d in info.values()})
    v_banner = leer(query=SQL_QUERIES['query_venta_banner'].substitute(
        table=ent.tabla_regresion, store_banner=ent.store_banner,
        fechas=filtro_fechas(ent))).rename(columns=str.lower)
    v_cat = leer(query=SQL_QUERIES['query_venta_categoria'].substitute(
        table=ent.tabla_regresion, store_banner=ent.store_banner,
        categorias=literal_sql(categorias), fechas=filtro_fechas(ent))
    ).rename(columns=str.lower)
    v_sub = leer(query=SQL_QUERIES['query_venta_subcategoria'].substitute(
        table=ent.tabla_regresion, table_promo=ent.tabla_promo,
        store_banner=ent.store_banner, pares=literal_sql(pares_sub),
        fechas=filtro_fechas(ent, 'r'))).rename(columns=str.lower)
    for tabla in (v_banner, v_cat, v_sub):
        tabla['p_date'] = pd.to_datetime(tabla['p_date'])
    v_banner = a_numerico(v_banner, ['v_banner']).set_index('p_date')['v_banner']
    v_cat = a_numerico(v_cat, ['v_categoria'])
    v_sub = a_numerico(v_sub, ['v_sub', 'v_sub_promo'])
    return {
        'info': info,
        'series': series,
        'v_banner': v_banner,
        'v_cat': {c: g.set_index('p_date')['v_categoria'] for c, g in v_cat.groupby(
            'category_description')},
        'v_sub': {
            (c, s): g.set_index('p_date')[['v_sub', 'v_sub_promo']]
            for (c, s), g in v_sub.groupby(['category_description', 'sub_category_description'])
        },
    }


def marcar_viabilidad(pares: pd.DataFrame, env: dict) -> pd.DataFrame:
    """Agrega MOTIVO_NO_ESTIMADO ('' si el par se puede estimar)."""
    estadisticas = {}
    for ean, d in env['series'].items():
        precio = np.log(d['p'].where(d['p'] > 0))
        estadisticas[ean] = (len(d), precio.std())

    def motivo(fila: pd.Series) -> str:
        a, b = fila['EAN_FOCAL'], fila['EAN_RELACIONADO']
        if a not in env['series']:
            return 'FOCAL_SIN_DATOS'
        if b not in env['series']:
            return 'RELACIONADO_SIN_DATOS'
        dias, desv = estadisticas[b]
        if dias < MIN_DIAS_RELACIONADO:
            return 'RELACIONADO_POCOS_DIAS'
        if not desv >= MIN_STD_PRECIO_RELACIONADO:
            return 'RELACIONADO_POCA_VARIACION_PRECIO'
        info = env['info'][b]
        if (info['categoria'], info['subcategoria']) not in env['v_sub']:
            return 'SIN_VENTA_SUBCATEGORIA'
        if info['categoria'] not in env['v_cat']:
            return 'SIN_VENTA_CATEGORIA'
        return ''

    pares = pares.copy()
    pares['MOTIVO_NO_ESTIMADO'] = pares.apply(motivo, axis=1)
    viable = pares['MOTIVO_NO_ESTIMADO'] == ''
    orden = pares[viable].sort_values('POSICION').groupby(
        ['TIPO_RELACION', 'EAN_FOCAL']
    ).cumcount()
    fuera = orden[orden >= MAX_RELACIONADOS].index
    pares.loc[fuera, 'MOTIVO_NO_ESTIMADO'] = 'FUERA_DEL_TOP'
    return pares


# ====================================================================
#  PASO 3: modelo log-log por par
# ====================================================================
def construir_cerca_feriado(fechas: pd.DatetimeIndex) -> pd.Series:
    """1 si la fecha esta a menos de VENTANA_FERIADO_DIAS de un feriado."""
    origen = pd.Timestamp('1970-01-01')
    dias = (fechas - origen).days.to_numpy()
    dias_feriado = np.sort((FERIADOS - origen).days.to_numpy())
    derecha = np.searchsorted(dias_feriado, dias + VENTANA_FERIADO_DIAS, side='right')
    izquierda = np.searchsorted(dias_feriado, dias - VENTANA_FERIADO_DIAS, side='left')
    return pd.Series((derecha > izquierda).astype(float), index=fechas)


def armar_base(env: dict, a: str, b: str, control: str) -> pd.DataFrame:
    """Una fila por dia con las variables del modelo (focal A -> rel B)."""
    da, db = env['series'][a], env['series'][b]
    cat_b = env['info'][b]['categoria']
    sub_b = env['info'][b]['subcategoria']
    misma_sub = (env['info'][a]['categoria'], env['info'][a]['subcategoria']) == (cat_b, sub_b)
    dias = da.index.intersection(db.index)
    base = pd.DataFrame(
        {
            'LN_Q_B': np.log(db['q'].where(db['q'] > 0)).reindex(dias),
            'LN_P_A': np.log(da['p'].where(da['p'] > 0)).reindex(dias),
            'LN_P_B': np.log(db['p'].where(db['p'] > 0)).reindex(dias),
        },
        index=dias,
    )
    sub = env['v_sub'][(cat_b, sub_b)].reindex(dias)
    v_resto = sub['v_sub'] - db['v'].reindex(dias)
    promo_resto = sub['v_sub_promo'] - db['v'].reindex(dias) * db['promo'].reindex(dias)
    if misma_sub:
        v_resto = v_resto - da['v'].reindex(dias)
        promo_resto = promo_resto - da['v'].reindex(dias) * da['promo'].reindex(dias)
    v_resto = v_resto.where(v_resto > 1e-6 * sub['v_sub'])
    if control == 'otras_categorias':
        v_otras = env['v_banner'].reindex(dias) - env['v_cat'][cat_b].reindex(dias)
        base['LN_V_OTRAS_CAT'] = np.log(v_otras.where(v_otras > 0))
    elif control == 'resto_subcategoria':
        base['LN_V_RESTO_SUBCAT'] = np.log(v_resto)
    if INCLUIR_PROMO:
        base['PROMO_A'] = da['promo'].reindex(dias)
        base['PROMO_B'] = db['promo'].reindex(dias)
        base['PROMO_RESTO'] = (promo_resto / v_resto).clip(0.0, 1.0)
    if INCLUIR_APO:
        base['APO_A'] = da['apo'].reindex(dias)
        base['APO_B'] = db['apo'].reindex(dias)
    base['CERCA_FERIADO'] = construir_cerca_feriado(dias)
    base = base.dropna().sort_index()
    base['TENDENCIA'] = (base.index - base.index.min()).days / 365.0
    if CONTROLES == 'completo':
        base['PRIMER_DIA_MES'] = base.index.is_month_start.astype(float)
        base['ULTIMO_DIA_MES'] = base.index.is_month_end.astype(float)
    return base


def contar_eventos(env: dict, a: str, dias: pd.DatetimeIndex) -> int:
    """Episodios (rachas de dias) de precio bajo o promocion del focal."""
    cache = env.setdefault('eventos', {})
    if a not in cache:
        d = env['series'][a]
        referencia = d['p'].rolling(
            f'{EVENTO_VENTANA_DIAS}D', min_periods=10, closed='left'
        ).median()
        cache[a] = (d['p'] <= referencia * (1 - EVENTO_CAIDA_PRECIO)) | (d['promo'] > 0)
    marca = cache[a].reindex(dias).fillna(False).astype(bool)  # noqa: FBT003
    previa = marca.shift(1, fill_value=False)
    corte = pd.Series(dias, index=dias).diff().dt.days.fillna(1) > 1
    return int((marca & (~previa | corte)).sum())


def calcular_vif(x: pd.DataFrame) -> dict:
    """VIF de cada columna de x contra todas las demas (con constante)."""
    z = x.to_numpy(dtype='float64')
    vif = {}
    for j, nombre in enumerate(x.columns):
        y = z[:, j]
        otras = np.column_stack([np.ones(len(z)), np.delete(z, j, axis=1)])
        coeficientes = np.linalg.lstsq(otras, y, rcond=None)[0]
        residuo = y - otras @ coeficientes
        total = ((y - y.mean()) ** 2).sum()
        r2 = 1 - (residuo @ residuo) / total if total > 0 else 1.0
        vif[nombre] = np.inf if r2 >= 1 - 1e-12 else 1 / (1 - r2)
    return vif


def ajustar_ols(base: pd.DataFrame) -> tuple | None:
    """OLS sin pesos con errores HAC (Newey-West, LAGS_HAC rezagos)."""
    y = base['LN_Q_B']
    x = base.drop(columns='LN_Q_B')
    if CONTROLES == 'completo':
        dias = pd.get_dummies(
            pd.Series(base.index.dayofweek.astype(str), index=base.index),
            prefix='DOW', drop_first=True, dtype='float64',
        )
        x = x.join(dias)
    x = x.loc[:, x.std() > 1e-8]
    if 'LN_P_A' not in x.columns or len(x) <= x.shape[1] + 5:
        return None
    exog = sm.add_constant(x, has_constant='add')
    ajuste = sm.OLS(y, exog).fit(
        cov_type='HAC', cov_kwds={'maxlags': LAGS_HAC, 'use_correction': True}
    )
    return ajuste, x


def coeficiente(ajuste: object, nombre: str) -> dict:
    """Coeficiente, error, t, p-value e IC95% de una variable."""
    if nombre not in ajuste.params.index:
        return dict.fromkeys(('coef', 'se', 't', 'p', 'low', 'up'), np.nan)
    intervalo = ajuste.conf_int().loc[nombre]
    return {
        'coef': float(ajuste.params[nombre]),
        'se': float(ajuste.bse[nombre]),
        't': float(ajuste.tvalues[nombre]),
        'p': float(ajuste.pvalues[nombre]),
        'low': float(intervalo.iloc[0]),
        'up': float(intervalo.iloc[1]),
    }


def beta_bloque(base_bloque: pd.DataFrame) -> tuple[float, float]:
    """Elasticidad cruzada y p-value en un bloque temporal."""
    if len(base_bloque) < MIN_OBS_BLOQUE:
        return np.nan, np.nan
    resultado = ajustar_ols(base_bloque)
    if resultado is None:
        return np.nan, np.nan
    cruzado = coeficiente(resultado[0], 'LN_P_A')
    return cruzado['coef'], cruzado['p']


def estimar_variantes(env: dict, a: str, b: str) -> dict:
    """Elasticidad cruzada con cada control de demanda (robustez)."""
    salida = {}
    for opcion, sufijo in VARIANTES_CONTROL:
        coef, p_valor = np.nan, np.nan
        base_v = armar_base(env, a, b, opcion)
        if len(base_v) >= MIN_OBS_AJUSTE:
            resultado = ajustar_ols(base_v)
            if resultado is not None:
                cruzado = coeficiente(resultado[0], 'LN_P_A')
                coef, p_valor = cruzado['coef'], cruzado['p']
        salida[f'ELAST_{sufijo}'] = coef
        salida[f'P_{sufijo}'] = p_valor
    return salida


def fila_vacia(env: dict, a: str, b: str) -> dict:
    """Fila de resultados del par con el modelo aun sin estimar."""
    ia, ib = env['info'][a], env['info'][b]
    fila = {
        'EAN_FOCAL': a,
        'EAN_RELACIONADO': b,
        'PRODUCTO_FOCAL': ia['descripcion'],
        'PRODUCTO_RELACIONADO': ib['descripcion'],
        'CATEGORIA_FOCAL': ia['categoria'],
        'SUBCATEGORIA_FOCAL': ia['subcategoria'],
        'CATEGORIA_RELACIONADO': ib['categoria'],
        'SUBCATEGORIA_RELACIONADO': ib['subcategoria'],
        'ERROR_ESTIMACION': '',
    }
    fila.update(dict.fromkeys(COLUMNAS_MODELO, np.nan))
    return fila


def estimar_par(env: dict, a: str, b: str) -> dict:
    """Estima focal A -> relacionado B y devuelve la fila de resultados."""
    base = armar_base(env, a, b, CONTROL_DEMANDA)
    fila = fila_vacia(env, a, b)
    hay = len(base) > 1
    fila.update(
        {
            'N_OBSERVATIONS': len(base),
            'N_EVENTOS_FOCAL': contar_eventos(env, a, base.index),
            'STD_LN_P_A': base['LN_P_A'].std() if hay else np.nan,
            'STD_LN_P_B': base['LN_P_B'].std() if hay else np.nan,
            'CORR_LN_PRECIO_A_B': base['LN_P_A'].corr(base['LN_P_B']) if hay else np.nan,
            'PCT_DIAS_PROMO_A': float(env['series'][a]['promo'].mean()),
            'PCT_DIAS_PROMO_B': float(env['series'][b]['promo'].mean()),
            'CORR_PROMO_A_B': base['PROMO_A'].corr(base['PROMO_B']) if (
                hay and 'PROMO_A' in base) else np.nan,
        }
    )
    if len(base) < MIN_OBS_AJUSTE:
        return fila
    resultado = ajustar_ols(base)
    if resultado is None:
        return fila
    ajuste, x = resultado
    cruzado = coeficiente(ajuste, 'LN_P_A')
    fila.update(
        {
            'ELASTICIDAD_CRUZADA': cruzado['coef'], 'STD_ERROR': cruzado['se'],
            'T_STAT': cruzado['t'], 'P_VALUE': cruzado['p'],
            'IC95_LOWER': cruzado['low'], 'IC95_UPPER': cruzado['up'],
            'R2': float(ajuste.rsquared),
            'VIF_MAXIMO': max(calcular_vif(x).values()),
        }
    )
    if cruzado['p'] < P_VALOR_UMBRAL or not ROBUSTEZ_SOLO_SIGNIFICATIVOS:
        mitad = len(base) // 2
        beta_1, p_1 = beta_bloque(base.iloc[:mitad])
        beta_2, p_2 = beta_bloque(base.iloc[mitad:])
        fila.update(
            {
                'BETA_PERIODO_1': beta_1, 'P_PERIODO_1': p_1,
                'BETA_PERIODO_2': beta_2, 'P_PERIODO_2': p_2,
                **estimar_variantes(env, a, b),
            }
        )
    return fila


# ====================================================================
#  Estimacion en paralelo (lotes de focales)
# ====================================================================
def subentorno(env: dict, unicos: pd.DataFrame) -> dict:
    """Recorte de los datos con solo lo que necesita un lote de pares."""
    eans = set(unicos['EAN_FOCAL']) | set(unicos['EAN_RELACIONADO'])
    subs = {
        (env['info'][b]['categoria'], env['info'][b]['subcategoria'])
        for b in unicos['EAN_RELACIONADO']
    }
    return {
        'info': {e: env['info'][e] for e in eans},
        'series': {e: env['series'][e] for e in eans},
        'v_banner': env['v_banner'],
        'v_cat': {c: env['v_cat'][c] for c, _ in subs},
        'v_sub': {k: env['v_sub'][k] for k in subs},
    }


def estimar_lote(env_lote: dict, pares: list[tuple[str, str]]) -> pd.DataFrame:
    """Estima una lista de pares (focal, relacionado)."""
    warnings.filterwarnings('ignore')
    filas = []
    for a, b in pares:
        try:
            filas.append(estimar_par(env_lote, a, b))
        except Exception as exc:  # noqa: BLE001
            fila = fila_vacia(env_lote, a, b)
            fila['ERROR_ESTIMACION'] = f'{type(exc).__name__}: {exc}'[:200]
            filas.append(fila)
    return pd.DataFrame(filas)


def lotes_de_pares(env: dict, unicos: pd.DataFrame):  # noqa: ANN201
    """Genera (entorno recortado, pares) por grupo de focales."""
    focales = sorted(unicos['EAN_FOCAL'].unique())
    for inicio in range(0, len(focales), LOTE_FOCALES):
        sub = unicos[unicos['EAN_FOCAL'].isin(focales[inicio:inicio + LOTE_FOCALES])]
        pares = sorted(zip(sub['EAN_FOCAL'], sub['EAN_RELACIONADO']))
        yield subentorno(env, sub), pares


def estimar_todo(env: dict, unicos: pd.DataFrame) -> pd.DataFrame:
    """Estima todos los pares unicos, por tandas de lotes en paralelo."""
    total = len(unicos)
    logger.info(f'Pares por estimar: {total:,} - procesos: {N_JOBS}')
    inicio = time.time()
    resultados = []
    generador = lotes_de_pares(env, unicos)
    while True:
        tanda = [lote for _, lote in zip(range(N_JOBS * 4), generador)]
        if not tanda:
            break
        resultados.extend(
            Parallel(n_jobs=N_JOBS)(delayed(estimar_lote)(e, p) for e, p in tanda)
        )
        listos = sum(len(r) for r in resultados)
        ritmo = (time.time() - inicio) / max(listos, 1)
        logger.info(
            f'  {listos:,}/{total:,} pares - {time.time() - inicio:.0f} s - '
            f'faltan ~{ritmo * (total - listos) / 60:.1f} min'
        )
    return pd.concat(resultados, ignore_index=True)


# ====================================================================
#  PASO 4: quality gate, FDR y confiabilidad
# ====================================================================
def clasificar(fila: dict) -> dict:
    """Quality gate y robustez de un par estimado."""
    beta = fila['ELASTICIDAD_CRUZADA']
    motivos = []
    if fila['ERROR_ESTIMACION']:
        motivos.append('ERROR')
    elif pd.isna(beta):
        motivos.append('SIN_ESTIMACION')
    else:
        if fila['N_OBSERVATIONS'] < MIN_OBS:
            motivos.append(f'N<{MIN_OBS}')
        if not fila['P_VALUE'] < P_VALOR_UMBRAL:
            motivos.append(f'P>={P_VALOR_UMBRAL}')
        if fila['IC95_LOWER'] <= 0 <= fila['IC95_UPPER']:
            motivos.append('IC_CRUZA_0')
        if abs(beta) > ELASTICIDAD_MAX:
            motivos.append('FUERA_DE_RANGO')
        if fila['VIF_MAXIMO'] >= VIF_MAX:
            motivos.append(f'VIF>={VIF_MAX:g}')
        if fila['STD_LN_P_A'] < MIN_STD_PRECIO:
            motivos.append('POCA_VARIACION_PRECIO')
        b1, b2 = fila['BETA_PERIODO_1'], fila['BETA_PERIODO_2']
        if (
            pd.notna(b1) and pd.notna(b2) and np.sign(b1) != np.sign(b2)
            and fila['P_PERIODO_1'] < P_BLOQUE_SEVERA and fila['P_PERIODO_2'] < P_BLOQUE_SEVERA
        ):
            motivos.append('INESTABILIDAD_SEVERA')
    fila['ESTADO_QUALITY_GATE'] = 'NO VÁLIDA' if motivos else 'VÁLIDA'
    fila['MOTIVO_NO_VALIDA'] = ', '.join(motivos)
    variantes = [(fila[f'ELAST_{s}'], fila[f'P_{s}']) for _, s in VARIANTES_CONTROL]
    variantes = [(c, p) for c, p in variantes if pd.notna(c)]
    if pd.isna(beta):
        fila['ROBUSTEZ'] = 'SIN DATOS'
    elif not variantes:
        fila['ROBUSTEZ'] = 'NO EVALUADA'
    elif any(np.sign(c) != np.sign(beta) for c, _ in variantes):
        fila['ROBUSTEZ'] = 'SIGNO CAMBIA'
    elif any(not p < P_VALOR_UMBRAL for _, p in variantes):
        fila['ROBUSTEZ'] = 'PIERDE SIGNIFICANCIA'
    else:
        fila['ROBUSTEZ'] = 'ROBUSTO'
    return fila


def aplicar_fdr(tabla: pd.DataFrame) -> pd.DataFrame:
    """Q-value (Benjamini-Hochberg) por tipo de relacion."""
    tabla['Q_VALUE'] = np.nan
    for _, indices in tabla.groupby('TIPO_RELACION').groups.items():  # noqa: PERF102
        p_valores = tabla.loc[indices, 'P_VALUE'].dropna()
        if len(p_valores):
            q_valores = multipletests(p_valores, method='fdr_bh')[1]
            tabla.loc[p_valores.index, 'Q_VALUE'] = q_valores
    return tabla


def confiabilidad(tabla: pd.DataFrame) -> pd.DataFrame:
    """Nivel de confiabilidad; sin el signo del modelo no se reporta."""
    signo = tabla['TIPO_RELACION'].map(SIGNO_ESPERADO)
    ajustada = tabla['ELASTICIDAD_CRUZADA'] * signo
    valida = tabla['ESTADO_QUALITY_GATE'] == 'VÁLIDA'
    firme = tabla['Q_VALUE'] < Q_UMBRAL
    estimado = tabla['ELASTICIDAD_CRUZADA'].notna()
    coherente = estimado & (ajustada > 0)
    en_rango = tabla['ELASTICIDAD_CRUZADA'].abs() <= ELASTICIDAD_MAX
    if ROBUSTEZ_MUY_ALTA == 'completa':
        robusta = tabla['ROBUSTEZ'] == 'ROBUSTO'
    else:
        robusta = tabla['ROBUSTEZ'].isin(['ROBUSTO', 'PIERDE SIGNIFICANCIA'])
    estable = (tabla['BETA_PERIODO_1'] * tabla['BETA_PERIODO_2']) > 0
    eventos = tabla['N_EVENTOS_FOCAL'] >= MIN_EVENTOS_MUY_ALTA
    reportable = coherente & en_rango
    muy_alta = reportable & valida & firme & robusta & estable & eventos
    alta = reportable & valida & firme & ~muy_alta
    media = reportable & valida & ~firme
    baja = reportable & ~valida
    tabla['CONFIABILIDAD'] = np.select(
        [muy_alta, alta, media, baja], list(NIVELES), default='NO REPORTAR'
    )
    return tabla


def armar_tabla(pares: pd.DataFrame, estimados: pd.DataFrame) -> pd.DataFrame:
    """Une pares y estimaciones, aplica FDR y la confiabilidad."""
    columnas = ['TIPO_RELACION', 'EAN_FOCAL', 'EAN_RELACIONADO', 'POSICION']
    tabla = pares[columnas].merge(
        estimados, on=['EAN_FOCAL', 'EAN_RELACIONADO'], how='left'
    )
    sin_estimar = tabla['ESTADO_QUALITY_GATE'].isna()
    tabla.loc[sin_estimar, 'ESTADO_QUALITY_GATE'] = 'NO ESTIMADO'
    tabla['ROBUSTEZ'] = tabla['ROBUSTEZ'].fillna('SIN DATOS')
    return confiabilidad(aplicar_fdr(tabla))


# ====================================================================
#  PASO 5: tabla final
# ====================================================================
def tabla_final(tabla: pd.DataFrame, store_banner: str, mes: str) -> pd.DataFrame:
    """Pares reportables con las columnas de la tabla productiva."""
    reporte = tabla[tabla['CONFIABILIDAD'].isin(NIVELES_A_SUBIR)].copy()
    reporte['_orden'] = reporte['CONFIABILIDAD'].map({n: i for i, n in enumerate(NIVELES)})
    reporte = reporte.sort_values(['_orden', 'POSICION'])
    reporte = reporte.drop_duplicates(['EAN_FOCAL', 'EAN_RELACIONADO'])
    reporte['ELASTICIDAD_CRUZADA'] = reporte['ELASTICIDAD_CRUZADA'].round(
        DECIMALES_ELASTICIDAD
    )
    reporte['STORE_BANNER'] = store_banner
    reporte['MES_EJECUCION'] = mes
    reporte = reporte.rename(
        columns={'SUBCATEGORIA_RELACIONADO': 'SUB_CATEGORIA_RELACIONADO'}
    )
    reporte = reporte.sort_values(
        ['CATEGORIA_FOCAL', 'SUBCATEGORIA_FOCAL', 'PRODUCTO_FOCAL', '_orden', 'POSICION']
    )
    return reporte[COLUMNAS_FINALES_ORDENADAS].reset_index(drop=True)


def validar_parametros() -> None:
    """Falla temprano si algun parametro no es valido."""
    if CONTROL_DEMANDA not in COLUMNA_CONTROL:
        msg = f'CONTROL_DEMANDA debe ser uno de {list(COLUMNA_CONTROL)}'
        raise ValueError(msg)
    if CONTROLES not in ('reducido', 'completo'):
        msg = "CONTROLES debe ser 'reducido' o 'completo'."
        raise ValueError(msg)
    if MAX_RELACIONADOS > CANDIDATOS_POR_FOCAL:
        msg = 'MAX_RELACIONADOS no puede superar CANDIDATOS_POR_FOCAL.'
        raise ValueError(msg)
    if set(NIVELES_A_SUBIR) - set(NIVELES):
        msg = f'NIVELES_A_SUBIR debe estar dentro de {NIVELES}'
        raise ValueError(msg)


def main() -> None:  # noqa: D103
    # Parse input variables
    args = vars(parser.parse_args())
    execution_date: str = args['execution_date']
    proyecto: str = args['project_id']
    store_banner: str = args['store_banner']
    categorias = tuple(
        c.strip().upper() for c in (args['categorias'] or '').split(',') if c.strip()
    )
    logger.info(f'execution_date: {execution_date}')
    logger.info(f'proyecto: {proyecto}')
    logger.info(f'store_banner: {store_banner}')
    logger.info(f'categorias: {list(categorias) or "todas"}')
    validar_parametros()
    mes_ejecucion = f'{pd.to_datetime(execution_date):%Y-%m}'
    logger.info(f'mes_ejecucion: {mes_ejecucion}')

    # Set gbq client for all subsequent queries
    gbq_client = Client()

    # REGION: Inputs del proceso
    # -------------------------------------------------------------------
    usuario = 'elasticidad_cruzada'
    esquema = 'PRECIO_PROMOCIONES'
    tabla = 'ELASTICITY_CROSS'
    ent = Entorno(
        proyecto=proyecto,
        store_banner=store_banner,
        fecha_hasta=f'{pd.to_datetime(execution_date):%Y-%m-%d}',
        categorias=categorias,
        tabla_regresion=f'{proyecto}.{esquema}.TMP_REGRESSION_PROCESSED_DATA_ELASTICITY',
        tabla_promo=f'{proyecto}.{esquema}.TMP_PROMOTION_DAILY',
        tabla_sustitutos=f'{proyecto}.ML_LAB.SKU_SUBSTITUTES_BY_CATEGORY',
        tabla_complementarios=f'{proyecto}.ML_LAB.COMPLEMENTARY_PRODUCTS',
    )

    def leer(query: str) -> pd.DataFrame:
        return readBigQuery(query=query, user=usuario, gbq_client=gbq_client)
    # ENDREGION

    # REGION: EAN focales y sus relacionados
    # -------------------------------------------------------------------
    seleccion = seleccionar_focales(ent, leer)
    focales = list(seleccion['ean'])
    pares = cargar_pares(ent, leer, focales)
    if pares.empty:
        msg = 'Ningun EAN focal tiene sustitutos ni complementarios en las tablas.'
        raise ValueError(msg)
    universo = sorted(set(pares['EAN_FOCAL']) | set(pares['EAN_RELACIONADO']))
    logger.info(
        f'EAN focales con relacionados: {pares["EAN_FOCAL"].nunique():,} de '
        f'{len(focales):,} - pares: {len(pares):,}'
    )
    logger.info(pares.groupby('TIPO_RELACION').size().to_string())
    # ENDREGION

    # REGION: Datos diarios y viabilidad de los pares
    # -------------------------------------------------------------------
    env = cargar_datos(ent, leer, universo)
    pares = marcar_viabilidad(pares, env)
    motivos = pares.loc[pares['MOTIVO_NO_ESTIMADO'] != '', 'MOTIVO_NO_ESTIMADO']
    logger.info('Pares no estimables:')
    logger.info(motivos.value_counts().to_string() if len(motivos) else '  (ninguno)')
    unicos = pares.loc[
        pares['MOTIVO_NO_ESTIMADO'] == '', ['EAN_FOCAL', 'EAN_RELACIONADO']
    ].drop_duplicates()
    if unicos.empty:
        msg = 'No quedan pares viables para estimar.'
        raise ValueError(msg)
    # ENDREGION

    # REGION: Estimacion y confiabilidad
    # -------------------------------------------------------------------
    estimados = estimar_todo(env, unicos)
    estimados = pd.DataFrame([clasificar(f) for f in estimados.to_dict('records')])
    resultado = armar_tabla(pares, estimados)
    logger.info('Confiabilidad por tipo de relacion:')
    logger.info(
        pd.crosstab(resultado['TIPO_RELACION'], resultado['CONFIABILIDAD']).to_string()
    )
    errores = estimados['ERROR_ESTIMACION'].astype(bool).sum()
    if errores:
        logger.info(f'ADVERTENCIA - pares con error de estimacion: {errores:,}')
    roster_slim = tabla_final(resultado, store_banner, mes_ejecucion)
    if roster_slim.empty:
        msg = 'Ningun par reportable: no se modifica la tabla destino.'
        raise ValueError(msg)
    logger.info(
        f'\nTabla final ({store_banner}): {len(roster_slim):,} filas, '
        f'{len(roster_slim.columns)} columnas'
    )
    logger.info(roster_slim.head(5).to_string(index=False))
    # ENDREGION

    # REGION: Asegurar que el dataset de destino exista
    # -------------------------------------------------------------------
    gbq_client.create_dataset(DatasetReference(proyecto, esquema), exists_ok=True)
    # ENDREGION

    # REGION: Carga a BigQuery -- historico por banner y mes de ejecucion
    # -------------------------------------------------------------------
    where_clause = (
        f"STORE_BANNER = '{store_banner}' AND MES_EJECUCION = '{mes_ejecucion}'"
    )

    deleteFromTable(table_ref=f'{proyecto}.{esquema}.{tabla}',
                    where_clause=where_clause,
                    gbq_client=gbq_client)

    uploadFrame(
        roster_slim,
        table_ddl_json_path=os.path.join('gbq_objects', 'ingest_elasticity_cross.json'),
        project=proyecto,
        gbq_client=gbq_client,
        if_exists='append'
    )

    logger.info(
        f'Se sube la tabla a GCP: {proyecto}.{esquema}.{tabla} '
        f'({len(roster_slim):,} filas)'
    )
    # ENDREGION


if __name__ == '__main__':
    main()
