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
from scipy import stats
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
#  Parametros de modelo (shrinkage continuo, partial pooling)
# -------------------------------------------------------------------------
# "dias equivalentes" de peso que se le da al prior de grupo -- con
# n_dias_propio=K_SHRINKAGE, peso 50/50
MIN_DIAS_PARA_INTENTAR_MODELO_PROPIO = 45
K_SHRINKAGE = 150

MIN_DIAS_REGULAR_BASELINE_SUBCAT = 200
MIN_SKU_POR_SUBCATEGORIA = 10
MIN_DIAS_REGULAR_BASELINE_CATEGORIA = 200
MIN_DIAS_PARA_INCLUIR_ESTACIONALIDAD = 90

ZONA_RIESGO_DIAS_MIN = 60
ZONA_RIESGO_DIAS_MAX = 100
# redundante con top3 (corr=0.74)
VARIABLE_A_DESCARTAR_EN_RIESGO = 'variacion_top1_sustituto'

MAX_VIF_BASELINE = 5.0

PERCENTIL_WINSOR_TARGET = 0.99

VARIABLES_MAHALANOBIS_CANDIDATAS = [
    'variacion_top1_sustituto', 'variacion_top3_sustitutos',
    'variacion_porcentual_subcategoria',
]
PERCENTIL_WINSOR_MAHALANOBIS = 0.01
PERCENTIL_UMBRAL_MAHALANOBIS = 0.99

UMBRAL_DISPERSION_MAXIMA = 0.20
BANDA_INDICE_MINIMA = 0.2
BANDA_INDICE_MAXIMA = 5.0

CANDIDATAS_DOW = ['martes', 'miercoles', 'jueves', 'viernes', 'sabado', 'domingo']
CANDIDATAS_ESTACIONALIDAD = ['estacional_sin', 'estacional_cos']
CANDIDATAS_CALENDARIO_EXTRA = ['feriado', 'pre_feriado', 'primer_dia_mes',
                                'ultimo_dia_mes', 'multiplicador_x05', 'apo']
CANDIDATAS_CONTEXTO = ['variacion_top1_sustituto', 'variacion_top3_sustitutos',
                        'variacion_porcentual_subcategoria']

# Ecommerce tiene su PROPIA tabla productiva de regresion -- y dentro de
# esa tabla, el campo store_banner dice literalmente "Unimarc" o "Alvi"
# (no "Ecommerce Unimarc"/"Ecommerce Alvi").
MAPA_BANNER_REGRESSION_ECOMMERCE = {
    'Ecommerce Unimarc': 'Unimarc',
    'Ecommerce Alvi': 'Alvi',
}


# -------------------------------------------------------------------------
#  SQL Queries
# -------------------------------------------------------------------------
SQL_QUERIES = QueryDict({  # Region: Explicacion de query

    'query_venta_precio_completo':
    """
    SELECT
        material,
        ean,
        product_description,
        category_description,
        sub_category_description,
        sales_uom,
        p_date,
        precio_promedio,
        cantidad_total,
        primer_dia_mes,
        ultimo_dia_mes,
        multiplicador_x05,
        apo,
        variacion_porcentual_subcategoria,
        variacion_top1_sustituto,
        variacion_top3_sustitutos
    FROM `${table}`
    WHERE store_banner = '${store_banner}'
    ORDER BY material, p_date
    """,

    'query_promo_diario':
    """
    SELECT
        material,
        p_date,
        atributo_promocion
    FROM `${table}`
    WHERE store_banner = '${store_banner}'
    ORDER BY material, p_date
    """,
})


# -------------------------------------------------------------------------
#  Funciones: feriados y variables de calendario
# -------------------------------------------------------------------------
def agregarFeriados(df_datos: pd.DataFrame) -> pd.DataFrame:
    """Agrega columnas binarias 'feriado' y 'pre_feriado' por fecha."""
    df = df_datos.copy()  # noqa: PD901

    def marcar_fechas(fechas: list[str]) -> pd.Series:
        fechas_dt = pd.to_datetime(fechas)
        return df['p_date'].isin(fechas_dt).astype(int)

    columnas_feriados = []
    columnas_pre_feriados = []

    pares = [
        ('viernes_santo',
        ['2023-04-07', '2024-03-29', '2025-04-18', '2026-04-03'], True),
        ('jueves_santo',
        ['2023-04-06', '2024-03-28', '2025-04-17', '2026-04-02'], False),
        ('21_mayo', ['2023-05-21', '2024-05-21', '2025-05-21', '2026-05-21'], True),
        ('20_mayo', ['2023-05-20', '2024-05-20', '2025-05-20', '2026-05-20'], False),
        ('20_junio', ['2023-06-21', '2024-06-20', '2025-06-20', '2026-06-21'], True),
        ('19_junio', ['2023-06-20', '2024-06-19', '2025-06-19', '2026-06-20'], False),
        ('16_julio', ['2023-07-16', '2024-07-16', '2025-07-16', '2026-07-16'], True),
        ('15_julio', ['2023-07-15', '2024-07-15', '2025-07-15', '2026-07-15'], False),
        ('15_agosto', ['2023-08-15', '2024-08-15', '2025-08-15', '2026-08-15'], True),
        ('14_agosto', ['2023-08-14', '2024-08-14', '2025-08-14', '2026-08-14'], False),
        ('halloween', ['2023-10-31', '2024-10-31', '2025-10-31', '2026-10-31'], True),
        ('pre_halloween',
        ['2023-10-30', '2024-10-30', '2025-10-30', '2026-10-30'], False),
        ('navidad', ['2023-12-24', '2024-12-24', '2025-12-24', '2026-12-24'], False),
        ('pre_navidad',
        ['2023-12-23', '2024-12-23', '2025-12-23', '2026-12-23'], False),
        ('ano_nuevo', ['2023-12-31', '2024-12-31', '2025-12-31', '2026-12-31'], False),
        ('pre_ano_nuevo',
        ['2023-12-30', '2024-12-30', '2025-12-30', '2026-12-30'], False),
    ]

    for nombre, fechas, es_feriado in pares:
        df[nombre] = marcar_fechas(fechas)
        if es_feriado:
            columnas_feriados.append(nombre)
        else:
            columnas_pre_feriados.append(nombre)

    df['feriado'] = (df[columnas_feriados].sum(axis=1) > 0).astype(int)
    df['pre_feriado'] = (df[columnas_pre_feriados].sum(axis=1) > 0).astype(int)

    return df[['p_date', 'feriado', 'pre_feriado']].drop_duplicates()


def agregar_variables_calendario(df: pd.DataFrame) -> pd.DataFrame:
    """Agrega dummies de dia de semana y terminos de Fourier (estacionalidad
    continua) a partir de 'p_date'.
    """  # noqa: W505
    df = df.copy()  # noqa: PD901
    dow = df['p_date'].dt.dayofweek  # 0=lunes (referencia, sin dummy propia)
    dias = {
        1: 'martes', 2: 'miercoles', 3: 'jueves',
        4: 'viernes', 5: 'sabado', 6: 'domingo',
    }
    for num, nombre in dias.items():
        df[nombre] = (dow == num).astype(int)

    dia_anio = df['p_date'].dt.dayofyear
    df['estacional_sin'] = np.sin(2 * np.pi * dia_anio / 365.25)
    df['estacional_cos'] = np.cos(2 * np.pi * dia_anio / 365.25)

    return df


# -------------------------------------------------------------------------
#  Variables candidatas y helpers de preparacion
# -------------------------------------------------------------------------
def _variables_disponibles(df: pd.DataFrame, candidatas: list[str]) -> list[str]:
    """Filtra las variables candidatas a las que existen y tienen varianza."""  # noqa: W505
    return [v for v in candidatas if v in df.columns and df[v].notna().any()
            and df[v].nunique() > 1]  # noqa: E501, PD101


def _filtrar_por_vif(df_regular: pd.DataFrame, variables: list[str]) -> list[str]:
    """Elimina iterativamente la variable con peor VIF (via matriz de
    correlacion invertida). No protege ninguna variable por defecto.
    """
    columnas = list(variables)
    if len(columnas) <= 1:
        return columnas

    df_para_vif = df_regular.copy()
    for col in CANDIDATAS_CONTEXTO:
        if col in df_para_vif.columns:
            df_para_vif[col] = df_para_vif[col].fillna(0.0)

    columnas = [c for c in columnas if df_para_vif[c].nunique() > 1]  # noqa: PD101
    if len(columnas) <= 1:
        return columnas

    while len(columnas) > 1:
        try:
            corr = df_para_vif[columnas].corr().to_numpy()
            vifs = pd.Series(np.diag(np.linalg.inv(corr)), index=columnas)
        except np.linalg.LinAlgError:
            break

        if vifs.isna().any() or np.isinf(vifs).any():
            break

        if vifs.empty or vifs.max() <= MAX_VIF_BASELINE:
            break

        columnas.remove(vifs.idxmax())

    return columnas


def _construir_variables(df_regular: pd.DataFrame, incluir_estacionalidad: bool,
                        n_dias: int) -> list[str]:
    """Arma la lista final de variables candidatas: DOW+calendario+
    contexto,
    reduccion de zona de riesgo, y filtro de multicolinealidad (VIF).
    """
    variables = _variables_disponibles(
        df_regular, CANDIDATAS_DOW + CANDIDATAS_CALENDARIO_EXTRA + CANDIDATAS_CONTEXTO)
    if incluir_estacionalidad:
        variables += _variables_disponibles(df_regular, CANDIDATAS_ESTACIONALIDAD)

    if (ZONA_RIESGO_DIAS_MIN <= n_dias <= ZONA_RIESGO_DIAS_MAX
            and VARIABLE_A_DESCARTAR_EN_RIESGO in variables):
        variables = [v for v in variables if v != VARIABLE_A_DESCARTAR_EN_RIESGO]

    return _filtrar_por_vif(df_regular, variables)


def _preparar_x_y(df_regular: pd.DataFrame, variables: list[str]):
    """Arma (x, y) listos para ajustar, con winsorizacion del target al
    percentil 99 (evita que un dia de venta extrema distorsione el
    coeficiente aprendido).
    """
    df_regular = df_regular.copy()
    for col in CANDIDATAS_CONTEXTO:
        if col in df_regular.columns:
            df_regular[col] = df_regular[col].fillna(0.0)

    if variables:
        x = sm.add_constant(df_regular[variables], has_constant='add')
    else:
        x = pd.DataFrame({'const': 1.0}, index=df_regular.index)
    x = x.astype(float)

    limite_superior = df_regular['cantidad_total'].quantile(PERCENTIL_WINSOR_TARGET)
    cantidad_winsorizada = df_regular['cantidad_total'].clip(
        lower=0.01, upper=limite_superior)
    y = np.log(cantidad_winsorizada)

    return x, y


# -------------------------------------------------------------------------
#  Proteccion multivariada (distancia de Mahalanobis)
# -------------------------------------------------------------------------
def _centro_covarianza_robusta(x_continuas: np.ndarray) -> tuple:
    """Calcula centro y covarianza de un set de variables continuas,
    recortando cada columna a [percentil 1, percentil 99] antes de
    estimar. Retorna (centro, cov_inv, umbral_chi2, k).
    """
    k = x_continuas.shape[1]
    if k == 0:
        return None, None, None, 0

    x_recortada = x_continuas.copy()
    for j in range(k):
        p1 = np.percentile(x_recortada[:, j], PERCENTIL_WINSOR_MAHALANOBIS * 100)
        p99 = np.percentile(x_recortada[:, j], (1 - PERCENTIL_WINSOR_MAHALANOBIS) * 100)
        x_recortada[:, j] = np.clip(x_recortada[:, j], p1, p99)

    centro = x_recortada.mean(axis=0)
    covarianza = np.cov(x_recortada, rowvar=False)
    if k == 1:
        covarianza = np.array([[covarianza]])
    covarianza = covarianza + np.eye(k) * 1e-6

    try:
        cov_inv = np.linalg.inv(covarianza)
    except np.linalg.LinAlgError:
        cov_inv = np.linalg.pinv(covarianza)

    umbral = stats.chi2.ppf(PERCENTIL_UMBRAL_MAHALANOBIS, df=k)

    return centro, cov_inv, umbral, k


def _proyectar_mahalanobis(df_pred: pd.DataFrame, variables_continuas: list[str],
                            centro, cov_inv, umbral) -> pd.DataFrame:
    """Proyecta cada fila al borde de la zona de confianza de Mahalanobis
    si la excede, preservando la direccion del vector.
    """
    if not variables_continuas or centro is None:
        return df_pred

    df_pred = df_pred.copy()
    x_cont = df_pred[variables_continuas].fillna(0.0).to_numpy().astype(float)

    diffs = x_cont - centro
    d2 = np.einsum('ij,jk,ik->i', diffs, cov_inv, diffs)
    d2 = np.clip(d2, a_min=1e-12, a_max=None)

    factor = np.ones(len(d2))
    mask = d2 > umbral
    factor[mask] = np.sqrt(umbral / d2[mask])

    x_proyectado = centro + diffs * factor[:, None]
    df_pred[variables_continuas] = x_proyectado

    return df_pred


# -------------------------------------------------------------------------
#  Entrenamiento de un modelo (log-normal, con info de Mahalanobis)
# -------------------------------------------------------------------------
def _entrenar_modelo(df_regular: pd.DataFrame, incluir_estacionalidad: bool) -> dict:
    """Entrena un modelo log-normal (OLS) sobre dias 'Regular'. Retorna un
    diccionario con el modelo, variables, correccion de Duan, y la info
    de Mahalanobis. Retorna {'modelo': None, ...} si no se pudo entrenar.
    """
    vacio = {
        'modelo': None, 'variables': [], 'factor_correccion': np.nan,
        'variables_continuas': [], 'centro': None, 'cov_inv': None,
        'umbral_mahalanobis': None, 'r2_ajustado': np.nan,
    }
    if df_regular.empty:
        return vacio

    n_dias = len(df_regular)
    variables = _construir_variables(df_regular, incluir_estacionalidad, n_dias)
    x, y = _preparar_x_y(df_regular, variables)

    try:
        modelo = sm.OLS(y, x).fit()
    except Exception:  # noqa: BLE001
        return vacio

    factor_correccion = np.exp(modelo.resid).mean()

    variables_continuas = [
        v for v in VARIABLES_MAHALANOBIS_CANDIDATAS if v in variables]
    if variables_continuas:
        centro, cov_inv, umbral, _ = _centro_covarianza_robusta(
            x[variables_continuas].to_numpy())
    else:
        centro, cov_inv, umbral = None, None, None

    return {
        'modelo': modelo, 'variables': variables,
        'factor_correccion': factor_correccion,
        'variables_continuas': variables_continuas, 'centro': centro,
        'cov_inv': cov_inv, 'umbral_mahalanobis': umbral,
        'r2_ajustado': modelo.rsquared_adj,
    }


# -------------------------------------------------------------------------
#  Prediccion de una sola pieza (un modelo, con su propio factor_escala)
# -------------------------------------------------------------------------
def _predecir_pieza(df_pred: pd.DataFrame, modelo, variables: list[str],
                    factor_correccion: float, variables_continuas: list[str],
                    centro, cov_inv, umbral_mahalanobis, factor_escala: float = 1.0):
    """Prediccion cruda de UN modelo (propio o de grupo), con Mahalanobis
    ya aplicado. Es el bloque de construccion que predecir_baseline usa
    dos veces (propio + grupo) y combina con el peso de shrinkage.
    Retorna None si el modelo no existe.
    """
    if modelo is None:
        return None

    df_local = df_pred.copy()
    for col in CANDIDATAS_CONTEXTO:
        if col in df_local.columns:
            df_local[col] = df_local[col].fillna(0.0)

    df_local = _proyectar_mahalanobis(
        df_local, variables_continuas, centro, cov_inv, umbral_mahalanobis)

    if variables:
        x = sm.add_constant(df_local[variables], has_constant='add')
    else:
        x = pd.DataFrame({'const': 1.0}, index=df_local.index)

    columnas_diseno = ['const', *variables]
    x = x.reindex(columns=columnas_diseno, fill_value=0.0)
    x = x.astype(float)

    log_pred = modelo.predict(x)
    pred = np.exp(log_pred) * factor_correccion * factor_escala

    return pd.Series(np.asarray(pred), index=df_pred.index)


# -------------------------------------------------------------------------
#  Prediccion mezclada (shrinkage continuo: propio + grupo)
# -------------------------------------------------------------------------
def predecir_baseline(df_material: pd.DataFrame, info_baseline: dict) -> pd.Series:
    """Predice la cantidad esperada mezclando la prediccion del modelo
    propio y la del modelo de grupo, ponderadas por 'peso_propio' --
    el mecanismo de shrinkage continuo.
    """
    if info_baseline.get('nivel') == 'sin_baseline':
        return pd.Series(np.nan, index=df_material.index)

    peso_propio = info_baseline.get('peso_propio', 0.0)

    pred_propio = None
    if peso_propio > 0 and info_baseline.get('modelo_propio') is not None:
        pred_propio = _predecir_pieza(
            df_material, info_baseline['modelo_propio'],
            info_baseline['variables_propio'],
            info_baseline['factor_correccion_propio'],
            info_baseline['variables_continuas_propio'],
            info_baseline['centro_propio'], info_baseline['cov_inv_propio'],
            info_baseline['umbral_mahalanobis_propio'], factor_escala=1.0,
        )

    pred_grupo = None
    if peso_propio < 1 and info_baseline.get('modelo_grupo') is not None:
        pred_grupo = _predecir_pieza(
            df_material, info_baseline['modelo_grupo'],
            info_baseline['variables_grupo'],
            info_baseline['factor_correccion_grupo'],
            info_baseline['variables_continuas_grupo'],
            info_baseline['centro_grupo'], info_baseline['cov_inv_grupo'],
            info_baseline['umbral_mahalanobis_grupo'],
            factor_escala=info_baseline.get('factor_escala_grupo', 1.0),
        )

    if pred_propio is not None and pred_grupo is not None:
        return peso_propio * pred_propio + (1 - peso_propio) * pred_grupo
    if pred_propio is not None:
        return pred_propio
    if pred_grupo is not None:
        return pred_grupo

    return pd.Series(np.nan, index=df_material.index)


# -------------------------------------------------------------------------
#  Validacion de honestidad (sobre la prediccion YA MEZCLADA)
# -------------------------------------------------------------------------
def _es_baseline_confiable(df_regular: pd.DataFrame, info_blend: dict) -> bool:
    """Chequea la calibracion de la prediccion MEZCLADA (propio + grupo,
    ya ponderada) contra los propios dias de entrenamiento del SKU. Si
    mas de UMBRAL_DISPERSION_MAXIMA de esos dias caen fuera de banda,
    NO se considera confiable.
    """
    sin_propio = info_blend.get('modelo_propio') is None
    sin_grupo = info_blend.get('modelo_grupo') is None
    if sin_propio and sin_grupo:
        return False

    pred = predecir_baseline(df_regular, info_blend)
    indice = df_regular['cantidad_total'].to_numpy() / pred.to_numpy()
    indice = indice[np.isfinite(indice)]

    if len(indice) == 0:
        return False

    fuera_de_banda = (
        (indice < BANDA_INDICE_MINIMA) | (indice > BANDA_INDICE_MAXIMA)
    ).mean()
    return fuera_de_banda <= UMBRAL_DISPERSION_MAXIMA


def _calcular_factor_escala(df_material: pd.DataFrame, info_pieza: dict) -> float:
    """Recalibra un modelo de GRUPO al nivel de venta propio del SKU:
    compara el promedio real vs. lo que el modelo de grupo predice (ya
    con Mahalanobis aplicado, sin recalibrar aun) para esos dias.
    """
    pred = _predecir_pieza(
        df_material, info_pieza['modelo'], info_pieza['variables'],
        info_pieza['factor_correccion'],
        info_pieza['variables_continuas'], info_pieza['centro'], info_pieza['cov_inv'],
        info_pieza['umbral_mahalanobis'], factor_escala=1.0,
    )
    if pred is None:
        return np.nan

    promedio_predicho = pred.mean()
    promedio_real = df_material['cantidad_total'].mean()

    return (promedio_real / promedio_predicho) if promedio_predicho > 0 else np.nan


# -------------------------------------------------------------------------
#  Construccion de baselines -- shrinkage continuo (partial pooling)
# -------------------------------------------------------------------------
def construir_baselines(df_panel: pd.DataFrame) -> dict:
    """Entrena un modelo de grupo (subcategoria, si no califico categoria)
    y un modelo propio por material (si tiene evidencia minima), y los
    combina con un peso continuo segun cuanta evidencia propia existe.
    Valida honestidad sobre la MEZCLA final antes de aceptarla.
    """
    baselines = {}
    df_regular_todo = df_panel[df_panel['estado'] == 'Regular'].copy()

    sku_por_subcat = df_panel.groupby('sub_category_description')['material'].nunique()

    # --- Modelos de categoria (respaldo final del grupo) ---
    modelos_categoria = {}
    for categoria, df_cat in df_regular_todo.groupby('category_description'):
        if len(df_cat) < MIN_DIAS_REGULAR_BASELINE_CATEGORIA:
            modelos_categoria[categoria] = None
            continue
        incluir_estacionalidad = len(df_cat) >= MIN_DIAS_PARA_INCLUIR_ESTACIONALIDAD
        modelos_categoria[categoria] = _entrenar_modelo(df_cat, incluir_estacionalidad)

    # --- Modelos de subcategoria (preferidos como grupo, si califican) ---
    modelos_subcat = {}
    for subcat, df_subcat in df_regular_todo.groupby('sub_category_description'):
        n_sku_subcat = sku_por_subcat.get(subcat, 0)
        if (len(df_subcat) < MIN_DIAS_REGULAR_BASELINE_SUBCAT
                or n_sku_subcat <= MIN_SKU_POR_SUBCATEGORIA):
            modelos_subcat[subcat] = None
            continue
        incluir_estacionalidad = len(df_subcat) >= MIN_DIAS_PARA_INCLUIR_ESTACIONALIDAD
        modelos_subcat[subcat] = _entrenar_modelo(df_subcat, incluir_estacionalidad)

    vacio_pieza = {
        'modelo': None, 'variables': [], 'factor_correccion': np.nan,
        'variables_continuas': [], 'centro': None, 'cov_inv': None,
        'umbral_mahalanobis': None,
    }

    for material, df_material in df_regular_todo.groupby('material'):
        subcat = df_material['sub_category_description'].iloc[0]
        categoria = df_material['category_description'].iloc[0]
        n_dias_propio = len(df_material)

        # --- Modelo propio (umbral bajo -- el shrinkage regula la
        #  confianza) ---
        info_propio = None
        if n_dias_propio >= MIN_DIAS_PARA_INTENTAR_MODELO_PROPIO:
            incluir_estacionalidad = (
                n_dias_propio >= MIN_DIAS_PARA_INCLUIR_ESTACIONALIDAD)
            candidato = _entrenar_modelo(df_material, incluir_estacionalidad)
            if candidato['modelo'] is not None:
                info_propio = candidato

        # --- Modelo de grupo: subcategoria primero, si no categoria ---
        info_grupo, nivel_grupo, factor_escala_grupo = None, None, np.nan
        for candidato_nivel, candidato_dict in [
            ('subcategoria', modelos_subcat.get(subcat)),
            ('categoria', modelos_categoria.get(categoria)),
        ]:
            if candidato_dict is not None and candidato_dict.get('modelo') is not None:
                fe = _calcular_factor_escala(df_material, candidato_dict)
                if np.isfinite(fe):
                    info_grupo = candidato_dict
                    nivel_grupo = candidato_nivel
                    factor_escala_grupo = fe
                    break

        if info_propio is None and info_grupo is None:
            baselines[material] = {
                'nivel': 'sin_baseline', 'peso_propio': np.nan, 'nivel_grupo': None,
                'modelo_propio': None, 'variables_propio': [],
                'factor_correccion_propio': np.nan,
                'variables_continuas_propio': [], 'centro_propio': None,
                'cov_inv_propio': None,
                'umbral_mahalanobis_propio': None,
                'modelo_grupo': None, 'variables_grupo': [],
                'factor_correccion_grupo': np.nan,
                'variables_continuas_grupo': [], 'centro_grupo': None,
                'cov_inv_grupo': None,
                'umbral_mahalanobis_grupo': None, 'factor_escala_grupo': np.nan,
                'r2_ajustado': np.nan, 'n_dias_entrenamiento': n_dias_propio,
            }
            continue

        # --- Peso de shrinkage: 0 si no hay propio, 1 si no hay grupo ---
        if info_propio is None:
            peso_propio = 0.0
        elif info_grupo is None:
            peso_propio = 1.0
        else:
            peso_propio = n_dias_propio / (n_dias_propio + K_SHRINKAGE)

        propio_o_vacio = info_propio or vacio_pieza
        grupo_o_vacio = info_grupo or vacio_pieza

        info_blend = {
            'nivel': 'blend',
            'peso_propio': peso_propio,
            'nivel_grupo': nivel_grupo,
            'modelo_propio': propio_o_vacio['modelo'],
            'variables_propio': propio_o_vacio['variables'],
            'factor_correccion_propio': propio_o_vacio['factor_correccion'],
            'variables_continuas_propio': propio_o_vacio['variables_continuas'],
            'centro_propio': propio_o_vacio['centro'],
            'cov_inv_propio': propio_o_vacio['cov_inv'],
            'umbral_mahalanobis_propio': propio_o_vacio['umbral_mahalanobis'],
            'modelo_grupo': grupo_o_vacio['modelo'],
            'variables_grupo': grupo_o_vacio['variables'],
            'factor_correccion_grupo': grupo_o_vacio['factor_correccion'],
            'variables_continuas_grupo': grupo_o_vacio['variables_continuas'],
            'centro_grupo': grupo_o_vacio['centro'],
            'cov_inv_grupo': grupo_o_vacio['cov_inv'],
            'umbral_mahalanobis_grupo': grupo_o_vacio['umbral_mahalanobis'],
            'factor_escala_grupo': factor_escala_grupo,
            'r2_ajustado': (info_propio or {}).get(
                'r2_ajustado', (info_grupo or {}).get('r2_ajustado', np.nan)),
            'n_dias_entrenamiento': n_dias_propio,
        }

        if _es_baseline_confiable(df_material, info_blend):
            baselines[material] = info_blend
        else:
            baselines[material] = {
                **info_blend,
                'nivel': 'sin_baseline',
            }

    return baselines


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
    usuario = 'baseline'
    esquema = 'PRECIO_PROMOCIONES'
    tabla = 'BASELINE_PANEL'

    dataset_regression = 'TMP'

    # Ecommerce tiene su PROPIA tabla productiva de regresion.
    if store_banner in MAPA_BANNER_REGRESSION_ECOMMERCE:
        table_processed_data = f'{proyecto}.{dataset_regression}.TMP_ECOMMERCE_REGRESSION_PROCESSED_DATA_ELASTICITY'  # noqa: E501
        store_banner_regression = MAPA_BANNER_REGRESSION_ECOMMERCE[store_banner]
    else:
        table_processed_data = f'{proyecto}.{dataset_regression}.TMP_REGRESSION_PROCESSED_DATA_ELASTICITY'  # noqa: E501
        store_banner_regression = store_banner

    table_promotion_daily = f'{proyecto}.{esquema}.TMP_PROMOTION_DAILY'
    # ENDREGION

    # REGION: Asegurar que el dataset de destino exista
    # -------------------------------------------------------------------
    dataset_ref = DatasetReference(proyecto, esquema)
    gbq_client.create_dataset(dataset_ref, exists_ok=True)
    # ENDREGION

    # REGION: Carga de datos
    # -------------------------------------------------------------------
    query_venta_precio = SQL_QUERIES['query_venta_precio_completo'].substitute(
        table=table_processed_data, store_banner=store_banner_regression)
    df_venta = readBigQuery(
        query=query_venta_precio, user=usuario, gbq_client=gbq_client)

    query_promo_diario = SQL_QUERIES['query_promo_diario'].substitute(
        table=table_promotion_daily, store_banner=store_banner)
    df_promo = readBigQuery(
        query=query_promo_diario, user=usuario, gbq_client=gbq_client)
    # ENDREGION

    # REGION: Construccion del panel diario (precio, estado, calendario,
    #  contexto)
    # -------------------------------------------------------------------
    df_venta = df_venta.copy()
    df_venta['material'] = df_venta['material'].astype(str)
    df_venta['p_date'] = pd.to_datetime(df_venta['p_date'])
    df_venta['precio_promedio'] = df_venta['precio_promedio'].astype(float)
    df_venta['cantidad_total'] = df_venta['cantidad_total'].astype(float)

    for col_entera in ['primer_dia_mes', 'ultimo_dia_mes', 'multiplicador_x05', 'apo']:
        if col_entera in df_venta.columns:
            df_venta[col_entera] = df_venta[col_entera].fillna(0).astype('int64')

    df_promo = df_promo.copy()
    if not df_promo.empty:
        df_promo['material'] = df_promo['material'].astype(str)
        df_promo['p_date'] = pd.to_datetime(df_promo['p_date'])
        df_promo = df_promo.drop_duplicates(subset=['material', 'p_date'])

    df_panel = df_venta.merge(
        df_promo[['material', 'p_date', 'atributo_promocion']] if not df_promo.empty
        else pd.DataFrame(columns=['material', 'p_date', 'atributo_promocion']),
        on=['material', 'p_date'],
        how='left',
    )

    df_panel['estado'] = df_panel['atributo_promocion'].fillna('Regular')
    df_panel['es_regular'] = (df_panel['estado'] == 'Regular').astype(int)

    feriados = agregarFeriados(df_panel[['p_date']].drop_duplicates())
    df_panel = df_panel.merge(feriados, on='p_date', how='left')

    df_panel = agregar_variables_calendario(df_panel)

    df_panel = df_panel.sort_values(['material', 'p_date']).reset_index(drop=True)

    logging.info(f'Panel diario construido: {df_panel.shape}')
    # ENDREGION

    # REGION: Construccion de baselines (shrinkage continuo)
    # -------------------------------------------------------------------
    baselines = construir_baselines(df_panel)

    logging.info('Distribucion de niveles:')
    niveles = pd.Series([b['nivel'] for b in baselines.values()])
    logging.info(niveles.value_counts().to_dict())
    # ENDREGION

    # REGION: Prediccion del baseline para todo el panel
    # -------------------------------------------------------------------
    piezas_prediccion = []
    for material, df_material in df_panel.groupby('material'):
        info_baseline = baselines.get(material, {'nivel': 'sin_baseline'})
        pred = predecir_baseline(df_material, info_baseline)
        piezas_prediccion.append(pred)

    df_panel['cantidad_esperada_baseline'] = pd.concat(piezas_prediccion).sort_index()
    df_panel['nivel_baseline'] = df_panel['material'].map(
        lambda m: baselines.get(m, {}).get('nivel', 'sin_baseline'))
    df_panel['peso_propio_baseline'] = df_panel['material'].map(
        lambda m: baselines.get(m, {}).get('peso_propio', np.nan))
    df_panel['indice_venta'] = (
        df_panel['cantidad_total'] / df_panel['cantidad_esperada_baseline'])

    n_materiales = df_panel['material'].nunique()
    logging.info(f'Prediccion de baseline completa para {n_materiales:,} materiales')
    # ENDREGION

    # REGION: Carga a BigQuery
    # -------------------------------------------------------------------
    columnas_a_subir = [
        'material', 'ean', 'product_description', 'category_description',
        'sub_category_description', 'sales_uom', 'p_date', 'estado', 'precio_promedio',
        'cantidad_total', 'cantidad_esperada_baseline', 'indice_venta',
        'nivel_baseline', 'peso_propio_baseline',
    ]
    columnas_disponibles = [c for c in columnas_a_subir if c in df_panel.columns]
    df_gcp = df_panel[columnas_disponibles].copy()
    df_gcp['store_banner'] = store_banner

    where_clause = f"store_banner = '{store_banner}'"

    # Se elimina los datos para este store_banner (si existen) antes de
    # recargar -- asi una corrida de otro banner no borra lo ya subido.
    deleteFromTable(table_ref=f'{proyecto}.{esquema}.{tabla}',
                    where_clause=where_clause,
                    gbq_client=gbq_client)

    uploadFrame(
        df_gcp,
        table_ddl_json_path=os.path.join('gbq_objects', 'ingest_baseline_panel.json'),
        project=proyecto,
        gbq_client=gbq_client,
        if_exists='append'
    )

    logging.info(
        f'Se sube la tabla a GCP: {proyecto}.{esquema}.{tabla} ({len(df_gcp):,} filas)')
    # ENDREGION


if __name__ == '__main__':
    main()
