# Default
from __future__ import annotations

import os
import json
import logging
import argparse
from logging import config

# Pip
import numpy as np
import pandas as pd
import statsmodels.api as sm
from sklearn.cluster import KMeans
from google.cloud.bigquery import Client

# Own
from common.constants import LOGGING_CONFIG
from common.databases.queries import QueryDict
from common.gcp_extended.bigquery import (
    uploadFrame,
    readBigQuery,
    deleteFromTable,
)
from common.gcp_extended.secretsmanager import getSecret  # noqa: F401


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
parser.add_argument(
    '--store_banner', type=str,
    help='Store banner'
)

parser.add_argument(
    '--store_id',
    type=str,
    help='Store id en formato JSON'
)

# -------------------------------------------------------------------------
#  SQL Queries
# -------------------------------------------------------------------------
SQL_QUERIES = QueryDict({    # Region: Explicación de query

    'query_principal':
    """
    SELECT * FROM `${table_processed_data}`
    where store_banner = '${store_banner}'
    and store_id  = '${store_id}'
    """,

    'query_sustitutos':
    """
    SELECT
        sku as material,
        substitute,
        substitution_score as score,
        substitution_rank as relevance,
        FORMAT_DATE('%Y%m', date) as p_month
    FROM `cl-bigdata-analytics-preprod.ML_LAB.SKU_SUBSTITUTES_BY_CATEGORY`
    WHERE store_banner = '${store_banner}'
    AND FORMAT_DATE('%Y%m', date) >= '${first_month}'
    AND FORMAT_DATE('%Y%m', date) <= '${last_month}'
    AND substitution_rank <= 5
    """,

    'query_pesos':
    """
    SELECT distinct ean,
                    CAST(CONTENIDO_BRUTO AS float64) * CAST(CONT_CONV_UMB AS int) as peso_total_ean
    FROM `${proyecto}.CDA_VISTAS.VW_DIM_PRODUCT`
    """
})


# -------------------------------------------------------------------------
# Functions and Classes
# -------------------------------------------------------------------------


def agregarFeriados(df_datos: pd.DataFrame) -> pd.DataFrame:
    """Agrega columnas binarias para feriados y días previos.

    Marca si la fecha es feriado o día previo usando listas fijas
    de fechas entre 2023 y 2026.

    Parameters
    ----------
    df_datos : pd.DataFrame
        DataFrame con columna 'p_date' en store_banner datetime64[ns].

    Returns
    -------
    pd.DataFrame
        Mismo DataFrame con columnas adicionales para feriados.
    """
    df_datos_feriados = df_datos.copy()

    # Función auxiliar para marcar fechas
    def marcar_fechas(columna: str, fechas: list[str]) -> None:
        fechas_dt = pd.to_datetime(fechas)
        df_datos_feriados[columna] = df_datos_feriados['p_date'].isin(fechas_dt).astype(int)

    # JUEVES Y VIERNES SANTO
    marcar_fechas('jueves_santo', ['2023-04-06','2024-03-28','2025-04-17','2026-04-02'])
    marcar_fechas('viernes_santo', ['2023-04-07','2024-03-29','2025-04-18','2026-04-03'])

    # 1 DE MAYO
    marcar_fechas('30_abril', ['2023-04-30','2024-04-30','2025-04-30','2026-04-30'])
    marcar_fechas('1_mayo', ['2023-05-01','2024-05-01','2025-05-01','2026-05-01'])

    # 21 DE MAYO
    marcar_fechas('20_mayo', ['2023-05-20','2024-05-20','2025-05-20','2026-05-20'])
    marcar_fechas('21_mayo', ['2023-05-21','2024-05-21','2025-05-21','2026-05-21'])

    # 20 DE JUNIO (PUEBLOS ORIGINARIOS)
    marcar_fechas('19_junio', ['2023-06-20','2024-06-19','2025-06-19','2026-06-20'])
    marcar_fechas('20_junio', ['2023-06-21','2024-06-20','2025-06-20','2026-06-21'])

    # 16 DE JULIO
    marcar_fechas('15_julio', ['2023-07-15','2024-07-15','2025-07-15','2026-07-15'])
    marcar_fechas('16_julio', ['2023-07-16','2024-07-16','2025-07-16','2026-07-16'])

    # 15 DE AGOSTO
    marcar_fechas('14_agosto', ['2023-08-14','2024-08-14','2025-08-14','2026-08-14'])
    marcar_fechas('15_agosto', ['2023-08-15','2024-08-15','2025-08-15','2026-08-15'])

    # FIESTAS PATRIAS
    marcar_fechas('14_septiembre', ['2023-09-14','2024-09-14','2025-09-14','2026-09-14'])
    marcar_fechas('15_septiembre', ['2023-09-15','2024-09-15','2025-09-15','2026-09-15'])
    marcar_fechas('16_septiembre', ['2023-09-16','2024-09-16','2025-09-16','2026-09-16'])
    marcar_fechas('17_septiembre', ['2023-09-17','2024-09-17','2025-09-17','2026-09-17'])
    marcar_fechas('18_septiembre', ['2023-09-18','2024-09-18','2025-09-18','2026-09-18'])
    marcar_fechas('19_septiembre', ['2023-09-19','2024-09-19','2025-09-19','2026-09-19'])

    # HALLOWEEN
    marcar_fechas('pre_halloween', ['2023-10-30','2024-10-30','2025-10-30','2026-10-30'])
    marcar_fechas('halloween', ['2023-10-31','2024-10-31','2025-10-31','2026-10-31'])

    # NAVIDAD
    marcar_fechas('pre_navidad', ['2023-12-23','2024-12-23','2025-12-23','2026-12-23'])
    marcar_fechas('navidad', ['2023-12-24','2024-12-24','2025-12-24','2026-12-24'])
    marcar_fechas('25_diciembre', ['2023-12-25','2024-12-25','2025-12-25','2026-12-25'])

    # AÑO NUEVO
    marcar_fechas('pre_ano_nuevo', ['2023-12-30','2024-12-30','2025-12-30','2026-12-30'])
    marcar_fechas('ano_nuevo', ['2023-12-31','2024-12-31','2025-12-31','2026-12-31'])
    marcar_fechas('1_enero', ['2023-01-01','2024-01-01','2025-01-01','2026-01-01'])

    # Agrupaciones
    columnas_feriados_irrenunciables = [
        '1_enero','1_mayo','18_septiembre','19_septiembre','25_diciembre'
    ]
    columnas_feriados = [
        'viernes_santo','21_mayo','20_junio','16_julio','15_agosto','halloween'
    ]
    columnas_pre_feriados = [
        'jueves_santo','30_abril','20_mayo','19_junio','15_julio','14_agosto',
        '14_septiembre','15_septiembre','16_septiembre','17_septiembre',
        'pre_halloween','pre_navidad','navidad','pre_ano_nuevo','ano_nuevo'
    ]

    df_datos_feriados['feriado_irrenunciable'] = (
        df_datos_feriados[columnas_feriados_irrenunciables].sum(axis=1) > 0).astype(int)
    df_datos_feriados['feriado'] = (
        df_datos_feriados[columnas_feriados].sum(axis=1) > 0).astype(int)
    df_datos_feriados['pre_feriado'] = (
        df_datos_feriados[columnas_pre_feriados].sum(axis=1) > 0).astype(int)

    return df_datos_feriados


def verificacionFactibilidadModelo(df_ean:pd.DataFrame) -> bool:
    """Verifica que se cumplan los requisitos para obtener un modelo.

    Realiza todas las verificaciones previas para ver si se puede obtener
    un modelo.

    Parameters
    ----------
    df_ean : pd.DataFrame
        Es el dataframe con los datos del ean que se quiere analizar.

    Returns
    -------
    cumple_verificaciones : bool
        Booleano que es True si cumple con todas las verificaciones.
    """
    # Se inicia el booleano con un valor de True
    cumple_verificaciones = True

    # Promedios anuales
    cantidad_promedio_anual = df_ean['cantidad_total'].mean()

    # Eliminar de ventas mucho menores
    proporcion_minima_cantidad = 10
    cantidad_min = cantidad_promedio_anual/proporcion_minima_cantidad

    df_ean_aux = df_ean[
        df_ean['cantidad_total'] >= cantidad_min].copy()


    # Verificación 1: Que tenga al menos 150 dias de ventas
    if df_ean_aux.shape[0] < 150:
        cumple_verificaciones = False

    return cumple_verificaciones


def obtenerBajaVariabilidadPrecio(df_ean: pd.DataFrame) -> float:
    """Calcula el porcentaje de días con precio dentro de un rango fijo.

    Esta función calcula el promedio anual del precio y determina qué
    porcentaje de los días el precio se mantuvo dentro de un rango de ±5%
    respecto al promedio. Es útil para evaluar la variabilidad del precio
    del ean.

    Parameters
    ----------
    df_ean : pd.DataFrame
        DataFrame con las columnas 'precio_promedio' y fechas del ean.

    Returns
    -------
    porcentaje_cumple : float
        Porcentaje de días en los que el precio estuvo dentro del rango
        establecido.
    """
    precio_promedio_anual = df_ean['precio_promedio'].mean()
    procentaje_variabilidad = 5
    limite_superior = precio_promedio_anual * (1 + procentaje_variabilidad/100)
    limite_inferior = precio_promedio_anual * (1 - procentaje_variabilidad/100)

    # Calcular si la mayoría de las filas cumplen con la variación
    cumple_precio = df_ean['precio_promedio'].between(limite_inferior,
                                                            limite_superior)
    # Porcentaje de filas que cumplieron (entre 0 y 1)
    return np.round(cumple_precio.mean()*100,2)


def obtenerModeloOLS(df: pd.DataFrame,
                       ean: str,
                       fecha_limite:str,
                       fecha_inicial_entrenamiento: str,
                       considerar_feriados: bool = True) -> sm.OLS:
    """Ajusta un modelo WLS (sin búsqueda de combinaciones) para un EAN.

    Usa todo el historial como entrenamiento. Incluye 'log_precio',
    dummies de días/feriados disponibles y, si hay cobertura suficiente,
    dummies de meses (sin estaciones). Suma variables de sustitutos si
    existen y no tienen NA.

    Parámetros
    ----------
    df : pd.DataFrame
        Histórico completo.
    ean : str
        Código EAN a modelar.
    fecha_inicial_entrenamiento : str
        Fecha mínima a considerar para entrenar (inclusive).
    considerar_feriados : bool
        Si False, excluye feriados y pre_feriado.

    Retorna
    -------
    sm.regression.linear_model.RegressionResultsWrapper | None
        Modelo ajustado; None si no es posible ajustar.
    """
    # -------------------------------------------------------------
    # 0) Filtrado básico
    # -------------------------------------------------------------
    df_ean = df[df['ean'] == ean].copy()
    df_ean = df_ean[df_ean['p_date'] >= fecha_inicial_entrenamiento]
    df_ean = df_ean[df_ean['p_date'] <= fecha_limite]

    if not considerar_feriados and not df_ean.empty:  # noqa: SIM102
        if {'feriado', 'pre_feriado'}.issubset(df_ean.columns):
            df_ean = df_ean.loc[(df_ean['feriado'] == 0) &
                                (df_ean['pre_feriado'] == 0)]

    if df_ean.empty:
        return None

    # -------------------------------------------------------------
    # 1) Transformaciones y limpieza
    # -------------------------------------------------------------
    # Requisitos mínimos
    req_cols = {'cantidad_total', 'precio_promedio', 'p_date'}
    if not req_cols.issubset(df_ean.columns):
        return None

    # Log-transform
    df_ean = df_ean.copy()
    df_ean['log_cantidad'] = np.log(df_ean['cantidad_total'])
    df_ean['log_precio'] = np.log(df_ean['precio_promedio'])

    # Filtro de outliers por baja venta (regla del 10% del promedio)
    cantidad_promedio = df_ean['cantidad_total'].mean()
    if not np.isfinite(cantidad_promedio) or cantidad_promedio <= 0:
        return None
    cantidad_min = cantidad_promedio / 10
    df_ean = df_ean[df_ean['cantidad_total'] >= cantidad_min].copy()
    if df_ean.empty:
        return None

    # -------------------------------------------------------------
    # 2) Variables candidatas (sin búsqueda)
    # -------------------------------------------------------------
    fixed_vars = [
        'log_precio',
        # dummies de días (si existen y varían)
        'martes', 'miercoles', 'jueves', 'viernes', 'sabado', 'domingo',
        # años (si existen y varían)
        '2024', '2025', '2026',
        # flags varios (si existen y varían)
        'multiplicador_x05',
        'jueves_santo', 'viernes_santo',
        '30_abril', '20_mayo', '21_mayo',
        '19_junio', '20_junio',
        '15_julio', '16_julio',
        '14_agosto', '15_agosto',
        '14_septiembre', '15_septiembre', '16_septiembre', '17_septiembre',
        'pre_halloween', 'halloween',
        'pre_navidad', 'navidad',
        'pre_ano_nuevo', 'ano_nuevo',
        'apo',
        'variacion_porcentual_subcategoria'
    ]
    # Mantener solo las que existen y varían (excepto log_precio)
    fixed_vars = [
        v for v in fixed_vars
        if (v == 'log_precio') or (v in df_ean.columns and df_ean[v].nunique() > 1)  # noqa: PD101
    ]

    # Meses (sin 'enero' para evitar trampa de dummies con constante)
    meses_vars = [
        'febrero', 'marzo', 'abril', 'mayo', 'junio',
        'julio', 'agosto', 'septiembre', 'octubre', 'noviembre', 'diciembre'
    ]
    meses_vars = [m for m in meses_vars if m in df_ean.columns and df_ean[m].nunique() > 1]  # noqa: PD101

    # Sustitutos (si existen y sin NA)
    sust_vars = []
    for v in ['variacion_top1_sustituto', 'variacion_top3_sustitutos']:
        if v in df_ean.columns and not df_ean[v].isna().any():
            sust_vars.append(v)  # noqa: PERF401

    # -------------------------------------------------------------
    # 3) Reglas adicionales
    # -------------------------------------------------------------
    # Anti-colinealidad: AÑO == diciembre en todo el set
    años_a_validar = ['2024', '2025']
    for año in años_a_validar:
        if (año in fixed_vars and año in df_ean.columns and 'diciembre' in df_ean.columns):  # noqa: SIM102
            # Comparamos si el año es idéntico a diciembre
            if (df_ean[año].values == df_ean['diciembre'].values).all():  # noqa: PD011
                fixed_vars = [v for v in fixed_vars if v != año]

    # Cobertura de meses: >=5 días en cada mes (1..12) según p_date
    dias_por_mes = (
        df_ean
        .assign(mes=df_ean['p_date'].dt.month,
                dia=df_ean['p_date'].dt.date)
        .groupby('mes')['dia'].nunique()
        .reindex(range(1, 13), fill_value=0)
    )
    cobertura_ok = (dias_por_mes >= 5).all()
    if not cobertura_ok:
        # Si no hay cobertura total, no incluir meses
        meses_vars = []

    # -------------------------------------------------------------
    # 4) Armar X y ajustar (WLS con pesos por recencia)
    # -------------------------------------------------------------
    current_vars = fixed_vars + meses_vars + sust_vars

    # Debe existir al menos la constante + log_precio
    if 'log_precio' not in current_vars:
        return None

    x = sm.add_constant(df_ean[current_vars], has_constant='add')
    y = df_ean['log_cantidad']

    # Pesos: fechas más recientes con mayor peso
    fechas = df_ean['p_date']
    fechas_ordenadas = fechas.sort_values().unique()
    mapa_pesos = {f: i + 1 for i, f in enumerate(fechas_ordenadas)}
    pesos = fechas.map(mapa_pesos)

    # Ajuste
    return sm.WLS(y, x, weights=pesos).fit()



# -------------------------------------------------------------------------
# Main function
# -------------------------------------------------------------------------
def main() -> None:  # noqa: D103


    # Parse input variables
    args = vars(parser.parse_args())
    execution_date: str = args['execution_date']
    proyecto: str = args['project_id']  # noqa: F841
    store_banner:str = args['store_banner']

    store_id_list = sorted(json.loads(args['store_id']), key=int)

    store_id_sql = ','.join(f"'{s}'" for s in store_id_list)
    store_id_str = ','.join(store_id_list)

    #parche nattype
    print('store_id_list: ', store_id_list)
    print('store_id_sql: ', store_id_sql)
    print('store_id_str: ', store_id_str)

    logging.info(f'execution_date: {execution_date}')
    logging.info(f'proyecto: {proyecto}')


    # Set gbq client for all subsequent queries
    gbq_client = Client()


    # REGION: Inputs del proceso
    #----------------------------------------------------------------------

    # Usuario
    usuario = 'product_elasticity'


    esquema = 'PRECIO_PROMOCIONES'
    # Parche 2: Nombre tabla ajustada para stores id
    tabla = 'PRODUCT_ELASTICITY_STORES_ID'

    # Tabla con data procesada
    # Parche 3: tabla con data procesada ajustada para stores id
    table_processed_data=f'{proyecto}.TMP.TMP_REGRESSION_PROCESSED_DATA_ELASTICITY_STORES_ID'

    #~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    # ENDREGION




    # REGION: Query de tabla con el detalle de productos
    #----------------------------------------------------------------------

    # Se genera query definitiva
    query_principal = SQL_QUERIES['query_principal'].substitute(
        table_processed_data = table_processed_data,
        store_banner = store_banner,
        store_id = store_id_str)


    logging.info('Inicia la consulta de principal ...')
    df_datos = readBigQuery(
        query=query_principal,
        user=usuario,
        gbq_client=gbq_client
    )

    #PARCHE nattypegit
    print('¿Está leyendo algo?: ', df_datos.shape)

    #--------------------------------------------------------------------------
    # ENDREGION

    # REGION: Se limpian y configuran los datos
    #----------------------------------------------------------------------

    # Columnas se dejan en minuscula
    df_datos.columns = df_datos.columns.str.lower()

    # Se transforma a store_banner fecha
    df_datos['p_date'] = pd.to_datetime(df_datos['p_date'], format='%Y-%m-%d')

    # Agregar columnas adicionales
    df_datos['store_banner'] = store_banner

    df_datos['p_year'] = df_datos['p_date'].dt.year.astype(str)
    logging.info('Agregados p_year')

    df_datos = pd.concat([df_datos,
                        pd.get_dummies(df_datos['p_year'], prefix='',
                                        prefix_sep='').astype(int)], axis=1)
    logging.info('Se agregan DUMMY del año')

    df_datos['ean'] = df_datos['ean'].astype(str)
    df_datos['product_description_ean'] = df_datos['product_description'] + ' - ' + df_datos['ean']
    df_datos['multiplicador_x05'] = df_datos['multiplicador_x05'].astype(int)

    #~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    # ENDREGION


    # REGION: Fin de semana
    #--------------------------------------------------------------------------

    # Crear la columna 'fds' basado en si es viernes, sábado o domingo
    df_datos['fds'] = df_datos['p_date'].dt.dayofweek.apply(
                                            lambda x: 1 if x in [4, 5, 6] else 0)

    # Crear la columna 'l_m_w' basado en si es lunes, martes o miercoles
    df_datos['l_m_w'] = df_datos['p_date'].dt.dayofweek.apply(
                                            lambda x: 1 if x in [0, 1, 2] else 0)

    # Crear la columna 'lunes' para indicar si el día es lunes
    df_datos['lunes'] = df_datos['p_date'].dt.dayofweek.apply(
                                                    lambda x: 1 if x == 0 else 0)

    # Crear la columna 'martes' para indicar si el día es martes
    df_datos['martes'] = df_datos['p_date'].dt.dayofweek.apply(
                                                    lambda x: 1 if x == 1 else 0)

    # Crear la columna 'miercoles' para indicar si el día es miércoles
    df_datos['miercoles'] = df_datos['p_date'].dt.dayofweek.apply(
                                                    lambda x: 1 if x == 2 else 0)

    # Crear la columna 'jueves' para indicar si el día es jueves
    df_datos['jueves'] = df_datos['p_date'].dt.dayofweek.apply(
                                                    lambda x: 1 if x == 3 else 0)

    # Crear la columna 'viernes' para indicar si el día es viernes
    df_datos['viernes'] = df_datos['p_date'].dt.dayofweek.apply(
                                                    lambda x: 1 if x == 4 else 0)

    # Crear la columna 'sabado' para indicar si el día es sábado
    df_datos['sabado'] = df_datos['p_date'].dt.dayofweek.apply(
                                                    lambda x: 1 if x == 5 else 0)

    # Crear la columna 'domingo' para indicar si el día es domingo
    df_datos['domingo'] = df_datos['p_date'].dt.dayofweek.apply(
                                                    lambda x: 1 if x == 6 else 0)


    logging.info('Se agregan DUMMY de dia de la semana (l_m_w, jueves, viernes, sábado o domingo)')

    #~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    # ENDREGION



    # REGION: Agregar DUMMY de meses del año
    #--------------------------------------------------------------------------

    # Extraer el número de mes desde 'p_month'
    df_datos['mes'] = df_datos['p_month'].astype(str).str[4:].astype(int)

    # Crear columnas dummy para cada mes
    for i, nombre_mes in enumerate(['enero', 'febrero', 'marzo',
                                    'abril', 'mayo', 'junio',
                                    'julio', 'agosto', 'septiembre',
                                    'octubre', 'noviembre', 'diciembre'],
                                    start=1):
        df_datos[nombre_mes] = (df_datos['mes'] == i).astype(int)

    # (Opcional) eliminar la columna auxiliar 'mes'
    df_datos = df_datos.drop(columns='mes')

    logging.info('Se agregan DUMMY meses del año')
    #~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    # ENDREGION



    # REGION: Agregar FERIADOS
    #--------------------------------------------------------------------------

    # Se deja como funcion ya que se usa el mismo codigo mas adelante para
    # agregar feriados a las proyecciones


    df_datos = agregarFeriados(df_datos)
    logging.info('Se agregan los feriados')

    #~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    # ENDREGION


    # REGION: Reordenación de dataframe principal
    #--------------------------------------------------------------------------

    # Lista de columnas deseadas
    columnas_deseadas = [
        'store_banner','product_description_ean',
        'category_description', 'sub_category_description', 'material', 'product_description',
        'ean', 'sales_uom', 'sales_unit', 'p_date', 'p_week', 'p_month',
        'ventas_totales_producto', 'cantidad_total', 'precio_promedio',
        '2023', '2024', '2025', '2026',
        'l_m_w', 'lunes', 'martes', 'miercoles', 'jueves', 'viernes', 'sabado', 'domingo',
        'enero', 'febrero', 'marzo', 'abril', 'mayo', 'junio',
        'julio', 'agosto', 'septiembre', 'octubre', 'noviembre', 'diciembre',
        'primer_dia_mes', 'ultimo_dia_mes',
        'jueves_santo', 'viernes_santo', '30_abril',
        '20_mayo', '21_mayo', '19_junio', '20_junio', '15_julio', '16_julio',
        '14_agosto', '15_agosto',
        '14_septiembre', '15_septiembre', '16_septiembre', '17_septiembre',
        'pre_halloween', 'halloween', 'pre_navidad', 'navidad',
        'pre_ano_nuevo', 'ano_nuevo',
        'feriado', 'pre_feriado',
        'multiplicador_x05', 'apo', 'proporcion_categoria',
        'ean_sustituto_1', 'ean_sustituto_2', 'ean_sustituto_3',
        'ean_sustituto_4', 'ean_sustituto_5',
        'variacion_porcentual_subcategoria',
        'variacion_top1_sustituto', 'variacion_top3_sustitutos'
    ]


    # Filtrar solo las columnas que existen en df_datos
    columnas_existentes = [col for col in columnas_deseadas if col in df_datos.columns]

    # Crear el nuevo dataframe
    df_final = df_datos[columnas_existentes].copy()

    logging.info('Se limpia y reordena por ventas el dataframe')

    #~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    # ENDREGION

    # REGION: Ordenar EAN por ventas
    #--------------------------------------------------------------------------

    # Paso 1: Calcular la suma de ventas por material
    suma_ventas_por_material = df_final.groupby('ean')['ventas_totales_producto'].sum()

    # Paso 2: Crear una columna temporal con la suma de ventas y ordenar
    df_final['suma_ventas'] = df_final['ean'].map(suma_ventas_por_material)
    df_final = df_final.sort_values(by='suma_ventas', ascending=False)

    #Eliminar la columna temporal si no la necesitas más
    df_final = df_final.drop('suma_ventas', axis=1)


    logging.info('Se ordena por ventas')

    #~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    # ENDREGION


    # REGION: Se establecen tipos
    #--------------------------------------------------------------------------

    dtypes_dict = {
        'category_description': 'string',
        'sub_category_description': 'string',
        'material': 'int32',
        'product_description': 'string',
        'ean': 'string',
        'sales_uom': 'string',
        'sales_unit': 'string',
        'p_week': 'int64',
        'p_month': 'int32',
        'ventas_totales_producto': 'int32',
        'cantidad_total': 'float64',
        'precio_promedio': 'int32',
        'l_m_w': 'int64',
        'lunes': 'int64',
        'martes': 'int64',
        'miercoles': 'int64',
        'jueves': 'int64',
        'viernes': 'int64',
        'sabado': 'int64',
        'domingo': 'int64',
        'enero': 'int32',
        'febrero': 'int32',
        'marzo': 'int32',
        'abril': 'int32',
        'mayo': 'int32',
        'junio': 'int32',
        'julio': 'int32',
        'agosto': 'int32',
        'septiembre': 'int32',
        'octubre': 'int32',
        'noviembre': 'int32',
        'diciembre': 'int32',
        'primer_dia_mes': 'int32',
        'ultimo_dia_mes': 'int32',
        'jueves_santo': 'int32',
        'viernes_santo': 'int32',
        '30_abril': 'int32',
        '20_mayo': 'int32',
        '21_mayo': 'int32',
        '19_junio': 'int32',
        '20_junio': 'int32',
        '15_julio': 'int32',
        '16_julio': 'int32',
        '14_agosto': 'int32',
        '15_agosto': 'int32',
        '14_septiembre': 'int32',
        '15_septiembre': 'int32',
        '16_septiembre': 'int32',
        '17_septiembre': 'int32',
        'pre_halloween': 'int32',
        'halloween': 'int32',
        'pre_navidad': 'int32',
        'navidad': 'int32',
        'pre_ano_nuevo': 'int32',
        'ano_nuevo': 'int32',
        'feriado': 'int32',
        'pre_feriado': 'int32',
        'multiplicador_x05': 'int32',
        'apo': 'int32',
        'proporcion_categoria': 'float64',
        'ean_sustituto_1': 'string',
        'ean_sustituto_2': 'string',
        'ean_sustituto_3': 'string',
        'ean_sustituto_4': 'string',
        'ean_sustituto_5': 'string',
        'variacion_porcentual_subcategoria': 'float64',
        'variacion_top1_sustituto': 'float64',
        'variacion_top3_sustitutos': 'float64',
    }

    df_final = df_final.astype(dtypes_dict)


    #~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    # ENDREGION


    # REGION: Calculo ELASTICIDAD
    #--------------------------------------------------------------------------

    # Datos entrenamiento

    ###PARCHE ERROR .min() arroja nantye y .strftime se cae:
    print(df_final['p_date'].min())
    print(df_final['p_date'].isna().sum())
    print(df_final['p_date'].notna().sum())
    print(len(df_final))

    fecha_inicial_entrenamiento = df_final['p_date'].min().strftime('%Y-%m-%d')
    fecha_limite = df_final['p_date'].max().strftime('%Y-%m-%d')
    considerar_feriados = True

    resultados = []
    procesados = 0
    total_eanes = df_final['ean'].nunique()

    # siguiente porcentaje meta (10, 20, 30, ..., 100)
    meta_avance = 10

    for categoria, df_categoria in df_final.groupby('category_description'):
        for ean in df_categoria['ean'].unique():
            df_ean = df_categoria[df_categoria['ean'] == ean]
            product_description = df_ean['product_description'].iloc[0]
            subcategoria = df_ean['sub_category_description'].iloc[0]
            material = df_ean['material'].iloc[0]
            sales_uom = df_ean['sales_uom'].iloc[0]
            ventas_ean = df_ean['ventas_totales_producto'].sum()
            elasticidad_valor = 'Sin datos'
            r2_valor = None
            n_obs_valor = None
            coeficientes = {}

            if verificacionFactibilidadModelo(df_ean):
                modelo = obtenerModeloOLS(
                    df_ean, ean, fecha_limite,
                    fecha_inicial_entrenamiento,
                    considerar_feriados=considerar_feriados
                )
                var_precio = obtenerBajaVariabilidadPrecio(df_ean)
                beta_precio = modelo.params['log_precio']
                r2_valor = round(modelo.rsquared_adj, 3)
                n_obs_valor = int(modelo.nobs)

                if var_precio > 90:
                    elasticidad_valor = 'Poca variabilidad'
                    coeficientes = modelo.params.to_dict()
                elif beta_precio >= 0:
                    elasticidad_valor = 'Elasticidad positiva'
                    coeficientes = modelo.params.to_dict()
                else:
                    elasticidad_valor = round(beta_precio, 2)
                    coeficientes = modelo.params.to_dict()
                    coef_names   = list(coeficientes.keys())
            else:
                elasticidad_valor = 'Modelo no factible'

            resultados.append({
                'categoria': categoria,
                'subcategoria': subcategoria,
                'ean': ean,
                'product_description': product_description,
                'material': material,
                'sales_uom': sales_uom,
                'ventas_ean': ventas_ean,
                'r2': r2_valor,
                'nun_observaciones': n_obs_valor,
                'elasticidad_original': elasticidad_valor,
                **coeficientes
            })

            procesados += 1
            pct = procesados * 100 / total_eanes

            # imprimir solo si supera el siguiente 10%
            if pct >= meta_avance:
                logging.info(f'Avance: {pct:.1f}%  ({procesados:,}/{total_eanes:,})')
                meta_avance += 10

    df_resultados = pd.DataFrame(resultados)

    logging.info(f'df resultados shape: {df_resultados.shape}')
    logging.info(f'value counts elasticidades: {df_resultados["elasticidad_original"].value_counts()}')  # noqa: E501

    #~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    # ENDREGION


    # REGION: Información necesaria para el contagio
    #----------------------------------------------------------------------


    p_month_inicial = df_final['p_month'].min()
    p_month_final = df_final['p_month'].max()

    # Query sustitutos
    query_sustitutos = SQL_QUERIES['query_sustitutos'].substitute(
        first_month=p_month_inicial,
        last_month=p_month_final,
        store_banner = store_banner
    )

    df_sustitutos  = readBigQuery(
        query = query_sustitutos,
        user = usuario,
        gbq_client=gbq_client
    )



    # Query de los pesos para ver mejor sustituo (material->ean)
    query_pesos = SQL_QUERIES['query_pesos'].substitute(proyecto = proyecto)

    df_pesos = readBigQuery(
        query=query_pesos,
        user=usuario,
        gbq_client=gbq_client
    )


    #~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    # ENDREGION


    # REGION: Contagio de elasticidades
    #----------------------------------------------------------------------


    # ---------------------------------------------------------------------
    # TANDA 0: PREPARACIÓN
    # ---------------------------------------------------------------------
    df_resultados = pd.DataFrame(resultados)
    df_pesos['ean'] = df_pesos['ean'].astype(str)
    df_resultados = df_resultados.merge(df_pesos, on='ean', how='left')

    # Convertir elasticidad original a numérica (si es posible)
    df_resultados['elasticidad_num'] = pd.to_numeric(
        df_resultados['elasticidad_original'], errors='coerce')


    # Calcular peso en ventas de EAN sin elasticidad original
    sin_elasticidad = df_resultados[df_resultados['elasticidad_num'].isna()]
    ventas_sin_elasticidad = sin_elasticidad['ventas_ean'].sum()
    ventas_total = df_resultados['ventas_ean'].sum()
    porcentaje_sin_elasticidad = ventas_sin_elasticidad / ventas_total * 100

    logging.info('TANDA 0 - VENTAS SIN ELASTICIDAD ORIGINAL:')
    logging.info(f'Ventas sin elasticidad: ${ventas_sin_elasticidad:,.0f}')
    logging.info(f'Porcentaje del total: {porcentaje_sin_elasticidad:.2f}%\n')

    logging.info(f'n EAN sin elasticidad: {df_resultados["elasticidad_original"].isna().sum()}')

    # ---------------------------------------------------------------------
    # TANDA 1: CONTAGIO MEJOR SUSTITUTO (usando similitud peso_total_ean)
    # ---------------------------------------------------------------------

    # Paso 1: Filtrar últimos 24 p_month en df_sustitutos
    ultimos_24 = sorted(df_sustitutos['p_month'].unique())[-24:]
    df_sust = df_sustitutos[df_sustitutos['p_month'].isin(ultimos_24)].copy()

    # Paso 2: Puntaje por relevance
    mapa_puntaje = {1: 5, 2: 4, 3: 3, 4: 2}
    df_sust['puntaje'] = df_sust['relevance'].map(mapa_puntaje).fillna(1).astype(int)

    # Paso 3: Puntaje total por (material, substitute)
    df_sust_sumado = (
        df_sust.groupby(['material', 'substitute'], as_index=False)['puntaje']
        .sum()
        .sort_values(['material', 'puntaje'], ascending=[True, False])
    )

    # Paso 4: Subset con elasticidades válidas
    df_validos = df_resultados.dropna(subset=['elasticidad_num'])

    # Paso 5: Función para obtener elasticidad del mejor sustituto
    # más parecido en peso
    def obtener_elasticidad_por_similitud(material_origen, peso_origen):
        candidatos = df_sust_sumado[  # noqa: PD011
            df_sust_sumado['material'] == material_origen]['substitute'].values
        for sustituto in candidatos:
            filas_sustituto = df_validos[df_validos['material'] == sustituto]
            if not filas_sustituto.empty:
                filas_sustituto = filas_sustituto.copy()
                filas_sustituto['diferencia_peso'] = (
                    filas_sustituto['peso_total_ean'] - peso_origen).abs()
                mejor_fila = filas_sustituto.sort_values('diferencia_peso').iloc[0]

                return pd.Series({
                    'elasticidad_contagiada': mejor_fila['elasticidad_num'],
                    'material_contagiante': sustituto,
                })
        return pd.Series({
            'elasticidad_contagiada': np.nan,
            'material_contagiante': np.nan,
        })



    # Paso 6: Aplicar función sustituto a filas con elasticidad no numérica
    mascarasin = df_resultados['elasticidad_num'].isna()
    antes_tanda1 = int(mascarasin.sum())

    resultado_contagio = (
        df_resultados.loc[mascarasin]
        .apply(
            lambda row: obtener_elasticidad_por_similitud(
                row['material'],
                row['peso_total_ean']
            ),
            axis=1,
        )
    )

    df_resultados.loc[
        mascarasin,
        ['elasticidad_contagiada', 'material_contagiante']
    ] = resultado_contagio

    # Paso 7: Las que ya tenían elasticidad original válida se copian

    df_resultados.loc[~mascarasin, 'elasticidad_contagiada'] = (
        df_resultados.loc[~mascarasin, 'elasticidad_num']
    )

    df_resultados.loc[~mascarasin, 'material_contagiante'] = (
        df_resultados.loc[~mascarasin, 'material'])

    print('df resultados columnas: ', df_resultados.columns)
    print('df_resultados shape: ', df_resultados.shape)
    print('df_resultados \n',
          df_resultados[['material','elasticidad_contagiada', 'material_contagiante']].head(10))

    logging.info(
        f'MATERIAL_CONTAGIANTE: {df_resultados["material_contagiante"].notna().sum()} con valor, '
        f'{df_resultados["material_contagiante"].isna().sum()} nulos'
    )

    despues_tanda1 = int(df_resultados['elasticidad_contagiada'].notna().sum())
    contagiados_tanda1 = despues_tanda1 - (len(df_resultados) - antes_tanda1)
    faltantes_tanda1 = len(df_resultados) - despues_tanda1
    logging.info(f'TANDA 1 - SUSTITUTO: {contagiados_tanda1} contagiados,'
        f' {faltantes_tanda1} aún sin elasticidad')

    # ---------------------------------------------------------------------
    # TANDA 2: CONTAGIO X SUBCATEGORÍA (ventas_ean más alta en subcat)
    # ---------------------------------------------------------------------

    faltantes_subcat = df_resultados['elasticidad_contagiada'].isna()
    df_validas = df_resultados[
        ~df_resultados['elasticidad_contagiada'].isna()].copy()

    def contagiar_por_subcategoria(row):
        subcat = row['subcategoria']
        candidatos = df_validas[df_validas['subcategoria'] == subcat]
        if candidatos.empty:
            return pd.Series({
                'elasticidad_contagiada': np.nan,
                'material_contagiante': np.nan,
            })

        fila_mejor = candidatos.sort_values('ventas_ean', ascending=False).iloc[0]

        return pd.Series({
            'elasticidad_contagiada': fila_mejor['elasticidad_contagiada'],
            'material_contagiante': fila_mejor['material'],
        })



    resultado_subcat = (
        df_resultados.loc[faltantes_subcat]
        .apply(contagiar_por_subcategoria, axis=1)
    )

    df_resultados.loc[
        faltantes_subcat,
        ['elasticidad_contagiada', 'material_contagiante']
    ] = resultado_subcat

    ###### Tipo de contagio #######################################
    df_resultados['tipo_contagio'] = np.na

    df_resultados.loc[
        mascarasin & df_resultados['material_contagiante'].notna(),
        'tipo_contagio'] = 'sustituto'

    df_resultados.loc[
        faltantes_subcat &
        resultado_subcat['material_contagiante'].notna().values,
        'tipo_contagio'
    ] = 'subcategoria'

    df_resultados['tipo_contagio'].value_counts(dropna=False)

    ##############################################################
    logging.info(
    f'MATERIAL_CONTAGIANTE: '
    f'{df_resultados["material_contagiante"].notna().sum()} con valor, '
    f'{df_resultados["material_contagiante"].isna().sum()} nulos')

    despues_tanda2 = int(df_resultados['elasticidad_contagiada'].notna().sum())
    contagiados_tanda2 = despues_tanda2 - despues_tanda1
    faltantes_tanda2 = len(df_resultados) - despues_tanda2
    logging.info(f'TANDA 2 - SUBCATEGORÍA: {contagiados_tanda2} contagiados,'
        f' {faltantes_tanda2} aún sin elasticidad')

    # ---------------------------------------------------------------------
    # TANDA 3: CONTAGIO POR CATEGORÍA (ventas_ean más alta en categoría)
    # ---------------------------------------------------------------------

    faltantes_cat = df_resultados['elasticidad_contagiada'].isna()
    df_validas = df_resultados[~df_resultados['elasticidad_contagiada'].isna()].copy()

    def contagiar_por_categoria(row):
        cat = row['categoria']
        candidatos = df_validas[df_validas['categoria'] == cat]
        if candidatos.empty:
            return np.nan
        fila_mejor = candidatos.sort_values('ventas_ean', ascending=False).iloc[0]
        return fila_mejor['elasticidad_contagiada']

    df_resultados.loc[faltantes_cat,
                    'elasticidad_contagiada'] = df_resultados.loc[faltantes_cat].apply(
        contagiar_por_categoria,
        axis=1
    )

    despues_tanda3 = df_resultados['elasticidad_contagiada'].notna().sum()
    contagiados_tanda3 = despues_tanda3 - despues_tanda2
    faltantes_tanda3 = len(df_resultados) - despues_tanda3
    logging.info(f'TANDA 3 - CATEGORÍA: {contagiados_tanda3} contagiados,'
        f' {faltantes_tanda3} aún sin elasticidad')

    # ---------------------------------------------------------------------
    # TANDA 4: ASIGNAR -1 A LOS QUE NO PUDIERON SER CONTAGIADOS
    # ---------------------------------------------------------------------

    df_resultados['elasticidad_contagiada'] = df_resultados[
        'elasticidad_contagiada'].fillna(-1)

    logging.info(f'TANDA 4 - DEFAULT: {faltantes_tanda3} filas rellenadas con -1')


    # ---------------------------------------------------------------------
    # AJUSTE FINAL: LIMITAR A UN MÍNIMO DE -8
    # ---------------------------------------------------------------------

    df_resultados['elasticidad_contagiada'] = df_resultados[
        'elasticidad_contagiada'].clip(lower=-8)

    # ---------------------------------------------------------------------
    # RESUMEN FINAL
    # ---------------------------------------------------------------------

    logging.info('\nRESUMEN FINAL DE ELASTICIDAD_CONTAGIADA:')
    print(df_resultados['elasticidad_contagiada'].describe())

    #~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    # ENDREGION


    # REGION: Clusterización de elasticidades
    #----------------------------------------------------------------------

    # Paso 1: Convertir a numérico y quitar NaN
    elasticidades = pd.to_numeric(df_resultados['elasticidad_contagiada'],
                                errors='coerce')
    mascara_valida = elasticidades.notna()

    # Paso 2: Identificar outliers (usaremos 1% y 99%)
    percentil_bajo = np.percentile(elasticidades[mascara_valida], 1)
    percentil_alto = np.percentile(elasticidades[mascara_valida], 99)

    # Separar datos
    mascara_interior = (elasticidades >= percentil_bajo) & (elasticidades <= percentil_alto)
    mascara_outlier_bajo = elasticidades < percentil_bajo
    mascara_outlier_alto = elasticidades > percentil_alto

    # Paso 3: KMeans sobre valores no outliers
    valores_interiores = elasticidades[mascara_interior].values.reshape(-1, 1)  # noqa: PD011
    kmeans = KMeans(n_clusters=2, n_init=10, random_state=42)
    labels_kmeans = kmeans.fit_predict(valores_interiores)

    # Paso 4: Determinar cuál es 'high' (más negativo)
    centroides = kmeans.cluster_centers_.flatten()
    label_high = np.argmin(centroides)  # más negativo
    mapeo = {label_high: 'high', 1 - label_high: 'low'}
    labels_asignados = [mapeo[label] for label in labels_kmeans]

    # Paso 5: Asignar resultado completo
    cluster_final = pd.Series(index=df_resultados.index, dtype='object')

    cluster_final[mascara_interior] = labels_asignados
    cluster_final[mascara_outlier_bajo] = 'high'  # más negativos aún → high
    cluster_final[mascara_outlier_alto] = 'low'   # menos negativos → low

    df_resultados['segmento_elasticidad'] = cluster_final


    #~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    # ENDREGION

    # REGION: Limpiar y subir a GCP
    #----------------------------------------------------------------------

    df_resultados['store_banner'] = store_banner
    df_resultados = df_resultados.rename(columns={'product_description':'descripcion_material',
                                                'sales_uom':'umv',
                                                'elasticidad_contagiada':'elasticidad'})


    df_gcp = df_resultados[['store_banner',  # noqa: RUF005
                            'categoria',
                            'material',
                            'descripcion_material',
                            'ean',
                            'umv',
                            'elasticidad',
                            'segmento_elasticidad'] + coef_names]

    print(df_gcp.info())

    df_gcp['store_id'] = store_id_str
    # Definir el WHERE
    where_clause = f"store_banner = '{store_banner}' and store_id = '{store_id_str}'"

    # Se elimina los datos para cierto store_banner y rango (si existen)
    deleteFromTable(table_ref=f'{proyecto}.{esquema}.{tabla}',
                    where_clause=where_clause,
                    gbq_client=gbq_client)



    # Se carga en BQ con los datos recalculados
    # Parche
    uploadFrame(
        df_gcp,
        table_ddl_json_path=os.path.join('gbq_objects',
                                         'ingest_product_elasticity_stores_id.json'),
        project=proyecto,
        gbq_client=gbq_client,
        if_exists='append'
    )

    logging.info('Se sube la tabla a GCP')


    #~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    # ENDREGION


if __name__ == '__main__':
    main()
