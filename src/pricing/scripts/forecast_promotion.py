# Default
from __future__ import annotations

import io
import re
import logging
import argparse
import posixpath
from logging import config

# Pip
import numpy as np
import pandas as pd
import statsmodels.api as sm
from google.cloud.bigquery import Client

import common.gcp_extended.secretsmanager as secretmanager
import common.office365_extended.sharepoint as sp

# Own
from common.constants import LOGGING_CONFIG
from common.databases.queries import QueryDict
from common.gcp_extended.bigquery import (
    readBigQuery,
)


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

# -------------------------------------------------------------------------
#  SQL Queries
# -------------------------------------------------------------------------
SQL_QUERIES = QueryDict({
    'query_data_procesada':
    """
    with categorias as (
        select distinct desc_categoria
        from `cl-bigdata-analytics-preprod.CDA_VISTAS.VW_FACT_WORKFLOW`
        where registro_valido = 'X'
        and n_promocion in (${categorias})
        )

    SELECT * FROM `${path_table}`
    Where store_banner = '${store_banner}'
    and CATEGORY_DESCRIPTION in (select desc_categoria from categorias)
    """,
    'query_promos_forecasting':
"""
SELECT DISTINCT
    n_promocion,
    nombre_promocion,
    descripcion_evento_promocional,
    descripcion_mecanica,
    desc_categoria,
    material,
    desc_material,
    un_medida_venta,
    ean,
    precio_modal,
    precio_modal_total,
    precio_promocional,
    precio_total_promocional,
    ahorro,
    ahorro_total,
    desc_promocion,
    cantidad_n,
    cantidad_m,
    fecha_inicio_de_promocion,
    fecha_fin_de_promocion,
    porcentaje_cobertura,
    organizacion_ventas,
    canal_distribucion
FROM `cl-bigdata-analytics-preprod.CDA_VISTAS.VW_FACT_WORKFLOW`
WHERE organizacion_ventas = '1000'
  AND canal_distribucion = '10'
  AND registro_valido = 'X'
  AND n_promocion in (${string_promos})
ORDER BY nombre_promocion, desc_categoria ,material
"""
})


# -------------------------------------------------------------------------
# Functions and Classes
# -------------------------------------------------------------------------


# Generar meses anteriores
def generarMesesPrevios(df: pd.DataFrame) -> pd.DataFrame:
    """Crea filas con desfases de 1, 2 y 3 meses hacia atrás.

    Para cada desfase, ajusta el campo 'p_month' manteniendo
    'p_month_ref' como referencia del mes original.

    Parameters
    ----------
    df : pd.DataFrame
        DataFrame con columnas 'ean' y 'p_month' en store_banner YYYYMM.

    Returns
    -------
    pd.DataFrame
        DataFrame con desfases mensuales agregados.
    """
    lista_frames = []
    for desfase in [1, 2, 3]:
        df_temp = df.copy()
        df_temp['p_month_ref'] = df_temp['p_month']

        año = df_temp['p_month_ref'] // 100
        mes = df_temp['p_month_ref'] % 100 - desfase

        año -= (mes <= 0)
        mes = (mes - 1) % 12 + 1
        df_temp['p_month'] = año * 100 + mes
        lista_frames.append(df_temp[['ean', 'p_month_ref', 'p_month']])
    return pd.concat(lista_frames)


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
        '2024', '2025',
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
    # Anti-colinealidad: 2024 == diciembre en todo el set
    if ('2024' in fixed_vars and  # noqa: SIM102
            '2024' in df_ean.columns and
            'diciembre' in df_ean.columns):
        if (df_ean['2024'].values == df_ean['diciembre'].values).all():  # noqa: PD011
            fixed_vars = [v for v in fixed_vars if v != '2024']

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


def obtenerProyeccionV2(df_pre_proyeccion: pd.DataFrame,
                      model: sm.OLS) -> pd.DataFrame:
    """Genera proyecciones de demanda y ventas para un EAN dado.

    Esta función utiliza un modelo OLS previamente entrenado para estimar
    la cantidad demandada a partir de un DataFrame con información de
    precios y otras variables relevantes. Calcula la cantidad proyectada
    y las ventas proyectadas multiplicando por el precio promedio.

    Parameters
    ----------
    df_pre_proyeccion : pd.DataFrame
        DataFrame con la información de fechas futuras a proyectar.

    model : sm.OLS
        Modelo OLS ajustado previamente sobre datos históricos.

    Returns
    -------
    df_proyeccion : pd.DataFrame
        DataFrame con las columnas de cantidad y ventas proyectadas
        por fecha.
    """
    # Obtener dataframe filtrado al ean de interes

    df_pre_proyeccion = df_pre_proyeccion.sort_values(by='p_date')
    df_pre_proyeccion['log_precio']   = np.log(df_pre_proyeccion['precio_promedio'])

    # Variables dependientes
    x_vars = [var for var in model.model.exog_names if var != 'const']


    # Variables independientes
    X_proyeccion = df_pre_proyeccion[x_vars]  # noqa: N806


    # En muy pocas ocaciones hay NaN en algunos valores
    # Esto ocurre en filas que se eliminaron en entrenamiento pero se
    # quieren predecir
    X_proyeccion = X_proyeccion.fillna(0)  # noqa: N806

    # Agregar constantes
    X_proyeccion = sm.add_constant(X_proyeccion, has_constant='add')  # noqa: N806

    # Se copia, ya que ahora se llamara df_proyeccion
    df_proyeccion = df_pre_proyeccion.copy()

    # Predicción de log_cantidad usando el modelo
    df_proyeccion['log_cantidad_predicha'] = model.predict(X_proyeccion)

    # Calcular cantidad_total a partir de log_cantidad
    df_proyeccion['cantidad_total_predicha'] = np.exp(df_proyeccion['log_cantidad_predicha'])

    # Calcular cantidad_total a partir de log_cantidad
    df_proyeccion['ventas_totales_producto_predicha'] = (
        df_proyeccion['precio_promedio'] * df_proyeccion['cantidad_total_predicha'])

    df_proyeccion[['cantidad_total_predicha',
                    'ventas_totales_producto_predicha']
                    ] = df_proyeccion[
                    ['cantidad_total_predicha',
                     'ventas_totales_producto_predicha']].astype(int)

    df_proyeccion = df_proyeccion.sort_values(by='p_date')
    df_proyeccion['p_date'] = pd.to_datetime(df_proyeccion['p_date'])

    return df_proyeccion


#--------------------------------------------------------------------------
# VERIFICACIONES
#--------------------------------------------------------------------------

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




def obtenerDfDefaultProyeccion(
                                df_final2,
                                ean,
                                fecha_inicial_proyeccion,
                                fecha_final_proyeccion,
                                fecha_limite,
                                considerar_feriados,
                                fecha_inicial_entrenamiento
                                )->pd.DataFrame:
    """Genera un DataFrame con datos históricos y proyectados.

    Construye el conjunto de datos necesario para proyectar un EAN
    entre fechas definidas. Incluye datos conocidos e incorpora
    fechas futuras con precios estimados y variables relevantes.

    Parameters
    ----------
    df_final2 : pd.DataFrame
        DataFrame histórico del EAN con precios y cantidades.
    ean : str
        EAN que se desea proyectar.
    fecha_inicial_proyeccion : str
        Fecha inicial del periodo a proyectar (YYYY-MM-DD).
    fecha_final_proyeccion : str
        Fecha final del periodo a proyectar (YYYY-MM-DD).
    fecha_limite : str
        Fecha límite usada para ajustar el modelo OLS.
    considerar_feriados : bool
        Indica si se consideran feriados en la proyección.
    fecha_inicial_entrenamiento : str
        Fecha inicial del entrenamiento

    Returns
    -------
    pd.DataFrame
        DataFrame con fechas, etiqueta de origen, precio promedio,
        cantidades, ventas y todas las variables usadas en la
        proyección.
    """

    modelo = obtenerModeloOLS(df_final2, ean,
                              fecha_limite,fecha_inicial_entrenamiento,
                              considerar_feriados=considerar_feriados)

    df_ean = df_final2[df_final2['ean'] == ean].copy()
    df_ean['log_cantidad'] = np.log(df_ean['cantidad_total'])
    df_ean['log_precio'] = np.log(df_ean['precio_promedio'])
    df_ean = df_ean.sort_values(by='p_date')

    x_vars = [var for var in modelo.model.exog_names if var != 'const']
    if 'log_precio' in x_vars:
        x_vars.remove('log_precio')

    # Hay veces que apo no esta en el modelo ya que no existia antes,
    # se debe agregar manualmente para que no haya error. Pero aunque
    # el usuario agregue apos no habra cambio en la proyeccion
    # ya que no estuvo en el entrenamiento
    if 'apo' not in x_vars:
        x_vars.append('apo')

    fecha_inicial_dt = pd.to_datetime(fecha_inicial_proyeccion)
    fecha_final_dt = pd.to_datetime(fecha_final_proyeccion)

    df_ean_proyeccion = df_ean[(df_ean['p_date'] >= fecha_inicial_dt) &
                               (df_ean['p_date'] <= fecha_final_dt)].copy()

    columnas_a_mostrar = ['p_date', 'precio_promedio', 'cantidad_total',
                          'ventas_totales_producto', 'feriado', 'pre_feriado',
                          *x_vars]
    columnas_a_mostrar = list(dict.fromkeys(columnas_a_mostrar))

    df_ean_proyeccion = df_ean_proyeccion[columnas_a_mostrar]
    df_ean_proyeccion['conocido'] = 'Si'  # ✅ dato original

    ultima_fecha_existente = df_final2['p_date'].max()

    if fecha_final_dt > ultima_fecha_existente:
        fechas_faltantes = pd.date_range(
            start=max(ultima_fecha_existente + pd.Timedelta(days=1), fecha_inicial_dt),
            end=fecha_final_dt,
            freq='D'
        )

        df_faltante = pd.DataFrame({'p_date': fechas_faltantes})

        # Asignar valores básicos
        df_faltante['precio_promedio'] = int(df_ean['precio_promedio'].mean())
        df_faltante['cantidad_total'] = -1
        df_faltante['ventas_totales_producto'] = -1

        # Día de la semana
        df_faltante['jueves'] = (df_faltante['p_date'].dt.dayofweek == 3).astype(int)
        df_faltante['viernes'] = (df_faltante['p_date'].dt.dayofweek == 4).astype(int)
        df_faltante['sabado'] = (df_faltante['p_date'].dt.dayofweek == 5).astype(int)
        df_faltante['domingo'] = (df_faltante['p_date'].dt.dayofweek == 6).astype(int)

        # Dummies de meses
        df_faltante['p_month'] = df_faltante['p_date'].dt.strftime('%Y%m')
        df_faltante['mes'] = df_faltante['p_month'].astype(int) % 100
        for i, nombre_mes in enumerate(['enero', 'febrero', 'marzo',
                                        'abril', 'mayo', 'junio',
                                        'julio', 'agosto', 'septiembre',
                                        'octubre', 'noviembre', 'diciembre'], start=1):
            df_faltante[nombre_mes] = (df_faltante['mes'] == i).astype(int)

        # Dummies de año
        df_faltante['p_year'] = df_faltante['p_date'].dt.year
        df_faltante['p_year'] = df_faltante['p_year'].replace({2026: 2025})

        df_faltante['p_year'] = df_faltante['p_year'].astype(str)


        df_faltante = pd.concat([df_faltante, pd.get_dummies(df_faltante['p_year'],
                                prefix='', prefix_sep='')], axis=1)


        # Feriados
        df_faltante = agregarFeriados(df_faltante)

        # Eliminar feriados irrenunciables
        df_faltante = df_faltante[df_faltante['feriado_irrenunciable'] != 1]

        # Etiqueta
        df_faltante['conocido'] = 'No'

        # Asegurar todas las columnas
        for col in df_ean_proyeccion.columns:
            if col not in df_faltante.columns:
                df_faltante[col] = 0
        df_faltante = df_faltante[df_ean_proyeccion.columns]


        df_ean_proyeccion = pd.concat([df_ean_proyeccion, df_faltante], ignore_index=True)



    # Formatear fecha
    df_ean_proyeccion['p_date'] = pd.to_datetime(
        df_ean_proyeccion['p_date']).dt.strftime('%Y-%m-%d')

    # Reordenar columnas al final
    orden_preferido = ['p_date', 'conocido', 'precio_promedio',
                       'cantidad_total','ventas_totales_producto', 'apo']
    otras_columnas = [col for col in df_ean_proyeccion.columns if col not in orden_preferido]
    return df_ean_proyeccion[orden_preferido + otras_columnas]



def calcular_forecast(
    gcp_project_id: str,
    store_banner: str,
    file_site: str,
    secret_name: str,
    gbq_client: Client,
    path_table : str,
    considerar_venta_incremental: bool = True,
) -> pd.DataFrame:
    """Genera el forecast para promociones desde SharePoint.

    Procesa archivos de entrada ubicados en la ruta indicada,
    obtiene datos desde BigQuery, ejecuta modelos OLS y construye
    las proyecciones de unidades y ventas para cada promoción.

    Parameters
    ----------
    gcp_project_id : str
        Proyecto de Google Cloud usado para credenciales y acceso.
    store_banner: str
        Formato al que pertenece el análisis.
    file_site : str
        Ruta del sitio SharePoint donde están los archivos.
    secret_name : str
        Nombre del secreto con las credenciales de SharePoint.
    gbq_client : Cliente
        Cliente de gcp
    path_table: str
        Ruta de la tabla con información materiales
    fecha_inicial_entrenamiento : str
        Fecha de inicio de entrenamiento
    considerar_venta_incremental : bool, optional
        Si es True, calcula baseline y variaciones incrementales.

    Returns
    -------
    pd.DataFrame
        DataFrame con el detalle de proyecciones generadas. Si no
        hay archivos pendientes por procesar, se retorna un
        DataFrame vacío.
    """


    # ---------------- Credenciales SharePoint ----------------
    sp_cred = secretmanager.getSecret(secret_name, project=gcp_project_id)

    # ---------------- Rutas de carpetas ----------------------
    inputs_dir = posixpath.join(file_site, 'Inputs')
    outputs_dir = posixpath.join(file_site, 'Outputs')


    # --------- Listar Inputs y Outputs -----------------------
    sp_folder_inputs = sp.SharePointFolder(
        **sp_cred,
        server_relative_folder=inputs_dir
    )
    sp_folder_outputs = sp.SharePointFolder(
        **sp_cred,
        server_relative_folder=outputs_dir
    )

    # Ahora: llamar fileList() directamente
    archivos_inputs = sp_folder_inputs.fileList()
    archivos_outputs = sp_folder_outputs.fileList()

    # Filtrar Excel reales (sin temporales) y por patrón
    patron_input = re.compile(
        r'^\d{4}_\d{2}_\d{2}_v\d+_input_proyeccion\.xlsx$', re.IGNORECASE)
    patron_output = re.compile(
        r'^\d{4}_\d{2}_\d{2}_v\d+_resultado_proyeccion\.xlsx$', re.IGNORECASE)

    inputs_validos = sorted([
        n for n in archivos_inputs
        if n.lower().endswith('.xlsx') and not n.startswith('~$') and patron_input.match(n)
    ])
    outputs_validos = set([  # noqa: C403
        n for n in archivos_outputs
        if n.lower().endswith('.xlsx') and not n.startswith('~$') and patron_output.match(n)
    ])

    # Mapear a clave base (YYYY_MM_DD_vN)
    def _clave_base(nombre: str) -> str:
        # ejemplo: 2025_11_05_v5_input_proyeccion.xlsx -> 2025_11_05_v5
        return nombre.split('_input_proyeccion.xlsx')[0]

    def _salida_de(clave: str) -> str:
        return f'{clave}_resultado_proyeccion.xlsx'

    # Encontrar pendientes: inputs cuyo output no exista
    pendientes = []
    for nombre_in in inputs_validos:
        clave = _clave_base(nombre_in)
        if _salida_de(clave) not in outputs_validos:
            pendientes.append(nombre_in)

    if not pendientes:
        print(archivos_inputs)
        print(archivos_outputs)
        print('No hay inputs pendientes por procesar (todos tienen output).')
        return pd.DataFrame()

    # Regla: si hay varios, procesar SOLO EL PRIMERO
    nombre_input = pendientes[0]
    clave = _clave_base(nombre_input)
    nombre_output = _salida_de(clave)

    # ---------------- Leer Excel desde SharePoint (input) -------------
    input_file_path = posixpath.join(inputs_dir, nombre_input)
    sharepoint_in = sp.SharePointFile(
        **sp_cred,
        server_relative_path=input_file_path
    )
    df_promos_importado = sharepoint_in.toFrame()

    # ---------------- Validaciones mínimas -------------------
    cols_requeridas = {'n_promocion', 'generar_proyeccion'}
    faltantes = cols_requeridas - set(df_promos_importado.columns)
    if faltantes:
        msg = f'Faltan columnas en el Excel: {sorted(faltantes)}'
        raise ValueError(msg)

    lista_promos = (
        df_promos_importado['n_promocion']
        .dropna().astype(str).unique().tolist()
    )
    if not lista_promos:
        msg = 'No se encontraron promociones en el archivo.'
        raise ValueError(msg)

    string_promos = ','.join(lista_promos)

    # ---------------- Consulta principal ---------------------

    df_final = generarDataFrame(categorias=string_promos,
                                gbq_client=gbq_client,
                                path_table= path_table,
                                usuario='pricing',
                                store_banner = store_banner)

    fecha_inicial_entrenamiento = df_final['p_date'].min().strftime('%Y-%m-%d')

    # ---------------- Consulta promos ---------------------
    query_promos_forecasting = SQL_QUERIES[
        'query_promos_forecasting'
    ].substitute(string_promos=string_promos)

    df_promos_forecasting = readBigQuery(
        query=query_promos_forecasting,
        user='sales_forecast',
        gbq_client=gbq_client
    )

    # ---------------- Limpieza de datos ----------------------
    df_promos_forecasting['precio_promocional'] = (
        df_promos_forecasting['precio_promocional'].astype(int)
    )
    df_promos_forecasting['precio_modal'] = (
        df_promos_forecasting['precio_modal'].astype(int)
    )

    df_promos_forecasting['fecha_inicio_de_promocion'] = pd.to_datetime(
        df_promos_forecasting['fecha_inicio_de_promocion']
    )
    df_promos_forecasting['fecha_fin_de_promocion'] = pd.to_datetime(
        df_promos_forecasting['fecha_fin_de_promocion']
    )

    df_promos_forecasting['clave_material'] = (
        df_promos_forecasting['material'].astype(str)
        + '_' + df_promos_forecasting['un_medida_venta']
    )

    df_promos_forecasting['p_date'] = df_promos_forecasting.apply(
        lambda row: pd.date_range(
            start=row['fecha_inicio_de_promocion'],
            end=row['fecha_fin_de_promocion']
        ),
        axis=1
    )

    df_expandido = df_promos_forecasting.explode('p_date')

    df_base = df_expandido[[
        'n_promocion', 'nombre_promocion', 'desc_categoria', 'material',
        'desc_material', 'un_medida_venta', 'clave_material',
        'desc_promocion', 'descripcion_evento_promocional', 'p_date',
        'precio_promocional', 'precio_modal', 'ean'
    ]].copy()

    def calcular_minimo(df_aux: pd.DataFrame) -> pd.Series:
        df_aux = df_aux.sort_values('precio_promocional')
        primera = df_aux.iloc[0]
        if primera['desc_promocion'] == 'COMBINACION NX$':
            if len(df_aux) > 1:
                segunda = df_aux.iloc[1]
                precio_promocional_minimo = (
                    primera['precio_promocional'] + segunda['precio_promocional']
                ) / 2
            else:
                precio_promocional_minimo = (
                    primera['precio_promocional'] + primera['precio_modal']
                ) / 2
        else:
            precio_promocional_minimo = primera['precio_promocional']

        return pd.Series({
            'precio_promocional_minimo': precio_promocional_minimo,
            'desc_promocion_minimo': primera['desc_promocion']
        })

    df_minimos = (
        df_base.groupby(['clave_material', 'p_date'])
        .apply(calcular_minimo)
        .reset_index()
    )

    df_resultado = df_base.merge(
        df_minimos, on=['clave_material', 'p_date'], how='left'
    )

    df_resultado = df_resultado[[
        'descripcion_evento_promocional', 'n_promocion', 'nombre_promocion',
        'desc_categoria', 'material', 'desc_material', 'un_medida_venta',
        'clave_material', 'p_date', 'desc_promocion', 'precio_modal',
        'precio_promocional', 'precio_promocional_minimo',
        'desc_promocion_minimo', 'ean'
    ]]

    df_promos_proyectables = df_promos_importado[
        df_promos_importado['generar_proyeccion'].str.lower() == 'si'
    ]
    promos_existentes = df_promos_proyectables['n_promocion'].unique()
    df_resultado = df_resultado[df_resultado['n_promocion'].isin(promos_existentes)]

    seleccion = df_resultado['desc_categoria'].unique()
    df_universo = df_final[df_final['category_description'].isin(seleccion)].copy()

    filas_resumen = []

    # ---------------- Bucle de proyección --------------------
    for _idx, promo in enumerate(promos_existentes, start=1):
        df_promo = df_resultado[df_resultado['n_promocion'] == promo]
        nombre_promo = str(df_promo['nombre_promocion'].iloc[0])

        logging.info('-----------------------------------------')
        logging.info(f'EVALUANDO LA PROMO {promo} - {nombre_promo}')
        logging.info('-----------------------------------------\n')

        claves = df_promo['clave_material'].unique()
        total_claves = len(claves)
        porcentaje_anterior = -10

        for j, clave in enumerate(claves, start=1):
            porcentaje = int(j / total_claves * 100)
            if porcentaje % 10 == 0 and porcentaje != porcentaje_anterior:
                porcentaje_anterior = porcentaje
                print('-----')
                print(f'Avance EANs: {porcentaje}%')
                print('-----')

            df_ean = df_promo[df_promo['clave_material'] == clave]
            material = int(df_ean['material'].iloc[0])
            sales_uom = str(df_ean['un_medida_venta'].iloc[0])
            fecha_inicio = df_ean['p_date'].min().strftime('%Y-%m-%d')
            fecha_fin = df_ean['p_date'].max().strftime('%Y-%m-%d')
            desc_mat = str(df_ean['desc_material'].iloc[0])
            categoria_prom = str(df_ean['desc_categoria'].iloc[0])
            ean_de_promo = (
                df_ean['ean'].dropna().astype(str).iloc[0]
                if 'ean' in df_ean.columns and not df_ean['ean'].dropna().empty
                else ''
            )

            df_prod = df_universo[
                (df_universo['material'] == material) &
                (df_universo['sales_uom'] == sales_uom)
            ]

            if df_prod.empty:
                filas_resumen.append({
                    'N° promoción': str(promo),
                    'Nombre promoción': nombre_promo,
                    'Categoría': categoria_prom,
                    'Descripcion': desc_mat,
                    'Material': material,
                    'UMV': sales_uom,
                    'EAN': ean_de_promo,
                    'R²': '-',
                    'Inicio Proy': fecha_inicio,
                    'Fin Proy': fecha_fin,
                    'Baseline UV': '-',
                    'UV Incremental Real': '-',
                    'UV Incremental Proy': '-',
                    'UV Real': '-',
                    'UV Proy': '-',
                    'Baseline Venta': '-',
                    'Venta Incremental Real': '-',
                    'Venta Incremental Proy': '-',
                    'Venta Real': '-',
                    'Venta Proy': '-',
                    'Comentarios': 'Sin ventas'
                })
                continue

            ean = str(df_prod['ean'].iloc[0])
            logging.info(f'Procesando: {ean} - {desc_mat}')

            if not verificacionFactibilidadModelo(df_prod):
                filas_resumen.append({
                    'N° promoción': str(promo),
                    'Nombre promoción': nombre_promo,
                    'Categoría': categoria_prom,
                    'Descripcion': desc_mat,
                    'Material': material,
                    'UMV': sales_uom,
                    'EAN': ean,
                    'R²': '-',
                    'Inicio Proy': fecha_inicio,
                    'Fin Proy': fecha_fin,
                    'Baseline UV': '-',
                    'UV Incremental Real': '-',
                    'UV Incremental Proy': '-',
                    'UV Real': '-',
                    'UV Proy': '-',
                    'Baseline Venta': '-',
                    'Venta Incremental Real': '-',
                    'Venta Incremental Proy': '-',
                    'Venta Real': '-',
                    'Venta Proy': '-',
                    'Comentarios': 'Sin historial suficiente'
                })
                continue

            fecha_limite = (pd.to_datetime(fecha_inicio) - pd.Timedelta(days=1))\
                .strftime('%Y-%m-%d')

            df_pre = obtenerDfDefaultProyeccion(
                df_final2=df_prod,
                ean=ean,
                fecha_inicial_proyeccion=fecha_inicio,
                fecha_final_proyeccion=fecha_fin,
                fecha_limite=fecha_limite,
                considerar_feriados=True
            )
            df_pre['apo'] = 1 if 'apo' in \
                df_ean['descripcion_evento_promocional'].iloc[0].lower() else 0

            df_prices = df_ean[['p_date', 'precio_promocional_minimo',
                                'precio_modal']].copy()
            df_prices['p_date'] = df_prices['p_date'].dt.strftime('%Y-%m-%d')
            df_pre = df_pre.merge(df_prices, on='p_date', how='left')
            df_pre['precio_promedio'] = df_pre['precio_promocional_minimo']\
                .fillna(df_pre['precio_promedio'])

            modelo = obtenerModeloOLS(
                df_prod, ean, fecha_limite, fecha_inicial_entrenamiento,
                considerar_feriados=True
            )


            try:
                r2_modelo = round(float(modelo.rsquared_adj), 2)
            except (AttributeError, TypeError, ValueError):
                r2_modelo = '-'

            elasticidad = (
                float(modelo.params['log_precio'])
                if hasattr(modelo, 'params') and 'log_precio' in modelo.params.index
                else None
            )
            if elasticidad is not None and elasticidad > 0:
                filas_resumen.append({
                    'N° promoción': str(promo),
                    'Nombre promoción': nombre_promo,
                    'Categoría': categoria_prom,
                    'Descripcion': desc_mat,
                    'Material': material,
                    'UMV': sales_uom,
                    'EAN': ean,
                    'R²': r2_modelo,
                    'Inicio Proy': fecha_inicio,
                    'Fin Proy': fecha_fin,
                    'Baseline UV': '-',
                    'UV Incremental Real': '-',
                    'UV Incremental Proy': '-',
                    'UV Real': '-',
                    'UV Proy': '-',
                    'Baseline Venta': '-',
                    'Venta Incremental Real': '-',
                    'Venta Incremental Proy': '-',
                    'Venta Real': '-',
                    'Venta Proy': '-',
                    'Comentarios': 'Elasticidad positiva'
                })
                continue

            def calculo_proyeccion(
                df_proj: pd.DataFrame,
                modelo_actual,
                df_hist_source: pd.DataFrame
            ) -> dict:
                df_pred = obtenerProyeccionV2(df_proj, modelo_actual)
                dias = df_pred['p_date'].unique()
                df_hist = df_hist_source[df_hist_source['p_date'].isin(dias)]

                uv_real = df_hist['cantidad_total'].sum()
                uv_proy = df_pred['cantidad_total_predicha'].sum()
                venta_real = df_hist['ventas_totales_producto'].sum()
                venta_proy = df_pred['ventas_totales_producto_predicha'].sum()

                contiene_art = (df_pred['cantidad_total'] == -1).any()

                return {
                    'uv_real': uv_real,
                    'uv_proy': uv_proy,
                    'venta_real': venta_real,
                    'venta_proy': venta_proy,
                    'contiene_artificial': bool(contiene_art)
                }

            # Proyección con precio promocional
            res_promo = calculo_proyeccion(df_pre, modelo, df_prod)

            if considerar_venta_incremental:
                df_base = df_pre.copy()
                df_base['precio_promedio'] = df_base['precio_modal']
                res_base = calculo_proyeccion(df_base, modelo, df_prod)
                baseline_uv = res_base['uv_proy']
                baseline_venta = res_base['venta_proy']
            else:
                res_base = None
                baseline_uv = '-'
                baseline_venta = '-'

            if res_promo['contiene_artificial']:
                uv_real_str = '-'
                venta_real_str = '-'
                uv_inc_real = '-'
                venta_inc_real = '-'
            else:
                uv_real_str = res_promo['uv_real']
                venta_real_str = res_promo['venta_real']
                if considerar_venta_incremental and baseline_uv != '-' \
                        and baseline_venta != '-':
                    uv_inc_real = res_promo['uv_real'] - baseline_uv
                    venta_inc_real = res_promo['venta_real'] - baseline_venta
                else:
                    uv_inc_real = '-'
                    venta_inc_real = '-'

            if considerar_venta_incremental and baseline_uv != '-' \
                    and baseline_venta != '-':
                uv_inc_proy = res_promo['uv_proy'] - baseline_uv
                venta_inc_proy = res_promo['venta_proy'] - baseline_venta
            else:
                uv_inc_proy = '-'
                venta_inc_proy = '-'

            filas_resumen.append({
                'N° promoción': str(promo),
                'Nombre promoción': nombre_promo,
                'Categoría': categoria_prom,
                'Descripcion': desc_mat,
                'Material': material,
                'UMV': sales_uom,
                'EAN': ean,
                'R²': r2_modelo,
                'Inicio Proy': fecha_inicio,
                'Fin Proy': fecha_fin,
                'Baseline UV': baseline_uv,
                'UV Incremental Real': uv_inc_real,
                'UV Incremental Proy': uv_inc_proy,
                'UV Real': uv_real_str,
                'UV Proy': res_promo['uv_proy'],
                'Baseline Venta': baseline_venta,
                'Venta Incremental Real': venta_inc_real,
                'Venta Incremental Proy': venta_inc_proy,
                'Venta Real': venta_real_str,
                'Venta Proy': res_promo['venta_proy'],
                'Comentarios': ''
            })

    df_resumen_proyeccion = pd.DataFrame(filas_resumen)

    # ----- Exportar y subir directamente a SharePoint -----
    output_remote_path = posixpath.join(outputs_dir, nombre_output)

    output_buffer = io.BytesIO()
    with pd.ExcelWriter(output_buffer, engine='xlsxwriter') as writer:
        df_resumen_proyeccion.to_excel(
            writer, index=False, sheet_name='desglose_proyeccion'
        )

        workbook = writer.book
        worksheet = writer.sheets['desglose_proyeccion']
        worksheet.freeze_panes(1, 0)

        formato_centrado = workbook.add_format(
            {'align': 'center', 'valign': 'vcenter'})
        formato_num = workbook.add_format({
            'num_format': '#,##0;[Red]-#,##0',
            'align': 'center',
            'valign': 'vcenter'
        })
        columnas_largas = ['Nombre promoción', 'Categoría']
        columnas_muy_largas = ['Descripcion']
        columnas_numericas = [
            'Baseline UV', 'UV Incremental Real', 'UV Incremental Proy',
            'UV Real', 'UV Proy', 'Baseline Venta', 'Venta Incremental Real',
            'Venta Incremental Proy', 'Venta Real', 'Venta Proy'
        ]

        for col_idx, col_name in enumerate(df_resumen_proyeccion.columns):
            if col_name in columnas_numericas:
                worksheet.set_column(col_idx, col_idx, 22, formato_num)
            elif col_name in columnas_largas:
                worksheet.set_column(col_idx, col_idx, 40, formato_centrado)
            elif col_name in columnas_muy_largas:
                worksheet.set_column(col_idx, col_idx, 50, formato_centrado)
            else:
                worksheet.set_column(col_idx, col_idx, 20, formato_centrado)

    output_buffer.seek(0)

    sp_output = sp.SharePointFile(
        **sp_cred,
        server_relative_path=output_remote_path
    )
    sp_output.upload(content=output_buffer)

    print(f'Archivo subido correctamente a SharePoint: {output_remote_path}')
    return df_resumen_proyeccion

def generarDataFrame(store_banner: str,
                     categorias: str,
                     gbq_client: Client,
                    path_table: str,
                    usuario: str = 'pricing'):

    query_datos = SQL_QUERIES['query_data_procesada'].substitute(store_banner = store_banner,
                                                                path_table = path_table,
                                                                categorias = categorias)

    df_final = readBigQuery(
            query=query_datos,
            user=usuario,
            gbq_client=gbq_client
    )

    logging.info('Query principal lista')
    #----------------------------------------------------------------------
    # ENDREGION

    # REGION: Se limpian y configuran los datos
    #----------------------------------------------------------------------

    # Columnas se dejan en minuscula
    df_final.columns = df_final.columns.str.lower()

    # Se transforma a store_banner fecha
    df_final['p_date'] = pd.to_datetime(df_final['p_date'], format='%Y-%m-%d')


    df_final['p_year'] = df_final['p_date'].dt.year.astype(str)
    logging.info('Agregados p_year')

    df_final = pd.concat([df_final,
                        pd.get_dummies(df_final['p_year'], prefix='',
                                        prefix_sep='').astype(int)], axis=1)
    logging.info('Se agregan DUMMY del año')

    df_final['ean'] = df_final['ean'].astype(str)
    df_final['product_description_ean'] = df_final['product_description'] + ' - ' + df_final['ean']
    df_final['multiplicador_x05'] = df_final['multiplicador_x05'].astype(int)

    #~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    # ENDREGION


    # REGION: Fin de semana
    #----------------------------------------------------------------------

    # Crear la columna 'fds' basado en si es viernes, sábado o domingo
    df_final['fds'] = df_final['p_date'].dt.dayofweek.apply(
                                            lambda x: 1 if x in [4, 5, 6] else 0)

    # Crear la columna 'l_m_w' basado en si es lunes, martes o miercoles
    df_final['l_m_w'] = df_final['p_date'].dt.dayofweek.apply(
                                            lambda x: 1 if x in [0, 1, 2] else 0)

    # Crear la columna 'lunes' para indicar si el día es lunes
    df_final['lunes'] = df_final['p_date'].dt.dayofweek.apply(
                                                    lambda x: 1 if x == 0 else 0)

    # Crear la columna 'martes' para indicar si el día es martes
    df_final['martes'] = df_final['p_date'].dt.dayofweek.apply(
                                                    lambda x: 1 if x == 1 else 0)

    # Crear la columna 'miercoles' para indicar si el día es miércoles
    df_final['miercoles'] = df_final['p_date'].dt.dayofweek.apply(
                                                    lambda x: 1 if x == 2 else 0)

    # Crear la columna 'jueves' para indicar si el día es jueves
    df_final['jueves'] = df_final['p_date'].dt.dayofweek.apply(
                                                    lambda x: 1 if x == 3 else 0)

    # Crear la columna 'viernes' para indicar si el día es viernes
    df_final['viernes'] = df_final['p_date'].dt.dayofweek.apply(
                                                    lambda x: 1 if x == 4 else 0)

    # Crear la columna 'sabado' para indicar si el día es sábado
    df_final['sabado'] = df_final['p_date'].dt.dayofweek.apply(
                                                    lambda x: 1 if x == 5 else 0)

    # Crear la columna 'domingo' para indicar si el día es domingo
    df_final['domingo'] = df_final['p_date'].dt.dayofweek.apply(
                                                    lambda x: 1 if x == 6 else 0)


    logging.info(
        'Se agregan DUMMY de dia de la semana (l_m_w, jueves, viernes, sábado o domingo)')

    #~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    # ENDREGION




    # REGION: Agregar DUMMY de meses del año
    #----------------------------------------------------------------------

    # Extraer el número de mes desde 'p_month'
    df_final['mes'] = df_final['p_month'].astype(str).str[4:].astype(int)

    # Crear columnas dummy para cada mes
    for i, nombre_mes in enumerate(['enero', 'febrero', 'marzo',
                                    'abril', 'mayo', 'junio',
                                    'julio', 'agosto', 'septiembre',
                                    'octubre', 'noviembre', 'diciembre'],
                                    start=1):
        df_final[nombre_mes] = (df_final['mes'] == i).astype(int)

    # (Opcional) eliminar la columna auxiliar 'mes'
    df_final = df_final.drop(columns='mes')

    logging.info('Se agregan DUMMY meses del año')
    #~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    # ENDREGION



    # REGION: Agregar FERIADOS
    #----------------------------------------------------------------------

    # Se deja como funcion ya que se usa el mismo codigo mas adelante para
    # agregar feriados a las proyecciones


    df_final = agregarFeriados(df_final)
    logging.info('Se agregan los feriados')

    #~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    # ENDREGION


    # REGION: Reordenación de dataframe principal
    #----------------------------------------------------------------------

    # Lista de columnas deseadas
    columnas_deseadas = [
        'store_banner','product_description_ean',
        'category_description', 'sub_category_description', 'material', 'product_description',
        'ean', 'sales_uom', 'sales_unit', 'p_date', 'p_week', 'p_month',
        'ventas_totales_producto', 'cantidad_total', 'precio_promedio',
        '2023', '2024', '2025',
        'l_m_w', 'lunes', 'martes', 'miercoles', 'jueves', 'viernes', 'sabado', 'domingo',
        'invierno', 'otono', 'primavera', 'verano',
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


    # Filtrar solo las columnas que existen en df_final
    columnas_existentes = [col for col in columnas_deseadas if col in df_final.columns]

    # Crear el nuevo dataframe
    df_final = df_final[columnas_existentes]

    logging.info('Se limpia y reordena por ventas el dataframe')

    #~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    # ENDREGION

    # REGION: Ordenar EAN por ventas
    #----------------------------------------------------------------------

    # Paso 1: Calcular la suma de ventas por material
    suma_ventas_por_material = df_final.groupby('ean')['ventas_totales_producto'].sum()

    # Paso 2: Crear una columna temporal con la suma de ventas y ordenar
    df_final['suma_ventas'] = df_final['ean'].map(suma_ventas_por_material)
    df_final = df_final.sort_values(by='suma_ventas', ascending=False)

    #Eliminar la columna temporal si no la necesitas más
    df_final = df_final.drop('suma_ventas', axis=1)


    logging.info('Se ordena por ventas')

    #~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    # ENDREGION

    # REGION: Se asignan tipos correctos en los campos
    #----------------------------------------------------------------------

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
    logging.info('Se asigna correctamente el tipo de cada campo')

    #~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    # ENDREGION



# -------------------------------------------------------------------------
# Main function
# -------------------------------------------------------------------------
def main() -> None:  # noqa: D103


    # Parse input variables
    args = vars(parser.parse_args())
    execution_date: str = args['execution_date']
    proyecto: str = args['project_id']  # noqa: F841
    store_banner:str = args['store_banner']
    logging.info(f'execution_date: {execution_date}')
    logging.info(f'proyecto: {proyecto}')


    # Set gbq client for all subsequent queries
    gbq_client = Client()


    # REGION: Inputs del proceso
    #----------------------------------------------------------------------
    esquema = 'TMP'
    tabla = 'TMP_REGRESSION_PROCESSED_DATA_FORECAST'
    path_table = f'{proyecto}.{esquema}.{tabla}'

    #~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    # ENDREGION


    # REGION: Llamada de función principal
    #--------------------------------------------------------------------------
    # Ruta
    file_site = '/sites/BigDatayAdvancedAnalytics/Documentos compartidos/'
    file_site += 'Pricing/Forecast Promociones'

    # Llamada
    _df_resultado = calcular_forecast(
        gcp_project_id = 'cl-bigdata-analytics-preprod',
        file_site = file_site,
        path_table = path_table,
        store_banner = store_banner,
        considerar_venta_incremental = True,
        secret_name = 'bdaa_sharepoint_credentials',  # noqa: S106
        gbq_client = gbq_client,
        )

    #~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    # ENDREGION

if __name__ == '__main__':
    main()
