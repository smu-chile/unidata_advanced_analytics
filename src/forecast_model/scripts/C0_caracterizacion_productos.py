from __future__ import annotations

import os
import sys
import logging
import argparse
from logging import config

import numpy as np
import pandas as pd

# Pip
from google.cloud.bigquery import Client


directorio_actual = os.path.abspath(os.curdir)

while directorio_actual != os.path.sep:
    try:
        sys.path.append(directorio_actual)
        from credentials import credentials  # noqa: F401
        break  # Si la importación es exitosa, sale del bucle
    except ModuleNotFoundError:
        sys.path.pop()  # Remueve el directorio que no contenía el módulo
        directorio_actual = os.path.dirname(directorio_actual)  # Retrocede


import common.gcp_extended.secretsmanager as secretmanager  # noqa: E402
from common.constants import LOGGING_CONFIG  # noqa: E402
from common.databases.queries import QueryDict  # noqa: E402
from common.gcp_extended.bigquery import (  # noqa: E402
    uploadFrame,  # noqa: F401
    readBigQuery,
    deleteFromTable,  # noqa: F401
    createTableAsSelect,  # noqa: F401
)


#######################################################
##########---------- 0. Auxiliares ----------##########
#######################################################


##########           0.1 Queries

# Query para obtener la data procesada de forecast para todos los
# productos disponibles.

QUERY_FORECAST= QueryDict({
    'query_data_procesada':
    """

    SELECT * EXCEPT(
        PRIMER_DIA_MES,
        ULTIMO_DIA_MES,
        P_WEEK,
        P_MONTH,
        VARIACION_TOP1_SUSTITUTO,
        VARIACION_TOP3_SUSTITUTOS
        )

    FROM `${path_table}`
    Where store_banner = 'Unimarc'
    """})



SQL_QUERIES_ALL_PROMOS = QueryDict({
    'query_promos_forecasting':
"""
WITH productos_objetivo AS (
    -- 1. Identificar los productos que tienen historial en la tabla forecast.
    SELECT DISTINCT
        ean
    FROM `${path_table}`
    Where store_banner = 'Unimarc'
),

historial_promocional AS (
    -- 2. Recuperar todo el historial promocional de los EAN objetivo.
    -- En esta etapa no se vuelve a filtrar por string_promos.
    SELECT DISTINCT
        promo.n_promocion,
        promo.nombre_promocion,
        promo.descripcion_evento_promocional,
        promo.descripcion_mecanica,
        promo.desc_categoria,
        promo.material,
        promo.desc_material,
        promo.un_medida_venta,
        promo.ean,

        SAFE_CAST(
            promo.precio_modal AS FLOAT64
        ) AS precio_modal,

        SAFE_CAST(
            promo.precio_modal_total AS FLOAT64
        ) AS precio_modal_total,

        SAFE_CAST(
            promo.precio_promocional AS FLOAT64
        ) AS precio_promocional,

        SAFE_CAST(
            promo.precio_total_promocional AS FLOAT64
        ) AS precio_total_promocional,

        promo.ahorro,
        promo.ahorro_total,
        promo.desc_promocion,
        promo.cantidad_n,
        promo.cantidad_m,

        DATE(
            promo.fecha_inicio_de_promocion
        ) AS fecha_inicio_de_promocion,

        DATE(
            promo.fecha_fin_de_promocion
        ) AS fecha_fin_de_promocion,

        promo.porcentaje_cobertura,
        promo.porcentaje_descuento,
        promo.organizacion_ventas,
        promo.canal_distribucion

    FROM `cl-bigdata-analytics-preprod.CDA_VISTAS.VW_FACT_WORKFLOW` AS promo

    INNER JOIN productos_objetivo AS producto
        ON promo.ean = producto.ean

    WHERE promo.organizacion_ventas = '1000'
      AND promo.canal_distribucion = '10'
      AND promo.registro_valido = 'X'

      -- Incluye promociones iniciadas antes de 2024
      -- que continúan vigentes durante 2024.
      AND DATE(
          promo.fecha_fin_de_promocion
      ) >= DATE '2024-01-01'

      -- Excluir períodos promocionales inconsistentes.
      AND DATE(
          promo.fecha_fin_de_promocion
      ) >= DATE(
          promo.fecha_inicio_de_promocion
      )
),

promociones_diarias AS (
    -- 3. Expandir cada promoción a una fila por día de vigencia.
    SELECT
        promo.*,
        p_date
    FROM historial_promocional AS promo

    CROSS JOIN UNNEST(
        GENERATE_DATE_ARRAY(
            GREATEST(
                promo.fecha_inicio_de_promocion,
                DATE '2024-01-01'
            ),
            promo.fecha_fin_de_promocion
        )
    ) AS p_date
),

promociones_con_precio_valido AS (
    -- 4. Conservar solo precios promocionales válidos:
    -- no nulos, finitos y estrictamente mayores que cero.
    SELECT *
    FROM promociones_diarias
    WHERE precio_promocional IS NOT NULL
      AND NOT IS_NAN(precio_promocional)
      AND NOT IS_INF(precio_promocional)
      AND precio_promocional > 0
),

precio_promocional_diario AS (
    -- 5. Ordenar las promociones activas de cada EAN y día
    -- desde el menor precio promocional válido.
    SELECT
        *,

        ROW_NUMBER() OVER (
            PARTITION BY
                ean,
                p_date
            ORDER BY
                precio_promocional ASC,
                n_promocion ASC,
                material ASC,
                nombre_promocion ASC
        ) AS orden_precio_diario,

        COUNT(*) OVER (
            PARTITION BY
                ean,
                p_date
        ) AS cantidad_promociones_activas

    FROM promociones_con_precio_valido
)

-- 6. Conservar una única promoción por EAN y día.
SELECT
    * EXCEPT (
        orden_precio_diario
    ),

    1 AS FLAG_PROMO,

    precio_promocional
        AS precio_promocional_minimo,

    desc_promocion
        AS desc_promocion_minimo

FROM precio_promocional_diario

WHERE orden_precio_diario = 1

ORDER BY
    ean,
    p_date
"""
})


SQL_QUERIES_ALL_PROMOS_COMBINACION = QueryDict({
    'query_promos_forecasting_combinacion':
"""
WITH productos_objetivo AS (
    -- 1. Identificar los productos que tienen historial en la tabla forecast.
    SELECT DISTINCT
        ean
    FROM `${path_table}`
    Where store_banner = 'Unimarc'
),

historial_promocional AS (
    -- 2. Recuperar todo el historial promocional de los EAN objetivo.
    -- En esta etapa no se vuelve a filtrar por string_promos.
    SELECT DISTINCT
        promo.n_promocion,
        promo.nombre_promocion,
        promo.descripcion_evento_promocional,
        promo.descripcion_mecanica,
        promo.desc_categoria,
        promo.material,
        promo.desc_material,
        promo.un_medida_venta,
        promo.ean,

        SAFE_CAST(
            promo.precio_modal AS FLOAT64
        ) AS precio_modal,

        SAFE_CAST(
            promo.precio_modal_total AS FLOAT64
        ) AS precio_modal_total,

        SAFE_CAST(
            promo.precio_promocional AS FLOAT64
        ) AS precio_promocional,

        SAFE_CAST(
            promo.precio_total_promocional AS FLOAT64
        ) AS precio_total_promocional,

        promo.ahorro,
        promo.ahorro_total,
        promo.desc_promocion,
        promo.cantidad_n,
        promo.cantidad_m,

        DATE(
            promo.fecha_inicio_de_promocion
        ) AS fecha_inicio_de_promocion,

        DATE(
            promo.fecha_fin_de_promocion
        ) AS fecha_fin_de_promocion,

        promo.porcentaje_cobertura,
        promo.porcentaje_descuento,
        promo.organizacion_ventas,
        promo.canal_distribucion

    FROM `cl-bigdata-analytics-preprod.CDA_VISTAS.VW_FACT_WORKFLOW` AS promo

    INNER JOIN productos_objetivo AS producto
        ON promo.ean = producto.ean

    WHERE promo.organizacion_ventas = '1000'
      AND promo.canal_distribucion = '10'
      AND promo.registro_valido = 'X'

      -- Incluye promociones iniciadas antes de 2024
      -- que continúan vigentes durante 2024.
      AND DATE(
          promo.fecha_fin_de_promocion
      ) >= DATE '2024-01-01'

      -- Excluir períodos promocionales inconsistentes.
      AND DATE(
          promo.fecha_fin_de_promocion
      ) >= DATE(
          promo.fecha_inicio_de_promocion
      )
),

promociones_diarias AS (
    -- 3. Expandir cada promoción a una fila por día de vigencia.
    SELECT
        promo.*,
        p_date
    FROM historial_promocional AS promo

    CROSS JOIN UNNEST(
        GENERATE_DATE_ARRAY(
            GREATEST(
                promo.fecha_inicio_de_promocion,
                DATE '2024-01-01'
            ),
            promo.fecha_fin_de_promocion
        )
    ) AS p_date
),

promociones_con_precio_valido AS (
    -- 4. Conservar solo precios promocionales válidos:
    -- no nulos, finitos y estrictamente mayores que cero.
    SELECT *
    FROM promociones_diarias
    WHERE precio_promocional IS NOT NULL
      AND NOT IS_NAN(precio_promocional)
      AND NOT IS_INF(precio_promocional)
      AND precio_promocional > 0
),

promociones_ordenadas AS (
    -- 5. Ordenar las promociones activas de cada EAN y día
    -- desde el menor precio promocional válido.
    SELECT
        *,

        ROW_NUMBER() OVER (
            PARTITION BY
                ean,
                p_date
            ORDER BY
                precio_promocional ASC,
                n_promocion ASC,
                material ASC,
                nombre_promocion ASC
        ) AS orden_precio_diario,

        COUNT(*) OVER (
            PARTITION BY
                ean,
                p_date
        ) AS cantidad_promociones_activas,

        LEAD(precio_promocional) OVER (
            PARTITION BY
                ean,
                p_date
            ORDER BY
                precio_promocional ASC,
                n_promocion ASC,
                material ASC,
                nombre_promocion ASC
        ) AS segundo_precio_promocional

    FROM promociones_con_precio_valido
),

precio_diario_calculado AS (
    -- 6. Calcular un único precio promocional por EAN y día.
    SELECT
        * EXCEPT (
            orden_precio_diario,
            segundo_precio_promocional
        ),

        1 AS FLAG_PROMO,

        CASE
            -- Caso general: utilizar directamente el menor
            -- precio promocional válido.
            WHEN desc_promocion != 'COMBINACION NX$$'
                 OR desc_promocion IS NULL
                THEN precio_promocional

            -- Tratamiento especial con más de una promoción activa:
            -- promedio de los dos precios promocionales más bajos.
            WHEN cantidad_promociones_activas > 1
                THEN (
                    precio_promocional
                    + segundo_precio_promocional
                ) / 2.0

            -- Tratamiento especial con una única promoción activa:
            -- promedio del precio promocional y el precio modal.
            WHEN precio_modal IS NOT NULL
                 AND NOT IS_NAN(precio_modal)
                 AND NOT IS_INF(precio_modal)
                 AND precio_modal > 0
                THEN (
                    precio_promocional
                    + precio_modal
                ) / 2.0

            -- No se puede calcular el precio cuando se necesita
            -- el precio modal y este no es válido.
            ELSE NULL
        END AS precio_promocional_minimo,

        desc_promocion
            AS desc_promocion_minimo,

        CASE
            WHEN desc_promocion != 'COMBINACION NX$$'
                 OR desc_promocion IS NULL
                THEN 'PRECIO_PROMOCIONAL_MINIMO_DIRECTO'

            WHEN cantidad_promociones_activas > 1
                THEN 'PROMEDIO_DOS_PRECIOS_PROMOCIONALES_MAS_BAJOS'

            WHEN precio_modal IS NOT NULL
                 AND NOT IS_NAN(precio_modal)
                 AND NOT IS_INF(precio_modal)
                 AND precio_modal > 0
                THEN 'PROMEDIO_PRECIO_PROMOCIONAL_Y_PRECIO_MODAL'

            ELSE 'SIN_PRECIO_DIARIO_VALIDO'
        END AS regla_calculo_precio_promocional

    FROM promociones_ordenadas

    -- La primera fila representa la promoción con el menor precio.
    WHERE orden_precio_diario = 1
)

-- 7. Conservar solamente precios diarios calculados válidos.
SELECT *
FROM precio_diario_calculado

WHERE precio_promocional_minimo IS NOT NULL
  AND NOT IS_NAN(precio_promocional_minimo)
  AND NOT IS_INF(precio_promocional_minimo)
  AND precio_promocional_minimo > 0

ORDER BY
    ean,
    p_date
"""
})


##########          0.2 Config

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

usuario = 'pricing'
MIN_DIAS = 150


##########---------- 1. Funciones principales ----------##########

#####----- 1.1 Creación de variables temporales frugales
def agregar_features_frugales(df_final: pd.DataFrame) -> pd.DataFrame:
    """Variables temporales optimizadas (Fourier y Tendencia Continua)

    para evitar la multicolinealidad y sobre-parametrización.
    """
    df_final = df_final.copy()

    # 1. Asegurar formato datetime en P_DATE
    if not pd.api.types.is_datetime64_any_dtype(df_final['P_DATE']):
        df_final['P_DATE'] = pd.to_datetime(df_final['P_DATE'])

    # 2. Tendencia lineal continua (Días transcurridos desde la fecha mínima global)  # noqa: W505
    p_date_min = df_final['P_DATE'].min()
    df_final['tendencia_lineal'] = (
        df_final['P_DATE'] - p_date_min
    ).dt.days.astype(np.int32)

    # 3. Componentes de Fourier (Estacionalidad anual suave)
    dias_del_ano = df_final['P_DATE'].dt.dayofyear.values  # noqa: PD011
    angulos = 2 * np.pi * dias_del_ano / 365.25

    df_final['sin_anual'] = np.sin(angulos).astype(np.float32)
    df_final['cos_anual'] = np.cos(angulos).astype(np.float32)

    return df_final

def procesar_feriados_chile(df_final: pd.DataFrame) -> pd.DataFrame:
    """Reconstruye de forma determinista y frugal las dummies agregadas de
    feriados para Chile: 'Feriado' y 'Pre_Feriado', omitiendo los feriados
    irrenunciables sin registros de venta.
    """
    df_final = df_final.copy()

    # Asegurar tipo datetime
    if not pd.api.types.is_datetime64_any_dtype(df_final['P_DATE']):
        df_final['P_DATE'] = pd.to_datetime(df_final['P_DATE'])

    # 1. Definir Feriados Fijos Generales (Mes, Día), incluyendo los irrenunciables  # noqa: W505
    feriados_fijos = {
        (1, 1),
        (5, 1),
        (5, 21),
        (6, 20),
        (6, 29),
        (7, 16),
        (8, 15),
        (9, 18),
        (9, 19),
        (10, 12),
        (10, 31),
        (11, 1),
        (12, 8),
        (12, 25),
    }

    # 2. Feriados Religiosos Móviles
    feriados_moviles = {
        2024: [pd.Timestamp('2024-03-29'), pd.Timestamp('2024-03-30')],
        2025: [pd.Timestamp('2025-04-18'), pd.Timestamp('2025-04-19')],
        2026: [pd.Timestamp('2026-04-03'), pd.Timestamp('2026-04-04')],
    }

    # Extraer fechas únicas presentes para evaluación de calendario
    fechas_unicas = df_final['P_DATE'].drop_duplicates()

    # Función para determinar si una fecha cualquiera en el calendario es
    #  feriado
    def es_fecha_feriado(dt):
        año = dt.year
        if año in feriados_moviles and dt in feriados_moviles[año]:
            return True
        return (dt.month, dt.day) in feriados_fijos

    # Evaluar si la fecha es Feriado
    es_fer = fechas_unicas.apply(es_fecha_feriado)

    # Evaluar si la fecha SIGUIENTE (t + 1 día) es Feriado -> Pre_Feriado
    es_pre = (fechas_unicas + pd.Timedelta(days=1)).apply(es_fecha_feriado)

    # DataFrame auxiliar de mapeo
    df_map = pd.DataFrame(
        {
            'P_DATE': fechas_unicas,
            'FERIADO': es_fer.astype(np.int8),
            'PRE_FERIADO': es_pre.astype(np.int8),
        }
    )

    # Limpiar columnas previas si existían
    cols_a_borrar = [
        c
        for c in ['FERIADO', 'PRE_FERIADO', 'FERIADO_IRRENUNCIABLE']
        if c in df_final.columns
    ]
    if cols_a_borrar:
        df_final = df_final.drop(columns=cols_a_borrar)

    # Unir con el dataset principal
    df_final = df_final.merge(df_map, on='P_DATE', how='left')

    return df_final

#####----- 1.2. Función Merge


def merge_historial_ventas_con_promociones(
    df_ventas: pd.DataFrame,
    df_promos_query_basic: pd.DataFrame,
    df_promos_query_tratada: pd.DataFrame,
) -> pd.DataFrame:
    """Adjunta información promocional básica y tratada al historial diario
    de ventas.

    El historial de ventas define completamente el universo del resultado:
    se conservan todos sus EAN, fechas y filas. La información promocional
    se incorpora únicamente cuando existe coincidencia exacta entre:

    - `EAN` y `P_DATE` en el historial de ventas;
    - `ean` y `p_date` en los historiales promocionales.

    Cuando no existe información promocional para un producto o una fecha,
    las columnas promocionales quedan nulas.

    Las columnas provenientes del historial básico reciben el sufijo `_B`
    y las provenientes del historial tratado reciben el sufijo `_T`.

    Parameters
    ----------
    df_ventas : pd.DataFrame
        Historial diario de ventas con las claves `EAN` y `P_DATE`.

    df_promos_query_basic : pd.DataFrame
        Historial promocional básico con las claves `ean` y `p_date`.

    df_promos_query_tratada : pd.DataFrame
        Historial promocional tratado con las claves `ean` y `p_date`.

    Returns
    -------
    pd.DataFrame
        Historial diario de ventas enriquecido. Conserva las claves finales
        como `EAN` y `P_DATE`.
    """
    logging.info(
        'Iniciando incorporación de promociones al historial de ventas.'
    )

    claves_ventas = ['EAN', 'P_DATE']
    claves_promos = ['ean', 'p_date']

    columnas_promocionales = [
        'precio_modal',
        'precio_promocional',
        'precio_promocional_minimo',
        'n_promocion',
        'nombre_promocion',
        'descripcion_evento_promocional',
        'porcentaje_descuento',
        'FLAG_PROMO'
    ]

    columnas_requeridas_promos = (
        claves_promos + columnas_promocionales
    )

    # Validación de columnas.
    columnas_faltantes_ventas = [
        columna
        for columna in claves_ventas
        if columna not in df_ventas.columns
    ]

    if columnas_faltantes_ventas:
        msg = (
            '`df_ventas` no contiene las columnas requeridas: '
            f'{columnas_faltantes_ventas}'
        )
        raise KeyError(
            msg
        )

    for nombre_df, df_promos in [
        ('df_promos_query_basic', df_promos_query_basic),
        ('df_promos_query_tratada', df_promos_query_tratada),
    ]:
        columnas_faltantes = [
            columna
            for columna in columnas_requeridas_promos
            if columna not in df_promos.columns
        ]

        if columnas_faltantes:
            msg_0 = (
                f'`{nombre_df}` no contiene las columnas requeridas: '
                f'{columnas_faltantes}'
            )
            raise KeyError(
                msg_0
            )

    # Copias para preservar los DataFrames originales.
    df_resultado = df_ventas.copy()

    df_basic = (
        df_promos_query_basic[columnas_requeridas_promos]
        .copy()
        .rename(
            columns={
                'ean': 'EAN',
                'p_date': 'P_DATE',
            }
        )
    )

    df_tratada = (
        df_promos_query_tratada[columnas_requeridas_promos]
        .copy()
        .rename(
            columns={
                'ean': 'EAN',
                'p_date': 'P_DATE',
            }
        )
    )

    # Homologación de fechas.
    df_resultado['P_DATE'] = pd.to_datetime(
        df_resultado['P_DATE'],
        errors='raise',
    ).dt.normalize()

    df_basic['P_DATE'] = pd.to_datetime(
        df_basic['P_DATE'],
        errors='raise',
    ).dt.normalize()

    df_tratada['P_DATE'] = pd.to_datetime(
        df_tratada['P_DATE'],
        errors='raise',
    ).dt.normalize()

    # Homologación del EAN como texto.
    for df_aux in [df_resultado, df_basic, df_tratada]:
        df_aux['EAN'] = (
            df_aux['EAN']
            .astype('string')
            .str.strip()
            .str.replace(r'\.0$', '', regex=True)
        )

    # Las fuentes promocionales deben contener una única fila por EAN-día.
    if df_basic.duplicated(['EAN', 'P_DATE']).any():
        msg_1 = (
            '`df_promos_query_basic` contiene más de una fila para alguna '
            'combinación `ean`–`p_date`. El merge multiplicaría las filas '  # noqa: RUF001
            'del historial de ventas.'
        )
        raise ValueError(
            msg_1
        )

    if df_tratada.duplicated(['EAN', 'P_DATE']).any():
        msg_2 = (
            '`df_promos_query_tratada` contiene más de una fila para alguna '
            'combinación `ean`–`p_date`. El merge multiplicaría las filas '  # noqa: RUF001
            'del historial de ventas.'
        )
        raise ValueError(
            msg_2
        )

    # Sufijos para distinguir ambos tratamientos promocionales.
    df_basic = df_basic.rename(
        columns={
            columna: f'{columna}_B'
            for columna in columnas_promocionales
        }
    )

    df_tratada = df_tratada.rename(
        columns={
            columna: f'{columna}_T'
            for columna in columnas_promocionales
        }
    )

    n_filas_originales = len(df_resultado)

    # El historial de ventas permanece a la izquierda en ambos cruces.
    df_resultado = df_resultado.merge(
        df_basic,
        on=['EAN', 'P_DATE'],
        how='left',
        validate='many_to_one',
    )

    df_resultado = df_resultado.merge(
        df_tratada,
        on=['EAN', 'P_DATE'],
        how='left',
        validate='many_to_one',
    )

    if len(df_resultado) != n_filas_originales:
        msg_3 = (
            'El merge alteró la cantidad de filas del historial de ventas. '
            f'Antes: {n_filas_originales:,}; '
            f'después: {len(df_resultado):,}.'
        )
        raise ValueError(
            msg_3
        )

    logging.info(
            'Merge finalizado correctamente: %s filas de ventas conservadas.',
            f'{n_filas_originales:,}',
        )

    return df_resultado


#####----- 1.3 Segmentación por ventas:


def segmentar_por_ventas(historial, cortes_abcd=(0.80, 0.90, 0.95)):
    corte_a, corte_b, corte_c = cortes_abcd

    if not 0 < corte_a < corte_b < corte_c < 1:
        msg = 'cortes_abcd debe cumplir 0 < A < B < C < 1.'
        raise ValueError(
            msg
        )

    # Un solo acumulador por producto; sin DataFrames auxiliares.
    segmentacion = (
        historial
        .assign(
            CANTIDAD_TOTAL=pd.to_numeric(
                historial['CANTIDAD_TOTAL'],
                errors='coerce',
            )
        )
        .groupby('EAN', as_index=False)
        .agg(
            UNIDADES_VENDIDAS=('CANTIDAD_TOTAL', 'sum'),
            DIAS_CON_VENTA=('P_DATE', 'nunique'),
        )
    )

    # Devoluciones/ajustes negativos no deben distorsionar la contribución.
    segmentacion['UNIDADES_VENDIDAS'] = (
        segmentacion['UNIDADES_VENDIDAS'].fillna(0).clip(lower=0)
    )

    total_unidades = segmentacion['UNIDADES_VENDIDAS'].sum()

    if total_unidades <= 0:
        msg = 'La suma de CANTIDAD_TOTAL debe ser mayor que cero.'
        raise ValueError(
            msg
        )

    segmentacion = (
        segmentacion
        .sort_values('UNIDADES_VENDIDAS', ascending=False)
        .reset_index(drop=True)
    )

    # Participación individual y acumulada.
    segmentacion['PARTICIPACION_VENTAS'] = (
        segmentacion['UNIDADES_VENDIDAS'] / total_unidades
    )
    segmentacion['PARTICIPACION_ACUMULADA'] = (
        segmentacion['PARTICIPACION_VENTAS'].cumsum()
    )

    # Acumulado previo: el producto que cruza un umbral pertenece
    # al segmento que completa ese tramo de cobertura.
    acumulado_anterior = (
        segmentacion['PARTICIPACION_ACUMULADA']
        - segmentacion['PARTICIPACION_VENTAS']
    )

    # A: <=corte_a | B: corte_a-corte_b | C: corte_b-corte_c | D: resto.
    segmentacion['SEGMENTO_ABCD'] = np.select(
        [
            acumulado_anterior < corte_a,
            acumulado_anterior < corte_b,
            acumulado_anterior < corte_c,
        ],
        ['A', 'B', 'C'],
        default='D',
    )

    return segmentacion



#####----- 1.4 Detección productos muertos

def caracterizar_productos(
    historial_ventas,
    ventas_por_producto,
    dias_recientes=28,
    dias_base=90,
    minimo_dias_historial=90,
    multiplicador_inactividad=3.0,
    umbrales_ratio=(0.25, 0.60, 1.50),
):
    """Caracteriza todos los productos (todos los segmentos ABCD) segun su
    actividad reciente vs. su ventana historica base, acumulando el
    diagnostico en un unico DataFrame por EAN.

    Estados: MUERTO PROBABLE, CAIDA SEVERA, CAIDA MODERADA, NORMAL,
    EN CRECIMIENTO, HISTORIAL INSUFICIENTE.

    umbrales_ratio : tuple(float, float, float)
        (severa, moderada, crecimiento) sobre el ratio de venta diaria
        promedio reciente / base:
        - ratio < severa      -> CAIDA SEVERA
        - ratio < moderada    -> CAIDA MODERADA
        - ratio > crecimiento -> EN CRECIMIENTO
        - resto               -> NORMAL
    """
    corte_severa, corte_moderada, corte_crecimiento = umbrales_ratio

    columnas_ventas = {'EAN', 'P_DATE', 'CANTIDAD_TOTAL'}
    columnas_segmentos = {'EAN', 'SEGMENTO_ABCD'}

    faltantes_ventas = columnas_ventas.difference(historial_ventas.columns)
    faltantes_segmentos = columnas_segmentos.difference(
        ventas_por_producto.columns
    )

    if faltantes_ventas:
        msg = f'Faltan columnas en historial_ventas: {sorted(faltantes_ventas)}'
        raise ValueError(
            msg
        )
    if faltantes_segmentos:
        msg = (
            'Faltan columnas en ventas_por_producto: '
            f'{sorted(faltantes_segmentos)}'
        )
        raise ValueError(
            msg
        )

    for nombre, valor in [
        ('dias_recientes', dias_recientes),
        ('dias_base', dias_base),
        ('minimo_dias_historial', minimo_dias_historial),
        ('multiplicador_inactividad', multiplicador_inactividad),
    ]:
        if valor <= 0:
            msg_0 = f'`{nombre}` debe ser mayor que cero.'
            raise ValueError(msg_0)

    if not 0 < corte_severa < corte_moderada < 1 <= corte_crecimiento:
        msg = (
            'umbrales_ratio debe cumplir 0 < severa < moderada < 1 <= '
            'crecimiento.'
        )
        raise ValueError(
            msg
        )

    def normalizar_ean(serie):
        return (
            serie.astype('string')
            .str.strip()
            .str.replace(r'\.0$', '', regex=True)
        )

    # ------------------------------------------------------------------
    # 1. Mapa EAN -> segmento
    # ------------------------------------------------------------------
    segmentos = ventas_por_producto[['EAN', 'SEGMENTO_ABCD']].copy()
    segmentos['EAN'] = normalizar_ean(segmentos['EAN'])
    segmentos['SEGMENTO_ABCD'] = (
        segmentos['SEGMENTO_ABCD'].astype('string').str.strip().str.upper()
    )
    segmentos = segmentos.dropna(
        subset=['EAN', 'SEGMENTO_ABCD']
    ).drop_duplicates(subset=['EAN'])

    mapa_segmento = segmentos.set_index('EAN')['SEGMENTO_ABCD']

    # ------------------------------------------------------------------
    # 2. Historial: solo columnas usadas, ventas efectivas
    # ------------------------------------------------------------------
    ventas = historial_ventas[['EAN', 'P_DATE', 'CANTIDAD_TOTAL']].copy()
    ventas['EAN'] = normalizar_ean(ventas['EAN'])
    ventas = ventas.loc[ventas['EAN'].isin(mapa_segmento.index)]
    ventas['P_DATE'] = pd.to_datetime(
        ventas['P_DATE'], errors='coerce'
    ).dt.normalize()
    ventas['CANTIDAD_TOTAL'] = pd.to_numeric(
        ventas['CANTIDAD_TOTAL'], errors='coerce'
    )
    ventas = ventas.dropna(subset=['EAN', 'P_DATE', 'CANTIDAD_TOTAL'])
    ventas = ventas.loc[ventas['CANTIDAD_TOTAL'].gt(0)]

    if ventas.empty:
        msg = 'No hay registros con CANTIDAD_TOTAL > 0.'
        raise ValueError(msg)

    ventas_diarias = ventas.groupby(
        ['EAN', 'P_DATE'], as_index=False
    ).agg(CANTIDAD_TOTAL=('CANTIDAD_TOTAL', 'sum'))

    fecha_corte = ventas_diarias['P_DATE'].max()
    inicio_reciente = fecha_corte - pd.Timedelta(days=dias_recientes - 1)
    fin_base = inicio_reciente - pd.Timedelta(days=1)
    inicio_base = fin_base - pd.Timedelta(days=dias_base - 1)

    resultados = []

    # ------------------------------------------------------------------
    # 3. Diagnostico por EAN (todos los segmentos)
    # ------------------------------------------------------------------
    for ean, producto in ventas_diarias.groupby('EAN'):
        producto = producto.sort_values('P_DATE')
        fechas = producto['P_DATE']

        primera_venta = fechas.min()
        ultima_venta = fechas.max()
        dias_desde_ultima_venta = int((fecha_corte - ultima_venta).days)
        antiguedad_dias = int((fecha_corte - primera_venta).days + 1)

        intervalos = fechas.diff().dt.days.dropna()
        intervalo_mediano = (
            float(intervalos.median()) if not intervalos.empty else np.nan
        )
        intervalo_p90 = (
            float(intervalos.quantile(0.90))
            if not intervalos.empty
            else np.nan
        )

        mask_base = fechas.between(inicio_base, fin_base)
        mask_reciente = fechas.between(inicio_reciente, fecha_corte)

        cantidad_base = float(producto.loc[mask_base, 'CANTIDAD_TOTAL'].sum())
        cantidad_reciente = float(
            producto.loc[mask_reciente, 'CANTIDAD_TOTAL'].sum()
        )
        dias_venta_base = int(mask_base.sum())
        dias_venta_reciente = int(mask_reciente.sum())

        promedio_diario_base = cantidad_base / dias_base
        promedio_diario_reciente = cantidad_reciente / dias_recientes
        frecuencia_semanal_base = dias_venta_base / dias_base * 7
        frecuencia_semanal_reciente = dias_venta_reciente / dias_recientes * 7

        if promedio_diario_base > 0:
            ratio_cantidad = promedio_diario_reciente / promedio_diario_base
            variacion_cantidad_pct = (ratio_cantidad - 1) * 100
        else:
            ratio_cantidad = np.nan
            variacion_cantidad_pct = np.nan

        ratio_frecuencia = (
            frecuencia_semanal_reciente / frecuencia_semanal_base
            if frecuencia_semanal_base > 0
            else np.nan
        )

        if pd.notna(intervalo_p90):
            umbral_inactividad = max(
                dias_recientes,
                int(np.ceil(intervalo_p90 * multiplicador_inactividad)),
            )
        else:
            umbral_inactividad = dias_recientes

        historial_suficiente = (
            antiguedad_dias >= minimo_dias_historial
            and dias_venta_base >= 2
            and cantidad_base > 0
        )

        if not historial_suficiente:
            estado = 'HISTORIAL INSUFICIENTE'
        elif (
            cantidad_reciente == 0
            and dias_desde_ultima_venta >= umbral_inactividad
        ):
            estado = 'MUERTO PROBABLE'
        elif ratio_cantidad < corte_severa:
            estado = 'CAIDA SEVERA'
        elif ratio_cantidad < corte_moderada:
            estado = 'CAIDA MODERADA'
        elif ratio_cantidad > corte_crecimiento:
            estado = 'EN CRECIMIENTO'
        else:
            estado = 'NORMAL'

        resultados.append({
            'EAN': ean,
            'SEGMENTO_ABCD': mapa_segmento.get(ean),
            'ESTADO': estado,
            'PRIMERA_VENTA': primera_venta,
            'ULTIMA_VENTA': ultima_venta,
            'DIAS_DESDE_ULTIMA_VENTA': dias_desde_ultima_venta,
            'ANTIGUEDAD_DIAS': antiguedad_dias,
            'DIAS_CON_VENTA_TOTAL': int(fechas.nunique()),
            'CANTIDAD_HISTORICA_TOTAL': float(
                producto['CANTIDAD_TOTAL'].sum()
            ),
            'INTERVALO_MEDIANO_VENTAS': intervalo_mediano,
            'INTERVALO_P90_VENTAS': intervalo_p90,
            'UMBRAL_INACTIVIDAD_DIAS': umbral_inactividad,
            'CANTIDAD_PERIODO_BASE': cantidad_base,
            'CANTIDAD_PERIODO_RECIENTE': cantidad_reciente,
            'PROMEDIO_DIARIO_BASE': promedio_diario_base,
            'PROMEDIO_DIARIO_RECIENTE': promedio_diario_reciente,
            'DIAS_VENTA_BASE': dias_venta_base,
            'DIAS_VENTA_RECIENTE': dias_venta_reciente,
            'FRECUENCIA_SEMANAL_BASE': frecuencia_semanal_base,
            'FRECUENCIA_SEMANAL_RECIENTE': frecuencia_semanal_reciente,
            'RATIO_CANTIDAD_RECIENTE_BASE': ratio_cantidad,
            'VARIACION_CANTIDAD_PCT': variacion_cantidad_pct,
            'RATIO_FRECUENCIA_RECIENTE_BASE': ratio_frecuencia,
        })

    caracterizacion = pd.DataFrame(resultados)

    # ------------------------------------------------------------------
    # 4. Orden de salida (por segmento, luego por severidad)
    # ------------------------------------------------------------------
    orden_estados = {
        'MUERTO PROBABLE': 1,
        'CAIDA SEVERA': 2,
        'CAIDA MODERADA': 3,
        'NORMAL': 4,
        'EN CRECIMIENTO': 5,
        'HISTORIAL INSUFICIENTE': 6,
    }
    orden_segmentos = {'A': 1, 'B': 2, 'C': 3, 'D': 4}

    caracterizacion = (
        caracterizacion
        .assign(
            ORDEN_SEGMENTO=caracterizacion['SEGMENTO_ABCD'].map(
                orden_segmentos
            ),
            ORDEN_ESTADO=caracterizacion['ESTADO'].map(orden_estados),
        )
        .sort_values(
            ['ORDEN_SEGMENTO', 'ORDEN_ESTADO', 'DIAS_DESDE_ULTIMA_VENTA',
             'RATIO_CANTIDAD_RECIENTE_BASE'],
            ascending=[True, True, False, True],
            na_position='last',
        )
        .drop(columns=['ORDEN_SEGMENTO', 'ORDEN_ESTADO'])
        .reset_index(drop=True)
    )

    parametros = {
        'FECHA_CORTE': fecha_corte,
        'INICIO_PERIODO_BASE': inicio_base,
        'FIN_PERIODO_BASE': fin_base,
        'INICIO_PERIODO_RECIENTE': inicio_reciente,
        'FIN_PERIODO_RECIENTE': fecha_corte,
        'DIAS_BASE': dias_base,
        'DIAS_RECIENTES': dias_recientes,
        'MINIMO_DIAS_HISTORIAL': minimo_dias_historial,
        'MULTIPLICADOR_INACTIVIDAD': multiplicador_inactividad,
        'UMBRALES_RATIO': umbrales_ratio,
    }

    return caracterizacion, parametros


def main():

    #------- Inputs ---------#
    args = vars(parser.parse_args())
    execution_date: str = args['execution_date']
    proyecto: str = args['project_id']  # noqa: F841
    store_banner:str = args['store_banner']

    file_site = '/sites/BigDatayAdvancedAnalytics/Documentos compartidos/'
    file_site += 'Pricing/Forecast Promociones'
    secret_name = 'bdaa_sharepoint_credentials'  # noqa: S105#HC
    sp_cred = secretmanager.getSecret(secret_name, project=proyecto)  # noqa: F841

    esquema = 'TMP'
    tabla = 'TMP_REGRESSION_PROCESSED_DATA_FORECAST'
    path_table = f'{proyecto}.{esquema}.{tabla}'

    # Nota: local queda definido, en producción se inyecta desde Airflow
    gbq_client = Client()
    logging.info(f'execution_date: {execution_date}')
    logging.info(f'proyecto: {proyecto}')
    logging.info(f'store_banner: {store_banner}')


    logging.info('##### [P1] Datasets queries #####')

    query_forecast = QUERY_FORECAST['query_data_procesada'].substitute(
        path_table=path_table)
    df_hist_venta = readBigQuery(
                    query=query_forecast,
                    user=usuario,
                    gbq_client=gbq_client)

    logging.info('[1.1] Dimensiones df hist_venta: %s', df_hist_venta.shape)
    logging.info('[1.1] Eans únicos: %s', df_hist_venta['EAN'].nunique())
    logging.info(f'[1.1] Memoria utilizada: {df_hist_venta.memory_usage(deep=True).sum() / 1024**2:.2f} MB')  # noqa: E501



    query_promos_basic = SQL_QUERIES_ALL_PROMOS['query_promos_forecasting'].substitute(
            path_table=path_table)
    df_promos_basic = readBigQuery(
        query=query_promos_basic,
        user=usuario,
        gbq_client=gbq_client)

    logging.info('[1.2] Dimensiones df promos basic: %s', df_promos_basic.shape)
    logging.info('[1.2] Eans únicos: %s', df_promos_basic['ean'].nunique())
    logging.info(f'[1.2] Memoria utilizada: {df_promos_basic.memory_usage(deep=True).sum() / 1024**2:.2f} MB')  # noqa: E501


    query_promos_tratada = SQL_QUERIES_ALL_PROMOS_COMBINACION['query_promos_forecasting_combinacion'].substitute(  # noqa: E501
            path_table=path_table)

    df_promos_tratada = readBigQuery(
        query=query_promos_tratada,
        user=usuario,
        gbq_client=gbq_client)

    logging.info('[1.3] Dimensiones df promos tratada: %s', df_promos_tratada.shape)
    logging.info('[1.3] Eans únicos: %s', df_promos_tratada['ean'].nunique())
    logging.info(f'[1.3] Memoria utilizada: {df_promos_tratada.memory_usage(deep=True).sum() / 1024**2:.2f} MB')  # noqa: E501


    logging.info('##### [P2] Agregar features frugales y feriados #####')

    logging.info('[2.1] Agregando features frugales: tendencia lineal y estacionalidad anual')
    df_hist_venta = agregar_features_frugales(df_hist_venta)

    logging.info('[2.2] Agregando dummies de feriados y pre-feriados para Chile')
    df_hist_venta = procesar_feriados_chile(df_hist_venta)

    logging.info('##### [P3] Merge de historial de ventas con promociones #####')
    df_historial = merge_historial_ventas_con_promociones(  # noqa: F841
    df_ventas=df_hist_venta,
    df_promos_query_basic=df_promos_basic,
    df_promos_query_tratada=df_promos_tratada)

    # Parche: Asegurar que las columnas FLAG_PROMO_B y FLAG_PROMO_T sean
    # de tipo int8 y no contengan valores nulos.
    columnas_flag_promo = [
        'FLAG_PROMO_B',
        'FLAG_PROMO_T',
    ]

    df_historial[columnas_flag_promo] = (
        df_historial[columnas_flag_promo]
        .fillna(0)
        .astype('int8'))

    ##-------------------------------------------------------------------##
    ########---------- [P4] Caracterización de productos ----------########
    ##-------------------------------------------------------------------##


    #---------- 4.1 Segmentación de productos por ventas --------#
    logging.info('##### [4.1] Segmentación de productos por ventas #####')
    ventas_por_producto = segmentar_por_ventas(
        df_historial,
        cortes_abcd=(0.80, 0.90, 0.95),
    )

    logging.info('[4.1] Frecuencias Segmentación: %s', ventas_por_producto['SEGMENTO_ABCD'].value_counts())  # noqa: E501

    # Printeo informativo (omitible)
    segmentos = ['A','B','C','D']
    logging.info('[4.1] Cantidad de productos bajo %d días por Segmento: ', MIN_DIAS)
    for seg in segmentos:
        mascara = (
            ventas_por_producto['SEGMENTO_ABCD'].eq(seg)
            & ventas_por_producto['DIAS_CON_VENTA'].le(MIN_DIAS))

        logging.info(f'[4.1] #-{seg}: {ventas_por_producto.loc[mascara].shape[0]}')


    #----------- 4.2 Detección de productos muertos ----------- #
    logging.info('[4.2] Detección de productos muertos: ')
    caracterizacion_caidas, _ = caracterizar_productos(
    df_historial,
    ventas_por_producto,
    dias_recientes=28,
    dias_base=90,
    minimo_dias_historial=90,
    multiplicador_inactividad=3.0,
    umbrales_ratio=(0.25, 0.60, 1.50))

    logging.info('[4.2] Resumen Detección de caídas por segmento')
    frecuencia_estados = pd.crosstab(
        caracterizacion_caidas['SEGMENTO_ABCD'],
        caracterizacion_caidas['ESTADO'],
        margins=True,
        margins_name='TOTAL')

    logging.info(frecuencia_estados)

if __name__ == '__main__':

    main()
