from __future__ import annotations

import io
import os
import sys
import logging
import argparse
import posixpath
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


from openpyxl import Workbook  # noqa: E402
from openpyxl.utils import get_column_letter  # noqa: E402
from openpyxl.styles import Font, Side, Border, Alignment, PatternFill  # noqa: E402

import common.gcp_extended.secretsmanager as secretmanager  # noqa: E402
import common.office365_extended.sharepoint as sp  # noqa: E402
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


segmentos_config = {

    'Descripcion Producto': {
        'color': 'F8CBAD',
        'color_header': 'C00000',
        'columnas': ['EAN','PRODUCT_DESCRIPTION','CATEGORY_DESCRIPTION','SUB_CATEGORY_DESCRIPTION',
                     'MATERIAL', 'SALES_UOM', 'SALES_UNIT']
    },
    'Ventas': {
        'color': 'C6E0B4',
        'color_header': '548235',
        'columnas': ['UNIDADES_VENDIDAS', 'DIAS_CON_VENTA', 'PARTICIPACION_VENTAS',
                     'PARTICIPACION_ACUMULADA', 'SEGMENTO_ABCD']
    },
    'Productos Muertos': {
        'color': 'FCE4D6',
        'color_header': 'C55A11',
        'columnas': ['ESTADO', 'PRIMERA_VENTA', 'ULTIMA_VENTA',
                     'DIAS_DESDE_ULTIMA_VENTA',
                     'INTERVALO_P90_VENTAS',
                     'DIAS_VENTA_BASE',
                     'DIAS_VENTA_RECIENTE',
                     'RATIO_CANTIDAD_RECIENTE_BASE',
                     'RATIO_FRECUENCIA_RECIENTE_BASE']
    },
    'Intermitencias': {
        'color': 'DDEBF7',
        'color_header': '2E74B5',
        'columnas': ['PORCENTAJE_COBERTURA', 'MAYOR_PAUSA_SIN_VENTA_DIAS',
                     'N_PAUSAS_INUSUALES', 'ELEGIBLE_PARA_EVALUACION_PAUSAS',
                     'RATIO_DIAS_PERDIDOS_RECIENTE', 'FLAG_LANZAMIENTO_FANTASMA',
                     'FLAG_MUERTE_RECIENTE', 'DIAS_SIN_VENTA_INICIO', 'DIAS_SIN_VENTA_MEDIO',
                     'DIAS_SIN_VENTA_RECIENTE', 'ZONA_CONCENTRACION_PAUSAS']
    },
    'Producto Nuevo': {
        'color': 'FFF2CC',
        'color_header': 'BF8F00',
        'columnas': ['ES_PRODUCTO_NUEVO']
    },
    'Caracterizacion Variabilidad': {
        'color': 'E2D9F3',
        'color_header': '7030A0',
        'columnas': ['CV_UNIDADES', 'RATIO_DISPERSION_UNIDADES',
                     'CV_ROBUSTO_PRECIO',
                     'CORRELACION_PRECIO_UNIDADES', 'FLAG_DEMANDA_MUY_VARIABLE',
                     'FLAG_SOBREDISPERSION_UNIDADES', 'FLAG_PRECIO_MUY_VARIABLE',
                     'FLAG_RELACION_PRECIO_UNIDADES']
    },
    'Informacion Promocional': {
        'color': 'FFE5F0',
        'color_header': 'C00060',
        'columnas': ['N_DIAS_PROMOCIONALES', 'INTENSIDAD_PROMOCIONAL', 'PRECIO_MIN_PROMO',
                     'PRECIO_MAX_PROMO',
                     'DESCUENTO_MIN_PROMO', 'DESCUENTO_MAX_PROMO', 'CV_PRECIO_PROMO',
                     'MECANICA_PRECIO_MIN', 'MECANICA_PRECIO_MAX', 'N_MECANICAS_OBSERVADAS',
                     'RACHA_MAX_MISMA_MECANICA', 'N_MECANICAS_VALIDAS',
                     'CASO_MODELO', 'INCLUIR_MECANICA', 'MECANICA_REFERENCIA']
    },
    'ADI/CV2': {
        'color': 'D9E1F2',
        'color_header': '203864',
        'columnas': ['ADI', 'CV2', 'TIPOLOGIA_DEMANDA']
    },
}

# Columnas que deben mostrarse solo con fecha (sin hora)
COLUMNAS_SOLO_FECHA = ['PRIMERA_VENTA', 'ULTIMA_VENTA']

# Anchos personalizados (columna: ancho en caracteres)
ANCHOS_PERSONALIZADOS = {
    'PRODUCT_DESCRIPTION': 45,
}


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


#####----- 1.5 Detección de intermitencias:

def _distribuir_pausas_por_zona(intervalos, primera_venta, ultima_venta):
    """Reparte los dias sin venta de cada pausa en tres tramos de la vida
    activa del producto: INICIO [0,1/3), MEDIO [1/3,2/3), RECIENTE [2/3,1].

    Cada pausa se ubica por su punto medio relativo. Devuelve el conteo de
    dias perdidos por zona, la zona dominante y la posicion relativa
    (0 = primera venta, 1 = ultima venta) de la mayor pausa.
    """
    span = (ultima_venta - primera_venta).days

    if intervalos.empty or span <= 0:
        return {
            'DIAS_SIN_VENTA_INICIO': 0,
            'DIAS_SIN_VENTA_MEDIO': 0,
            'DIAS_SIN_VENTA_RECIENTE': 0,
            'ZONA_CONCENTRACION_PAUSAS': 'SIN_PAUSAS',
            'POSICION_RELATIVA_MAYOR_PAUSA': np.nan,
        }

    dias_por_zona = {'INICIO': 0, 'MEDIO': 0, 'RECIENTE': 0}
    posicion_mayor = np.nan
    mayor_dias = -1

    for _, fila in intervalos.iterrows():
        dias = int(fila['DIAS_SIN_VENTA'])
        inicio_pausa = fila['FECHA_ULTIMA_VENTA_ANTERIOR']

        # Punto medio de la pausa en dias desde la primera venta.
        offset_medio = (inicio_pausa - primera_venta).days + (dias + 1) / 2
        posicion = min(max(offset_medio / span, 0.0), 1.0)

        if posicion < 1 / 3:
            dias_por_zona['INICIO'] += dias
        elif posicion < 2 / 3:
            dias_por_zona['MEDIO'] += dias
        else:
            dias_por_zona['RECIENTE'] += dias

        if dias > mayor_dias:
            mayor_dias = dias
            posicion_mayor = posicion

    zona_dominante = max(dias_por_zona, key=dias_por_zona.get)

    return {
        'DIAS_SIN_VENTA_INICIO': dias_por_zona['INICIO'],
        'DIAS_SIN_VENTA_MEDIO': dias_por_zona['MEDIO'],
        'DIAS_SIN_VENTA_RECIENTE': dias_por_zona['RECIENTE'],
        'ZONA_CONCENTRACION_PAUSAS': zona_dominante,
        'POSICION_RELATIVA_MAYOR_PAUSA': round(posicion_mayor, 3),
    }


def identificar_periodos_sin_venta_inusuales(  # noqa: D417
    historial_ventas,
    ventas_por_producto,
    segmentos=None,
    minimo_dias_sin_venta=14,
    multiplicador_p90=2.0,
    minimo_intervalos_referencia=3,
    incluir_gap_final=False,
    cobertura_maxima_fantasma=60.0,
    ratio_minimo_muerte_reciente=0.50):

    """Identifica periodos inusualmente largos sin venta por EAN y
    segmento, y caracteriza donde se concentran las pausas dentro de
    la vida activa del producto.

    Como los dias sin venta no existen como filas, se infieren mediante la
    diferencia entre fechas consecutivas con venta positiva:

        DIAS_SIN_VENTA =
            (FECHA_SIGUIENTE_VENTA - FECHA_ULTIMA_VENTA_ANTERIOR).days - 1

    Una pausa es inusual cuando cumple simultaneamente:

    1. DIAS_SIN_VENTA >= minimo_dias_sin_venta.
    2. DIAS_SIN_VENTA >= multiplicador_p90 * P90 de los intervalos
       historicos entre ventas del propio EAN (excluyendo el intervalo
       evaluado).

    Banderas derivadas
    ------------------
    FLAG_LANZAMIENTO_FANTASMA:
        Pausas concentradas al INICIO y cobertura temporal baja. Sugiere un
        lanzamiento/testeo temprano seguido de un arranque real posterior.

    FLAG_MUERTE_RECIENTE:
        Pausas concentradas en el tramo RECIENTE con una proporcion alta de
        los dias perdidos totales. Sugiere un producto deteriorandose.

    Parameters
    ----------
    cobertura_maxima_fantasma : float, default=60.0
        Cobertura (%) por debajo de la cual, con zona INICIO, se marca
        FLAG_LANZAMIENTO_FANTASMA.

    ratio_minimo_muerte_reciente : float, default=0.50
        Proporcion minima de dias sin venta ubicados en el tramo RECIENTE
        (sobre el total de dias perdidos) para marcar FLAG_MUERTE_RECIENTE.

    Returns
    -------
    tuple[pd.DataFrame, pd.DataFrame, dict]
        periodos_inusuales, resumen_productos, parametros.
    """
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

    if minimo_dias_sin_venta <= 0:
        msg = '`minimo_dias_sin_venta` debe ser mayor que cero.'
        raise ValueError(msg)
    if multiplicador_p90 <= 0:
        msg = '`multiplicador_p90` debe ser mayor que cero.'
        raise ValueError(msg)
    if minimo_intervalos_referencia < 1:
        msg = '`minimo_intervalos_referencia` debe ser al menos 1.'
        raise ValueError(msg)
    if not 0 <= cobertura_maxima_fantasma <= 100:
        msg = '`cobertura_maxima_fantasma` debe estar en [0, 100].'
        raise ValueError(msg)
    if not 0 < ratio_minimo_muerte_reciente <= 1:
        msg = '`ratio_minimo_muerte_reciente` debe estar en (0, 1].'
        raise ValueError(
            msg
        )

    if segmentos is None:
        segmentos = ['A', 'B', 'C', 'D']

    segmentos = [str(segmento).strip().upper() for segmento in segmentos]
    segmentos_invalidos = set(segmentos).difference({'A', 'B', 'C', 'D'})
    if segmentos_invalidos:
        msg = f'Segmentos invalidos: {sorted(segmentos_invalidos)}.'
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
    # 1. Universo de EAN y segmentos
    # ------------------------------------------------------------------
    segmentos_ref = ventas_por_producto[['EAN', 'SEGMENTO_ABCD']].copy()
    segmentos_ref['EAN_KEY'] = normalizar_ean(segmentos_ref['EAN'])
    segmentos_ref['SEGMENTO_ABCD'] = (
        segmentos_ref['SEGMENTO_ABCD'].astype('string').str.strip().str.upper()
    )
    segmentos_ref = (
        segmentos_ref
        .dropna(subset=['EAN_KEY', 'SEGMENTO_ABCD'])
        .loc[lambda tabla: tabla['SEGMENTO_ABCD'].isin(segmentos)]
        .drop_duplicates(subset=['EAN_KEY'], keep='first')
    )

    if segmentos_ref.empty:
        msg = 'No existen EAN para los segmentos solicitados.'
        raise ValueError(msg)

    # ------------------------------------------------------------------
    # 2. Historial de ventas positivas
    # ------------------------------------------------------------------
    ventas = historial_ventas[['EAN', 'P_DATE', 'CANTIDAD_TOTAL']].copy()
    ventas['EAN_KEY'] = normalizar_ean(ventas['EAN'])
    ventas['P_DATE'] = pd.to_datetime(
        ventas['P_DATE'], errors='coerce'
    ).dt.normalize()
    ventas['CANTIDAD_TOTAL'] = pd.to_numeric(
        ventas['CANTIDAD_TOTAL'], errors='coerce'
    )
    ventas = (
        ventas
        .dropna(subset=['EAN_KEY', 'P_DATE', 'CANTIDAD_TOTAL'])
        .loc[lambda tabla: tabla['CANTIDAD_TOTAL'].gt(0)]
        .drop(columns=['EAN'])
        .merge(
            segmentos_ref[['EAN_KEY', 'EAN', 'SEGMENTO_ABCD']],
            on='EAN_KEY',
            how='inner',
            validate='many_to_one',
        )
    )

    if ventas.empty:
        msg = 'No existen ventas positivas para los segmentos solicitados.'
        raise ValueError(
            msg
        )

    ventas_diarias = (
        ventas
        .groupby(
            ['EAN_KEY', 'EAN', 'SEGMENTO_ABCD', 'P_DATE'],
            as_index=False,
        )
        .agg(CANTIDAD_TOTAL=('CANTIDAD_TOTAL', 'sum'))
        .sort_values(['EAN_KEY', 'P_DATE'])
        .reset_index(drop=True)
    )

    fecha_corte = ventas_diarias['P_DATE'].max()

    eventos = []
    resumenes = []

    # ------------------------------------------------------------------
    # 3. Evaluacion de pausas por EAN
    # ------------------------------------------------------------------
    for (_ean_key, ean, segmento), producto in ventas_diarias.groupby(
        ['EAN_KEY', 'EAN', 'SEGMENTO_ABCD'], dropna=False
    ):
        fechas_venta = (
            producto['P_DATE'].drop_duplicates().sort_values().reset_index(
                drop=True
            )
        )

        primera_venta = fechas_venta.min()
        ultima_venta = fechas_venta.max()
        dias_con_venta = len(fechas_venta)
        dias_calendario = int((ultima_venta - primera_venta).days + 1)
        porcentaje_cobertura = dias_con_venta / dias_calendario * 100

        # Intervalos internos entre ventas consecutivas.
        if len(fechas_venta) >= 2:
            intervalos = pd.DataFrame({
                'FECHA_ULTIMA_VENTA_ANTERIOR': fechas_venta.iloc[:-1].to_numpy(),
                'FECHA_SIGUIENTE_VENTA': fechas_venta.iloc[1:].to_numpy(),
            })
            intervalos['DIAS_SIN_VENTA'] = (
                intervalos['FECHA_SIGUIENTE_VENTA']
                - intervalos['FECHA_ULTIMA_VENTA_ANTERIOR']
            ).dt.days - 1
        else:
            intervalos = pd.DataFrame(
                columns=[
                    'FECHA_ULTIMA_VENTA_ANTERIOR',
                    'FECHA_SIGUIENTE_VENTA',
                    'DIAS_SIN_VENTA',
                ]
            )

        # Gap final opcional (censurado).
        if incluir_gap_final:
            dias_gap_final = int((fecha_corte - ultima_venta).days)
            if dias_gap_final > 0:
                intervalos = pd.concat(
                    [
                        intervalos,
                        pd.DataFrame({
                            'FECHA_ULTIMA_VENTA_ANTERIOR': [ultima_venta],
                            'FECHA_SIGUIENTE_VENTA': [pd.NaT],
                            'DIAS_SIN_VENTA': [dias_gap_final],
                            'ES_GAP_FINAL_CENSURADO': [True],
                        }),
                    ],
                    ignore_index=True,
                )

        if 'ES_GAP_FINAL_CENSURADO' not in intervalos.columns:
            intervalos['ES_GAP_FINAL_CENSURADO'] = False
        else:
            intervalos['ES_GAP_FINAL_CENSURADO'] = (
                intervalos['ES_GAP_FINAL_CENSURADO'].fillna(False).astype(bool)  # noqa: FBT003
            )

        intervalos['DIAS_SIN_VENTA'] = pd.to_numeric(
            intervalos['DIAS_SIN_VENTA'], errors='coerce'
        )
        intervalos = intervalos.dropna(
            subset=['DIAS_SIN_VENTA']
        ).reset_index(drop=True)

        n_intervalos_totales = len(intervalos)
        intervalos_positivos = intervalos.loc[
            intervalos['DIAS_SIN_VENTA'].gt(0)
        ].copy()

        mayor_pausa = (
            int(intervalos_positivos['DIAS_SIN_VENTA'].max())
            if not intervalos_positivos.empty
            else 0
        )

        n_pausas_inusuales = 0
        umbrales_evaluados = []

        for indice, fila in intervalos_positivos.iterrows():
            # El intervalo actual no participa en su propio P90.
            intervalos_referencia = intervalos.drop(index=indice)[
                'DIAS_SIN_VENTA'
            ]

            if len(intervalos_referencia) < minimo_intervalos_referencia:
                continue

            p90_referencia = float(intervalos_referencia.quantile(0.90))
            umbral_p90 = int(np.ceil(p90_referencia * multiplicador_p90))
            umbral_pausa = max(minimo_dias_sin_venta, umbral_p90)
            umbrales_evaluados.append(umbral_pausa)

            if fila['DIAS_SIN_VENTA'] < umbral_pausa:
                continue

            n_pausas_inusuales += 1

            eventos.append({
                'EAN': ean,
                'SEGMENTO_ABCD': segmento,
                'FECHA_ULTIMA_VENTA_ANTERIOR': (
                    fila['FECHA_ULTIMA_VENTA_ANTERIOR']
                ),
                'FECHA_SIGUIENTE_VENTA': fila['FECHA_SIGUIENTE_VENTA'],
                'DIAS_SIN_VENTA': int(fila['DIAS_SIN_VENTA']),
                'P90_INTERVALOS_REFERENCIA': p90_referencia,
                'UMBRAL_PAUSA_INUSUAL_DIAS': umbral_pausa,
                'MULTIPLICADOR_P90': multiplicador_p90,
                'ES_GAP_FINAL_CENSURADO': bool(fila['ES_GAP_FINAL_CENSURADO']),
            })

        umbral_representativo = (
            float(np.median(umbrales_evaluados))
            if umbrales_evaluados
            else np.nan
        )

        # Concentracion de pausas por zona de la vida activa.
        zona_pausas = _distribuir_pausas_por_zona(
            intervalos_positivos, primera_venta, ultima_venta
        )

        total_dias_perdidos = (
            zona_pausas['DIAS_SIN_VENTA_INICIO']
            + zona_pausas['DIAS_SIN_VENTA_MEDIO']
            + zona_pausas['DIAS_SIN_VENTA_RECIENTE']
        )
        ratio_reciente = (
            zona_pausas['DIAS_SIN_VENTA_RECIENTE'] / total_dias_perdidos
            if total_dias_perdidos > 0
            else 0.0
        )

        flag_fantasma = (
            zona_pausas['ZONA_CONCENTRACION_PAUSAS'] == 'INICIO'
            and porcentaje_cobertura < cobertura_maxima_fantasma
        )
        flag_muerte_reciente = (
            zona_pausas['ZONA_CONCENTRACION_PAUSAS'] == 'RECIENTE'
            and ratio_reciente >= ratio_minimo_muerte_reciente
        )

        resumenes.append({
            'EAN': ean,
            'SEGMENTO_ABCD': segmento,
            'PRIMERA_VENTA': primera_venta,
            'ULTIMA_VENTA': ultima_venta,
            'DIAS_CON_VENTA': dias_con_venta,
            'DIAS_CALENDARIO_ENTRE_PRIMERA_ULTIMA_VENTA': dias_calendario,
            'PORCENTAJE_COBERTURA': porcentaje_cobertura,
            'N_INTERVALOS_TOTALES': n_intervalos_totales,
            'N_PAUSAS_CON_AL_MENOS_1_DIA': len(intervalos_positivos),
            'MAYOR_PAUSA_SIN_VENTA_DIAS': mayor_pausa,
            'UMBRAL_PAUSA_INUSUAL_REPRESENTATIVO': umbral_representativo,
            'N_PAUSAS_INUSUALES': n_pausas_inusuales,
            'TIENE_PAUSA_INUSUAL': n_pausas_inusuales > 0,
            'ELEGIBLE_PARA_EVALUACION_PAUSAS': (
                n_intervalos_totales >= minimo_intervalos_referencia + 1
            ),
            'RATIO_DIAS_PERDIDOS_RECIENTE': round(ratio_reciente, 3),
            'FLAG_LANZAMIENTO_FANTASMA': flag_fantasma,
            'FLAG_MUERTE_RECIENTE': flag_muerte_reciente,
            **zona_pausas,
        })

    # ------------------------------------------------------------------
    # 4. Outputs
    # ------------------------------------------------------------------
    columnas_eventos = [
        'EAN',
        'SEGMENTO_ABCD',
        'FECHA_ULTIMA_VENTA_ANTERIOR',
        'FECHA_SIGUIENTE_VENTA',
        'DIAS_SIN_VENTA',
        'P90_INTERVALOS_REFERENCIA',
        'UMBRAL_PAUSA_INUSUAL_DIAS',
        'MULTIPLICADOR_P90',
        'ES_GAP_FINAL_CENSURADO',
    ]

    periodos_inusuales = pd.DataFrame(eventos, columns=columnas_eventos)

    if not periodos_inusuales.empty:
        periodos_inusuales = (
            periodos_inusuales
            .sort_values(
                ['SEGMENTO_ABCD', 'DIAS_SIN_VENTA', 'EAN'],
                ascending=[True, False, True],
            )
            .reset_index(drop=True)
        )

    resumen_productos = pd.DataFrame(resumenes)
    resumen_productos['PORCENTAJE_COBERTURA'] = (
        resumen_productos['PORCENTAJE_COBERTURA'].round(2)
    )
    resumen_productos = (
        resumen_productos
        .sort_values(
            [
                'SEGMENTO_ABCD',
                'TIENE_PAUSA_INUSUAL',
                'PORCENTAJE_COBERTURA',
                'MAYOR_PAUSA_SIN_VENTA_DIAS',
                'EAN',
            ],
            ascending=[True, False, True, False, True],
        )
        .reset_index(drop=True)
    )

    parametros = {
        'SEGMENTOS_ANALIZADOS': segmentos,
        'FECHA_CORTE': fecha_corte,
        'MINIMO_DIAS_SIN_VENTA': minimo_dias_sin_venta,
        'MULTIPLICADOR_P90': multiplicador_p90,
        'MINIMO_INTERVALOS_REFERENCIA': minimo_intervalos_referencia,
        'INCLUIR_GAP_FINAL': incluir_gap_final,
        'COBERTURA_MAXIMA_FANTASMA': cobertura_maxima_fantasma,
        'RATIO_MINIMO_MUERTE_RECIENTE': ratio_minimo_muerte_reciente,
        'FORMULA_PORCENTAJE_COBERTURA': (
            'DIAS_CON_VENTA / '
            'DIAS_CALENDARIO_ENTRE_PRIMERA_ULTIMA_VENTA * 100'
        ),
    }

    return periodos_inusuales, resumen_productos, parametros


#####----- 1.6 Detección de productos nuevos:

def detectar_productos_nuevos(historial_ventas, dias_producto_nuevo=180):
    """Marca como nuevo todo EAN cuya primera venta ocurrio dentro de los
    ultimos `dias_producto_nuevo` dias respecto de la fecha maxima del
    historial. Solo se consideran ventas efectivas (CANTIDAD_TOTAL > 0).

    Retorna un DataFrame con una fila por EAN: fechas clave, antiguedad y
    la bandera ES_PRODUCTO_NUEVO.
    """
    columnas = {'EAN', 'P_DATE', 'CANTIDAD_TOTAL'}
    faltantes = columnas.difference(historial_ventas.columns)
    if faltantes:
        msg = f'Faltan columnas: {sorted(faltantes)}'
        raise ValueError(msg)

    if not isinstance(dias_producto_nuevo, int) or dias_producto_nuevo <= 0:
        msg = '`dias_producto_nuevo` debe ser un entero > 0.'
        raise ValueError(msg)

    ventas = historial_ventas[['EAN', 'P_DATE', 'CANTIDAD_TOTAL']].copy()
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

    fecha_corte = ventas['P_DATE'].max()
    inicio_ventana = fecha_corte - pd.Timedelta(days=dias_producto_nuevo - 1)

    productos = ventas.groupby('EAN', as_index=False).agg(
        FECHA_PRIMERA_VENTA=('P_DATE', 'min'),
        FECHA_ULTIMA_VENTA=('P_DATE', 'max'),
        DIAS_CON_VENTA=('P_DATE', 'nunique'),
    )

    productos['ANTIGUEDAD_DIAS'] = (
        fecha_corte - productos['FECHA_PRIMERA_VENTA']
    ).dt.days + 1

    # Nuevo: la primera venta cae dentro de la ventana reciente.
    productos['ES_PRODUCTO_NUEVO'] = (
        productos['FECHA_PRIMERA_VENTA'] >= inicio_ventana
    )

    parametros = {
        'DIAS_PRODUCTO_NUEVO': dias_producto_nuevo,
        'FECHA_CORTE': fecha_corte,
        'INICIO_VENTANA_NUEVO': inicio_ventana,
    }

    return productos, parametros


#####----- 1.7 Variabilidad precio y ventas.

def caracterizar_variabilidad(
    historial_ventas,
    umbral_cv_unidades_alto=1.0,
    umbral_cv_robusto_precio_alto=0.15,
    umbral_fano_sobredispersion=1.5,
    umbral_correlacion_relevante=0.3,
    minimo_dias_para_evaluar=5,
):
    """Caracteriza la variabilidad de precio y de unidades vendidas por
    EAN, usando estadisticos robustos y comparables entre productos.

    La variabilidad de unidades se calcula solo sobre dias con venta
    positiva, para no confundir intermitencia con dispersion de demanda.

    Parametros
    ----------
    umbral_cv_unidades_alto : float
        CV de unidades desde el cual se marca demanda muy variable.
    umbral_cv_robusto_precio_alto : float
        CV robusto de precio (IQR/mediana) desde el cual el precio se
        considera muy variable.
    umbral_fano_sobredispersion : float
        Ratio varianza/media desde el cual hay sobredispersion relevante.
    umbral_correlacion_relevante : float
        Correlacion (valor absoluto) desde la cual la relacion precio-
        unidades se considera material.
    minimo_dias_para_evaluar : int
        Minimo de dias con venta para calcular estadisticos confiables.

    Retorna
    -------
    tuple[pd.DataFrame, dict]
        variabilidad_producto (una fila por EAN) y parametros.
    """
    columnas = {'EAN', 'P_DATE', 'CANTIDAD_TOTAL', 'PRECIO_PROMEDIO'}
    faltantes = columnas.difference(historial_ventas.columns)
    if faltantes:
        msg = f'Faltan columnas: {sorted(faltantes)}'
        raise ValueError(msg)

    ventas = historial_ventas[
        ['EAN', 'P_DATE', 'CANTIDAD_TOTAL', 'PRECIO_PROMEDIO']
    ].copy()
    ventas['P_DATE'] = pd.to_datetime(
        ventas['P_DATE'], errors='coerce'
    ).dt.normalize()
    ventas['CANTIDAD_TOTAL'] = pd.to_numeric(
        ventas['CANTIDAD_TOTAL'], errors='coerce'
    )
    ventas['PRECIO_PROMEDIO'] = pd.to_numeric(
        ventas['PRECIO_PROMEDIO'], errors='coerce'
    )
    ventas = ventas.dropna(
        subset=['EAN', 'P_DATE', 'CANTIDAD_TOTAL', 'PRECIO_PROMEDIO']
    )
    ventas = ventas.loc[ventas['CANTIDAD_TOTAL'].gt(0)]

    if ventas.empty:
        msg = 'No hay registros validos con venta positiva.'
        raise ValueError(msg)

    ventas = (
        ventas
        .groupby(['EAN', 'P_DATE'], as_index=False)
        .agg(
            CANTIDAD_TOTAL=('CANTIDAD_TOTAL', 'sum'),
            PRECIO_PROMEDIO=('PRECIO_PROMEDIO', 'mean'),
        )
    )

    def estadisticos_ean(grupo):
        unidades = grupo['CANTIDAD_TOTAL']
        precio = grupo['PRECIO_PROMEDIO']
        n_dias = len(grupo)

        media_u = unidades.mean()
        std_u = unidades.std(ddof=1) if n_dias > 1 else 0.0
        var_u = unidades.var(ddof=1) if n_dias > 1 else 0.0
        cv_u = std_u / media_u if media_u > 0 else np.nan
        fano_u = var_u / media_u if media_u > 0 else np.nan
        p90_u = unidades.quantile(0.90)
        mediana_u = unidades.median()
        ratio_pico_u = p90_u / mediana_u if mediana_u > 0 else np.nan

        mediana_p = precio.median()
        iqr_p = precio.quantile(0.75) - precio.quantile(0.25)
        cv_rob_p = iqr_p / mediana_p if mediana_p > 0 else np.nan
        rango_rel_p = (
            (precio.quantile(0.95) - precio.quantile(0.05)) / mediana_p
            if mediana_p > 0 else np.nan
        )
        n_precios = int(precio.round(2).nunique())

        if n_dias >= minimo_dias_para_evaluar and precio.nunique() > 1:  # noqa: PD101
            corr_pu = precio.corr(unidades)
        else:
            corr_pu = np.nan

        return pd.Series({
            'DIAS_CON_VENTA_EVALUADOS': n_dias,
            'CV_UNIDADES': cv_u,
            'RATIO_DISPERSION_UNIDADES': fano_u,
            'RATIO_PICO_MEDIANA_UNIDADES': ratio_pico_u,
            'CV_ROBUSTO_PRECIO': cv_rob_p,
            'RANGO_RELATIVO_PRECIO': rango_rel_p,
            'N_PRECIOS_DISTINTOS': n_precios,
            'CORRELACION_PRECIO_UNIDADES': corr_pu,
        })

    variabilidad_producto = (
        ventas.groupby('EAN').apply(estadisticos_ean).reset_index()
    )

    evaluable = (
        variabilidad_producto['DIAS_CON_VENTA_EVALUADOS']
        >= minimo_dias_para_evaluar
    )

    variabilidad_producto['FLAG_DEMANDA_MUY_VARIABLE'] = (
        evaluable
        & (variabilidad_producto['CV_UNIDADES'] >= umbral_cv_unidades_alto)
    )
    variabilidad_producto['FLAG_SOBREDISPERSION_UNIDADES'] = (
        evaluable
        & (
            variabilidad_producto['RATIO_DISPERSION_UNIDADES']
            >= umbral_fano_sobredispersion
        )
    )
    variabilidad_producto['FLAG_PRECIO_MUY_VARIABLE'] = (
        evaluable
        & (
            variabilidad_producto['CV_ROBUSTO_PRECIO']
            >= umbral_cv_robusto_precio_alto
        )
    )
    variabilidad_producto['FLAG_RELACION_PRECIO_UNIDADES'] = (
        variabilidad_producto['CORRELACION_PRECIO_UNIDADES'].abs()
        >= umbral_correlacion_relevante
    )
    variabilidad_producto['FLAG_HISTORIAL_INSUFICIENTE_VARIABILIDAD'] = (
        ~evaluable
    )

    variabilidad_producto = variabilidad_producto.sort_values(
        ['CV_UNIDADES', 'CV_ROBUSTO_PRECIO'],
        ascending=[False, False],
    ).reset_index(drop=True)

    parametros = {
        'UMBRAL_CV_UNIDADES_ALTO': umbral_cv_unidades_alto,
        'UMBRAL_CV_ROBUSTO_PRECIO_ALTO': umbral_cv_robusto_precio_alto,
        'UMBRAL_FANO_SOBREDISPERSION': umbral_fano_sobredispersion,
        'UMBRAL_CORRELACION_RELEVANTE': umbral_correlacion_relevante,
        'MINIMO_DIAS_PARA_EVALUAR': minimo_dias_para_evaluar,
    }

    return variabilidad_producto, parametros


#####----- 1.8 Caracterización Promocional.

def caracterizar_promociones_producto(
    historial_ventas,
    sufijo_promo='B',
    umbral_uplift=0.40,
    umbral_regularizacion=0.80,
    incluir_mecanica=True,
    minimo_participacion_mecanica=0.10,
    minimo_mecanicas_distintas=2,
):
    """Caracteriza el comportamiento promocional de cada EAN y define, ya
    en Capa 0, que modelo se usara y como se tratara la mecanica
    promocional. Procesa todo el catalogo en un solo pase
    (sin loop por EAN)

    Casos de modelo segun intensidad promocional
    (i = dias_promo / dias_venta)
    -------------------------------------------------------------------------
    - BASELINE            : i = 0.
    - BASE_UPLIFT         : 0 < i <= umbral_uplift.
    - BASE_UPLIFT_REGULAR : umbral_uplift < i <= umbral_regularizacion.
    - BASELINE_SATURADO   : i > umbral_regularizacion (promo = normalidad).

    Estadisticos descriptivos entregados por EAN
    --------------------------------------------
    - N_DIAS_OBSERVADOS, N_DIAS_PROMOCIONALES, INTENSIDAD_PROMOCIONAL
    - PRECIO_MIN_PROMO / PRECIO_MAX_PROMO y la mecanica asociada a cada uno
    - PRECIO_MEDIO_PROMO, CV_PRECIO_PROMO (variabilidad relativa)
    - DESCUENTO_MIN_PROMO / DESCUENTO_MAX_PROMO / DESCUENTO_MEDIO_PROMO
    - DESCUENTO_STD_PROMO, CV_DESCUENTO_PROMO (variabilidad de profundidad)
    - N_MECANICAS_OBSERVADAS, N_MECANICAS_VALIDAS
    - RACHA_MAX_MISMA_MECANICA (dias de venta consecutivos, misma mecanica)
    """
    sufijo_promo = str(sufijo_promo).strip().upper()
    if sufijo_promo not in {'B', 'T'}:
        msg = "`sufijo_promo` debe ser 'B' o 'T'."
        raise ValueError(msg)
    if not 0 < umbral_uplift < umbral_regularizacion <= 1:
        msg_0 = 'Se requiere 0 < umbral_uplift < umbral_regularizacion <= 1.'
        raise ValueError(
            msg_0
        )
    if not 0 <= minimo_participacion_mecanica <= 1:
        msg_1 = '`minimo_participacion_mecanica` debe estar en [0, 1].'
        raise ValueError(
            msg_1
        )

    columna_flag = f'FLAG_PROMO_{sufijo_promo}'
    columna_descuento = f'porcentaje_descuento_{sufijo_promo}'
    columna_mecanica = f'descripcion_evento_promocional_{sufijo_promo}'

    columnas = {
        'EAN', 'P_DATE', 'CANTIDAD_TOTAL', 'PRECIO_PROMEDIO',
        columna_flag, columna_descuento, columna_mecanica,
    }
    faltantes = columnas.difference(historial_ventas.columns)
    if faltantes:
        msg_2 = f'Faltan columnas: {sorted(faltantes)}'
        raise ValueError(msg_2)

    trabajo = historial_ventas[list(columnas)].copy()

    trabajo['EAN_KEY'] = (
        trabajo['EAN'].astype('string').str.strip()
        .str.replace(r'\.0$', '', regex=True)
    )
    trabajo['P_DATE'] = pd.to_datetime(
        trabajo['P_DATE'], errors='coerce'
    ).dt.normalize()
    trabajo['CANTIDAD_TOTAL'] = pd.to_numeric(
        trabajo['CANTIDAD_TOTAL'], errors='coerce'
    )
    trabajo['PRECIO_PROMEDIO'] = pd.to_numeric(
        trabajo['PRECIO_PROMEDIO'], errors='coerce'
    )
    # Imputacion de descuento: NaN/negativo -> 0 (integridad de datos).
    trabajo[columna_descuento] = (
        pd.to_numeric(trabajo[columna_descuento], errors='coerce')
        .fillna(0).clip(lower=0)
    )

    trabajo = trabajo.dropna(
        subset=['EAN_KEY', 'P_DATE', 'CANTIDAD_TOTAL', 'PRECIO_PROMEDIO']
    )
    trabajo = trabajo.loc[
        trabajo['CANTIDAD_TOTAL'].gt(0) & trabajo['PRECIO_PROMEDIO'].gt(0)
    ]
    if trabajo.empty:
        msg_3 = 'No hay registros con venta y precio positivos.'
        raise ValueError(msg_3)

    trabajo[columna_flag] = (
        pd.to_numeric(trabajo[columna_flag], errors='coerce')
        .fillna(0).clip(lower=0).gt(0).astype(int)
    )
    trabajo[columna_mecanica] = (
        trabajo[columna_mecanica].astype('string').str.strip()
        .replace({'': pd.NA, 'nan': pd.NA, 'None': pd.NA, '<NA>': pd.NA})
    )

    # Consolidacion EAN-dia: flag=max, mecanica=first,
    # precio/descuento=medio
    consolidado = (
        trabajo
        .groupby(['EAN_KEY', 'P_DATE'], as_index=False)
        .agg(
            FLAG=(columna_flag, 'max'),
            MECANICA=(columna_mecanica, 'first'),
            PRECIO=('PRECIO_PROMEDIO', 'mean'),
            DESCUENTO=(columna_descuento, 'mean'),
        )
        .sort_values(['EAN_KEY', 'P_DATE'])
        .reset_index(drop=True)
    )
    # Ordinal de dia de venta por EAN (para rachas sobre dias de venta).
    consolidado['ORD_VENTA'] = (
        consolidado.groupby('EAN_KEY').cumcount()
    )

    # ---------------- Base de intensidad ----------------
    base = (
        consolidado
        .groupby('EAN_KEY', as_index=False)
        .agg(
            N_DIAS_OBSERVADOS=('P_DATE', 'nunique'),
            N_DIAS_PROMOCIONALES=('FLAG', 'sum'),
        )
    )
    base['INTENSIDAD_PROMOCIONAL'] = (
        base['N_DIAS_PROMOCIONALES'] / base['N_DIAS_OBSERVADOS']
    )

    # ---------------- Precio y descuento promocional ----------------
    promo = consolidado.loc[consolidado['FLAG'].eq(1)].copy()

    precio_stats = (
        promo.groupby('EAN_KEY')
        .agg(
            PRECIO_MIN_PROMO=('PRECIO', 'min'),
            PRECIO_MAX_PROMO=('PRECIO', 'max'),
            PRECIO_MEDIO_PROMO=('PRECIO', 'mean'),
            PRECIO_STD_PROMO=('PRECIO', 'std'),
            DESCUENTO_MIN_PROMO=('DESCUENTO', 'min'),
            DESCUENTO_MAX_PROMO=('DESCUENTO', 'max'),
            DESCUENTO_MEDIO_PROMO=('DESCUENTO', 'mean'),
            DESCUENTO_STD_PROMO=('DESCUENTO', 'std'),
        )
    )
    precio_stats['CV_PRECIO_PROMO'] = (
        precio_stats['PRECIO_STD_PROMO']
        / precio_stats['PRECIO_MEDIO_PROMO']
    ).replace([np.inf, -np.inf], np.nan)
    precio_stats['CV_DESCUENTO_PROMO'] = (
        precio_stats['DESCUENTO_STD_PROMO']
        / precio_stats['DESCUENTO_MEDIO_PROMO']
    ).replace([np.inf, -np.inf], np.nan)

    # Mecanica asociada al precio min/max (idxmin/idxmax sobre filas promo)
    idx_min = promo.groupby('EAN_KEY')['PRECIO'].idxmin()
    idx_max = promo.groupby('EAN_KEY')['PRECIO'].idxmax()
    mecanica_precio = pd.DataFrame({
        'MECANICA_PRECIO_MIN': promo.loc[idx_min, 'MECANICA'].values,  # noqa: PD011
        'MECANICA_PRECIO_MAX': promo.loc[idx_max, 'MECANICA'].values,  # noqa: PD011
    })
    mecanica_precio['EAN_KEY'] = promo.loc[idx_min, 'EAN_KEY'].values  # noqa: PD011

    # ---------------- Mecanica: participacion y variabilidad ------------
    promo_mec = promo.loc[promo['MECANICA'].notna()]
    conteo = (
        promo_mec.groupby(['EAN_KEY', 'MECANICA']).size()
        .reset_index(name='N')
    )
    conteo['PARTICIPACION'] = (
        conteo['N'] / conteo.groupby('EAN_KEY')['N'].transform('sum')
    )
    n_observadas = (
        conteo.groupby('EAN_KEY')['MECANICA'].nunique()
        .rename('N_MECANICAS_OBSERVADAS')
    )
    validas = (
        conteo.loc[conteo['PARTICIPACION'].ge(minimo_participacion_mecanica)]
        .sort_values(['EAN_KEY', 'N'], ascending=[True, False])
    )
    mecanicas_validas = (
        validas.groupby('EAN_KEY')['MECANICA'].agg(list)
        .rename('MECANICAS_VALIDAS')
    )

    # ---------------- Racha maxima (dias de venta consecutivos) ---------
    # Nueva racha cuando cambia el EAN, la mecanica, o los dias de venta no
    # son consecutivos en la secuencia de ventas del EAN (ORD_VENTA no
    # contiguo). Se ignoran los huecos de calendario: dos ventas seguidas
    # cuentan como continuas aunque medien semanas sin venta.

    cambio_ean = promo['EAN_KEY'].ne(promo['EAN_KEY'].shift())
    cambio_mec = promo['MECANICA'].ne(promo['MECANICA'].shift())
    gap_venta = promo['ORD_VENTA'].diff().ne(1)
    promo['ID_RACHA'] = (cambio_ean | cambio_mec | gap_venta).cumsum()
    largo_rachas = (
        promo.groupby(['EAN_KEY', 'ID_RACHA']).size()
        .groupby('EAN_KEY').max()
        .rename('RACHA_MAX_MISMA_MECANICA')
    )

    # ---------------- Ensamble ----------------
    caracterizacion = (
        base
        .merge(precio_stats, on='EAN_KEY', how='left')
        .merge(mecanica_precio, on='EAN_KEY', how='left')
        .merge(n_observadas, on='EAN_KEY', how='left')
        .merge(mecanicas_validas, on='EAN_KEY', how='left')
        .merge(largo_rachas, on='EAN_KEY', how='left')
    )
    caracterizacion['N_MECANICAS_OBSERVADAS'] = (
        caracterizacion['N_MECANICAS_OBSERVADAS'].fillna(0).astype(int)
    )
    caracterizacion['RACHA_MAX_MISMA_MECANICA'] = (
        caracterizacion['RACHA_MAX_MISMA_MECANICA'].fillna(0).astype(int)
    )
    caracterizacion['MECANICAS_VALIDAS'] = caracterizacion[
        'MECANICAS_VALIDAS'
    ].apply(lambda valor: valor if isinstance(valor, list) else [])
    caracterizacion['N_MECANICAS_VALIDAS'] = (
        caracterizacion['MECANICAS_VALIDAS'].str.len()
    )

    # ---------------- Asignacion de caso de modelo (4 casos) ------------
    intensidad = caracterizacion['INTENSIDAD_PROMOCIONAL']
    caracterizacion['CASO_MODELO'] = np.select(
        [
            intensidad.eq(0),
            intensidad.le(umbral_uplift),
            intensidad.le(umbral_regularizacion),
        ],
        ['BASELINE', 'BASE_UPLIFT', 'BASE_UPLIFT_REGULAR'],
        default='BASELINE_SATURADO',
    )
    admite_uplift = caracterizacion['CASO_MODELO'].isin(
        ['BASE_UPLIFT', 'BASE_UPLIFT_REGULAR']
    )
    caracterizacion['INCLUIR_MECANICA'] = (
        bool(incluir_mecanica)
        & admite_uplift
        & caracterizacion['N_MECANICAS_VALIDAS'].ge(minimo_mecanicas_distintas)
    )
    caracterizacion['MECANICA_REFERENCIA'] = caracterizacion.apply(
        lambda fila: (
            fila['MECANICAS_VALIDAS'][0]
            if fila['INCLUIR_MECANICA'] and fila['MECANICAS_VALIDAS']
            else None
        ),
        axis=1,
    )
    caracterizacion['ES_REGULARIZADO'] = caracterizacion['CASO_MODELO'].eq(
        'BASE_UPLIFT_REGULAR'
    )
    caracterizacion['MODELO_SUGERIDO'] = np.select(
        [
            caracterizacion['CASO_MODELO'].isin(
                ['BASELINE', 'BASELINE_SATURADO']
            ),
            caracterizacion['CASO_MODELO'].eq('BASE_UPLIFT'),
        ],
        ['GLM-NB (baseline)', 'GLM-NB'],
        default='GLM-NB (Ridge)',
    )
    caracterizacion['ESTRATEGIA_MECANICA'] = np.where(
        caracterizacion['INCLUIR_MECANICA'],
        'DUMMIES_M_MENOS_1_CON_REFERENCIA', 'SIN_MECANICA',
    )

    caracterizacion = (
        caracterizacion
        .rename(columns={'EAN_KEY': 'EAN'})
        .sort_values(
            ['CASO_MODELO', 'INTENSIDAD_PROMOCIONAL'],
            ascending=[True, False],
        )
        .reset_index(drop=True)
    )

    parametros = {
        'SUFIJO_PROMO': sufijo_promo,
        'COLUMNA_FLAG': columna_flag,
        'COLUMNA_DESCUENTO': columna_descuento,
        'COLUMNA_MECANICA': columna_mecanica,
        'UMBRAL_UPLIFT': umbral_uplift,
        'UMBRAL_REGULARIZACION': umbral_regularizacion,
        'INCLUIR_MECANICA': incluir_mecanica,
        'MINIMO_PARTICIPACION_MECANICA': minimo_participacion_mecanica,
        'MINIMO_MECANICAS_DISTINTAS': minimo_mecanicas_distintas,
    }
    return caracterizacion, parametros


#####----- 1.9 Segmentación ADI-CV

def clasificar_tipologia_demanda(
    historial_ventas,
    umbral_adi=1.32,
    umbral_cv2=0.49,
):
    """Clasifica cada EAN en una tipologia de demanda (Syntetos-Boylan) a
    partir de la regularidad (ADI) y la variabilidad del tamano (CV2).

    Se procesa todo el catalogo en un solo pase, usando solo dias con
    venta positiva.

    Indicadores
    -----------
    ADI = N_DIAS_OBSERVADOS / N_DIAS_CON_VENTA
        Intervalo medio entre ventas. Alto => demanda con muchas pausas.
    CV2 = (std(cantidad) / mean(cantidad)) ** 2
        Variabilidad relativa del tamano de venta (solo dias con venta).

    Tipologias (umbrales parametrizables)
    -------------------------------------
    - SUAVE        : ADI <  umbral_adi y CV2 <  umbral_cv2.
    - ERRATICA     : ADI <  umbral_adi y CV2 >= umbral_cv2.
    - INTERMITENTE : ADI >= umbral_adi y CV2 <  umbral_cv2.
    - GRUMOSA      : ADI >= umbral_adi y CV2 >= umbral_cv2.

    Parametros
    ----------
    historial_ventas : pd.DataFrame
        Debe contener EAN, P_DATE y CANTIDAD_TOTAL.
    umbral_adi : float, default=1.32
        Corte de ADI que separa demanda regular de intermitente.
    umbral_cv2 : float, default=0.49
        Corte de CV2 que separa volumen estable de disperso.

    Retorna
    -------
    tuple[pd.DataFrame, dict]
        tipologia (una fila por EAN) y parametros usados.
    """
    columnas = {'EAN', 'P_DATE', 'CANTIDAD_TOTAL'}
    faltantes = columnas.difference(historial_ventas.columns)
    if faltantes:
        msg = f'Faltan columnas: {sorted(faltantes)}'
        raise ValueError(msg)
    if umbral_adi <= 0 or umbral_cv2 < 0:
        msg = 'umbral_adi debe ser > 0 y umbral_cv2 debe ser >= 0.'
        raise ValueError(
            msg
        )

    trabajo = historial_ventas[list(columnas)].copy()
    trabajo['EAN_KEY'] = (
        trabajo['EAN'].astype('string').str.strip()
        .str.replace(r'\.0$', '', regex=True)
    )
    trabajo['P_DATE'] = pd.to_datetime(
        trabajo['P_DATE'], errors='coerce'
    ).dt.normalize()
    trabajo['CANTIDAD_TOTAL'] = pd.to_numeric(
        trabajo['CANTIDAD_TOTAL'], errors='coerce'
    )
    trabajo = trabajo.dropna(
        subset=['EAN_KEY', 'P_DATE', 'CANTIDAD_TOTAL']
    )
    trabajo = trabajo.loc[trabajo['CANTIDAD_TOTAL'].gt(0)]
    if trabajo.empty:
        msg = 'No hay registros con venta positiva.'
        raise ValueError(msg)

    # Consolidacion EAN-dia (una venta por dia calendario).
    consolidado = (
        trabajo
        .groupby(['EAN_KEY', 'P_DATE'], as_index=False)
        .agg(CANTIDAD=('CANTIDAD_TOTAL', 'sum'))
    )

    tipologia = (
        consolidado
        .groupby('EAN_KEY', as_index=False)
        .agg(
            N_DIAS_CON_VENTA=('P_DATE', 'nunique'),
            CANTIDAD_MEDIA=('CANTIDAD', 'mean'),
            CANTIDAD_STD=('CANTIDAD', 'std'),
            FECHA_PRIMERA_VENTA=('P_DATE', 'min'),
            FECHA_ULTIMA_VENTA=('P_DATE', 'max'),
        )
    )

    # Dias observados = ventana calendario entre primera y ultima venta.
    tipologia['N_DIAS_OBSERVADOS'] = (
        (tipologia['FECHA_ULTIMA_VENTA']
         - tipologia['FECHA_PRIMERA_VENTA']).dt.days + 1
    )
    tipologia['ADI'] = (
        tipologia['N_DIAS_OBSERVADOS'] / tipologia['N_DIAS_CON_VENTA']
    )
    tipologia['CV2'] = (
        tipologia['CANTIDAD_STD'] / tipologia['CANTIDAD_MEDIA']
    ).pow(2)
    # Un solo dia de venta => std NaN; CV2 no definido, se asume 0.
    tipologia['CV2'] = tipologia['CV2'].fillna(0.0)

    adi_alta = tipologia['ADI'].ge(umbral_adi)
    cv2_alto = tipologia['CV2'].ge(umbral_cv2)
    tipologia['TIPOLOGIA_DEMANDA'] = np.select(
        [
            ~adi_alta & ~cv2_alto,
            ~adi_alta & cv2_alto,
            adi_alta & ~cv2_alto,
        ],
        ['SUAVE', 'ERRATICA', 'INTERMITENTE'],
        default='GRUMOSA',
    )

    tipologia = (
        tipologia
        .rename(columns={'EAN_KEY': 'EAN'})
        .sort_values(['TIPOLOGIA_DEMANDA', 'ADI'], ascending=[True, False])
        .reset_index(drop=True)
    )

    parametros = {
        'UMBRAL_ADI': umbral_adi,
        'UMBRAL_CV2': umbral_cv2,
    }
    return tipologia, parametros


#####-----1.10 Creación de Excel y Subida a SP

def _sanitize_value(value, col_name=None):
    """Convierte valores no soportados por Excel (listas, dicts,
    numpy types, NaN) a algo compatible."""
    if isinstance(value, list | tuple | set | dict):
        return str(value)
    if isinstance(value, np.generic):
        value = value.item()
    try:
        if pd.isna(value):
            return None
    except (TypeError, ValueError):
        pass

    # Truncar valores de fecha/hora a solo la fecha para cols especificas
    if col_name in COLUMNAS_SOLO_FECHA:
        if isinstance(value, pd.Timestamp):
            return value.date()
        if hasattr(value, 'date') and callable(getattr(value, 'date')):  # noqa: B009
            return value.date()
        if isinstance(value, str):
            return value.split(' ')[0]

    return value


def generar_excel_buffer_segmentado(df: pd.DataFrame) -> io.BytesIO:
    """Genera el Excel segmentado y formateado, devolviendo un buffer
    en memoria."""
    wb = Workbook()
    ws = wb.active
    ws.title = 'Reporte'

    columnas_finales = []
    info_columnas = []  # (nombre_col, nombre_segmento, color, color_header)

    for seg_nombre, seg_info in segmentos_config.items():
        for col in seg_info['columnas']:
            if col in df.columns:
                columnas_finales.append(col)
                info_columnas.append((col, seg_nombre, seg_info['color'], seg_info['color_header']))  # noqa: E501

    # Forzar EAN como 1a columna y PRODUCT_DESCRIPTION como 2a;
    # el resto sigue igual.
    columnas_prioritarias = ['EAN', 'PRODUCT_DESCRIPTION']
    presentes = [col for col in columnas_prioritarias if col in columnas_finales]
    resto = [col for col in columnas_finales if col not in presentes]
    columnas_finales = presentes + resto

    # Reordenar tambien la metadata para que colores/segmentos
    # sigan a su columna
    orden = {col: pos for pos, col in enumerate(columnas_finales)}
    info_columnas = sorted(info_columnas, key=lambda item: orden[item[0]])

    df_final = df[columnas_finales]

    logging.info(f'Dimensiones finales df: {df_final.shape}')

    thin_border = Border(
        left=Side(style='thin', color='B7B7B7'),
        right=Side(style='thin', color='B7B7B7'),
        top=Side(style='thin', color='B7B7B7'),
        bottom=Side(style='thin', color='B7B7B7'),
    )
    center_align = Alignment(horizontal='center', vertical='center', wrap_text=True)

    # Fila 1: nombre del segmento (merge de celdas contiguas
    # del mismo segmento)
    col_idx = 1
    i = 0
    while i < len(info_columnas):
        seg_actual = info_columnas[i][1]
        color_header = info_columnas[i][3]
        j = i
        while j < len(info_columnas) and info_columnas[j][1] == seg_actual:
            j += 1
        start_col = col_idx
        end_col = col_idx + (j - i) - 1

        if end_col > start_col:
            ws.merge_cells(start_row=1, start_column=start_col, end_row=1, end_column=end_col)
        cell = ws.cell(row=1, column=start_col, value=seg_actual)
        cell.font = Font(bold=True, color='FFFFFF', size=12)
        cell.fill = PatternFill(start_color=color_header, end_color=color_header, fill_type='solid')  # noqa: E501
        cell.alignment = center_align

        for c in range(start_col, end_col + 1):
            cc = ws.cell(row=1, column=c)
            cc.fill = PatternFill(start_color=color_header, end_color=color_header, fill_type='solid')  # noqa: E501
            cc.border = thin_border

        col_idx = end_col + 1
        i = j

    # Fila 2: nombres de columnas
    for idx, (col_name, seg_nombre, color, color_header) in enumerate(info_columnas, start=1):  # noqa: B007
        cell = ws.cell(row=2, column=idx, value=col_name)
        cell.font = Font(bold=True, size=10)
        cell.fill = PatternFill(start_color=color, end_color=color, fill_type='solid')
        cell.alignment = center_align
        cell.border = thin_border

    # Filas de datos
    for r, (_, row) in enumerate(df_final.iterrows(), start=3):
        for c, (col_name, seg_nombre, color, color_header) in enumerate(info_columnas, start=1):  # noqa: B007
            value = _sanitize_value(row[col_name], col_name=col_name)
            cell = ws.cell(row=r, column=c, value=value)
            cell.alignment = center_align
            cell.border = thin_border
            cell.fill = PatternFill(start_color=color, end_color=color, fill_type='solid')
            if col_name in COLUMNAS_SOLO_FECHA and value is not None:
                cell.number_format = 'YYYY-MM-DD'

    # Ajustar ancho de columnas automaticamente
    for idx, (col_name, seg_nombre, color, color_header) in enumerate(info_columnas, start=1):  # noqa: B007
        col_letter = get_column_letter(idx)

        if col_name in ANCHOS_PERSONALIZADOS:
            ws.column_dimensions[col_letter].width = ANCHOS_PERSONALIZADOS[col_name]
            continue

        try:
            max_len_datos = df_final[col_name].astype(str).map(len).max() if len(df_final) > 0 else 0  # noqa: E501
        except Exception:  # noqa: BLE001
            max_len_datos = 0
        max_len = max(len(str(col_name)), max_len_datos)
        ws.column_dimensions[col_letter].width = min(max(max_len + 4, 12), 35)

    # Alturas de fila para encabezados
    ws.row_dimensions[1].height = 24
    ws.row_dimensions[2].height = 30

    # Congelar paneles: fija (EAN, PRODUCT_DESCRIPTION)
    # y las 2 filas de encabezado. El orden ya se garantiza arriba.
    ws.freeze_panes = 'C3'

    buffer = io.BytesIO()
    wb.save(buffer)
    buffer.seek(0)
    return buffer

# ----------------------------------------------------------------
# 2. Funciones de gestion e integracion con SharePoint
# ----------------------------------------------------------------
def listar_archivos_sharepoint(outputs_dir: str, sp_cred: dict) -> list:
    """Devuelve la lista de nombres de archivo existentes en la
      carpeta de SharePoint."""
    carpeta = sp.SharePointFolder(**sp_cred, server_relative_folder=outputs_dir)
    return carpeta.fileList()


def generar_nombre_no_duplicado(
    nombre_base: str,
    extension: str,
    outputs_dir: str,
    sp_cred: dict
) -> str:
    """Verifica si nombre_base.extension ya existe en SharePoint.
    Si existe, agrega sufijo _v{i} incremental hasta encontrar uno libre.
    """
    archivos_existentes = listar_archivos_sharepoint(outputs_dir, sp_cred)

    nombre_candidato = f'{nombre_base}.{extension}'
    if nombre_candidato not in archivos_existentes:
        return nombre_candidato

    i = 1
    while True:
        nombre_candidato = f'{nombre_base}_v{i}.{extension}'
        if nombre_candidato not in archivos_existentes:
            return nombre_candidato
        i += 1


def subir_archivo_sharepoint(
    contenido: io.BytesIO,
    nombre_archivo: str,
    outputs_dir: str,
    sp_cred: dict
) -> None:
    """Sube un archivo a SharePoint usando un buffer en memoria."""
    contenido.seek(0)

    output_remote_path = posixpath.join(outputs_dir, nombre_archivo)

    logging.info(f'Subiendo archivo a SharePoint: {output_remote_path}')

    sp_output = sp.SharePointFile(
        **sp_cred,
        server_relative_path=output_remote_path
    )

    sp_output.upload(content=contenido)

    logging.info('Archivo subido correctamente a SharePoint')


def exportar_y_subir_excel_segmentado(
    df: pd.DataFrame,
    execution_date: str,
    outputs_dir: str,
    sp_cred: dict,
    nombre_base_prefijo: str = 'reporte_segmentado'
) -> None:
    """Flujo completo: genera el excel segmentado en memoria,
    calcula un nombre no duplicado en SharePoint, y lo sube.
    """
    nombre_base = f'{nombre_base_prefijo}_{execution_date}'
    nombre_final = generar_nombre_no_duplicado(
        nombre_base=nombre_base,
        extension='xlsx',
        outputs_dir=outputs_dir,
        sp_cred=sp_cred
    )

    buffer = generar_excel_buffer_segmentado(df)

    subir_archivo_sharepoint(
        contenido=buffer,
        nombre_archivo=nombre_final,
        outputs_dir=outputs_dir,
        sp_cred=sp_cred
    )

    logging.info(f'Proceso finalizado. Archivo final: {nombre_final}')



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
    outputs_dir = posixpath.join(file_site, 'Caracterización_productos')

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

    #Primer Merge de Resultados Resumen
    resumen_global = pd.merge(  # noqa: PD015
        left=ventas_por_producto,
        right=caracterizacion_caidas.drop('SEGMENTO_ABCD', axis=1),
        on='EAN',
        how='inner')

    #------------ 4.3 Intermitencias ----------#

    logging.info('[4.3] Detección de intermitencias: ')
    periodos_inusuales, resumen_productos, _ = (  # noqa: RUF059
        identificar_periodos_sin_venta_inusuales(
            df_historial,
            ventas_por_producto,
            segmentos=['A', 'B', 'C', 'D'],              # solo A y B
            minimo_dias_sin_venta=28,          # piso de 3 semanas
            multiplicador_p90=2.5,
            minimo_intervalos_referencia=3,
            incluir_gap_final=False,
            cobertura_maxima_fantasma=50.0,    # mas estricto para "fantasma"
            ratio_minimo_muerte_reciente=0.60, # mas estricto para "muerte reciente"
        ))

    logging.info('[4.3] Zona concentración pausas: ',resumen_productos['ZONA_CONCENTRACION_PAUSAS'].value_counts() )  # noqa: E501

    #Segundo Merge de Resultados Resumen
    resumen_global = pd.merge(  # noqa: PD015
    left=resumen_global,
    right=resumen_productos.drop(['SEGMENTO_ABCD', 'DIAS_CON_VENTA', 'PRIMERA_VENTA', 'ULTIMA_VENTA'], axis=1),  # noqa: E501
    on='EAN',
    how='inner')

    #-------------- 4.4 Productos Nuevos ----------#

    logging.info('[4.4] Detección de productos nuevos: ')
    productos_nuevos, _ = detectar_productos_nuevos(
        df_historial, dias_producto_nuevo=180
    )

    #Terce Merge de Resultados Resumen
    resumen_global = pd.merge(  # noqa: PD015
        left=resumen_global,
        right=productos_nuevos[['EAN','ES_PRODUCTO_NUEVO']],
        on='EAN',
        how='inner'
    )

    nuevos_por_segmento = (
        productos_nuevos
        .merge(
            ventas_por_producto[['EAN', 'SEGMENTO_ABCD']],
            on='EAN',
            how='left',
        )
        .groupby('SEGMENTO_ABCD', observed=True)['ES_PRODUCTO_NUEVO']
        .sum()
        .reset_index(name='N_PRODUCTOS_NUEVOS')
    )

    logging.info('[4.4] Productos nuevos por segmento: ', nuevos_por_segmento)

    #----------------- 4.5 Variabilidad -------------#

    logging.info('[4.5] Variabilidad Precio y Demanda')
    variabilidad_producto, _ = caracterizar_variabilidad(df_historial)

    logging.info('[4.5] Revisión demanda muy variable por segmento: ',variabilidad_producto.merge(
        ventas_por_producto[['EAN', 'SEGMENTO_ABCD']],
        on='EAN',
        how='left').groupby('SEGMENTO_ABCD', observed=True)['FLAG_DEMANDA_MUY_VARIABLE'].sum().reset_index(name='N_DEMANDA_MUY_VARIABLE')) # noqa: E501

    #Cuarto Merge de Resultados Resumen
    resumen_global = pd.merge(  # noqa: PD015
        left=resumen_global,
        right=variabilidad_producto.drop('DIAS_CON_VENTA_EVALUADOS', axis=1),
        on='EAN',
        how='inner')

    #------------------ 4.6 Caracterización Promocional ------------ #
    logging.info('[4.6] Caracterización Promocional')

    df_caracterizacion_promos, _ = caracterizar_promociones_producto(
        df_historial,
        sufijo_promo='B',
        umbral_uplift=0.40,
        umbral_regularizacion=0.80,
        incluir_mecanica=True,
        minimo_participacion_mecanica=0.10,
        minimo_mecanicas_distintas=2)

    #Quinto Merge de Resultados Resumen
    resumen_global = pd.merge(  # noqa: PD015
        left=resumen_global,
        right=df_caracterizacion_promos.drop('N_DIAS_OBSERVADOS', axis=1),
        on='EAN',
        how='inner')

    #------------- 4.7 Segmentación Variabilidad: ADI /CV^2 -------------#
    logging.info('[4.7] Segmentación ADI/CV^2')
    df_tipologia_demanda, _ = clasificar_tipologia_demanda(df_historial)

    resumen_global = pd.merge(  # noqa: PD015
        left=resumen_global,
        right=df_tipologia_demanda.drop('N_DIAS_CON_VENTA', axis=1),
        on='EAN',
        how='inner')

    logging.info('[4.7] Frecuencia Segmentación ADI/CV^2 :',df_tipologia_demanda['TIPOLOGIA_DEMANDA'].value_counts())  # noqa: E501


    logging.info('[5] Columnas Resumen Global: ', resumen_global.columns)

    logging.info('[6] Subida a SP...')
    print('shape: ', resumen_global.shape)
    exportar_y_subir_excel_segmentado(
        df=resumen_global,
        execution_date=execution_date,
        outputs_dir=outputs_dir,
        sp_cred=sp_cred
    )
if __name__ == '__main__':

    main()
