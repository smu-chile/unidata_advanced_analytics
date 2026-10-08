"""Tablón de regresión a nivel (CATEGORIA, SUBCATEGORIA, MARCA).

Reemplaza a `processed_regression_data_marca.py`. Cambios:

1. Nivel de agregación: la marca se agrega DENTRO de su subcategoría
   (y categoría). La versión anterior agregaba solo por marca y después
   le pegaba la categoría: una marca con productos en 2 o más categorías
   quedaba con la cantidad y la venta de todas juntas, repetidas en cada
   categoría (y con filas duplicadas).
2. `FRAC_PROMO` y `DESC_PROMOCIONAL`: el cruce con TMP_PROMOTION_DAILY y
   con las campañas apoteósicas comparaba el código de producto del
   panel (`SKU_PRODUCT`, con ceros a la izquierda) contra un código sin
   ceros (la tabla de promociones usa `LTRIM(SKU_PRODUCT, '0')`), por lo
   que nunca coincidían y ambas columnas salían siempre en 0. Ahora los
   dos lados se normalizan a entero y el script falla si no hay ningún
   material en común.
3. Feriados: se agrega 2026 (antes terminaba en 2025-12-25).
4. Pares: `VARIACION_PORCENTUAL_SUBCATEGORIA` compara cada marca contra
   las demás marcas de su subcategoría-día; se mantiene
   `VARIACION_PORCENTUAL_CATEGORIA` (demás unidades de la categoría).
5. `PARTICIPACION_SUBCATEGORIA` y `PARTICIPACION_CATEGORIA`.

Archivo autocontenido -- Dataproc Serverless solo empaqueta
python_script_path + include_paths.

Nunca se ejecuta contra GCP desde este entorno -- es un script de
referencia para que el usuario ejecute en su propio proyecto.
"""
from __future__ import annotations  # noqa: I001

import argparse
import logging
import os
from dataclasses import dataclass
from logging import config
from string import Template

import numpy as np
import pandas as pd
import pendulum
from common.constants import LOGGING_CONFIG
from common.gcp_extended.bigquery import (
    createTableAsSelect,
    deleteFromTable,
    readBigQuery,
    setTableExpiration,
    uploadFrame,
)
from google.cloud.bigquery import Client

config.dictConfig(LOGGING_CONFIG)


# ========================================================================
# Logica compartida entre processed_regression_data_marca.py y
# processed_regression_data_subcategoria.py (antes en tablon_agregado_comun
# -- fusionada aca porque Dataproc Serverless no empaqueta modulos locales
# que no esten en include_paths; solo se envia python_script_path + lo
# listado ahi). Mantener los 2 runners sincronizados a mano si se edita
# esta seccion en cualquiera de los 2.
# =========================================================================

COBERTURA_MINIMA = 0.8  # inconsistencia (2) -- ver docstring del modulo
MIN_DIAS_PESO = 5  # dias minimos de venta en el mes previo para tener peso
FERIADOS = pd.to_datetime(
    [
        '2023-01-01', '2023-04-07', '2023-05-01', '2023-05-21', '2023-06-26',
        '2023-07-16', '2023-08-15', '2023-09-18', '2023-09-19', '2023-10-09',
        '2023-10-27', '2023-11-01', '2023-12-08', '2023-12-25',
        '2024-01-01', '2024-03-29', '2024-05-01', '2024-05-21', '2024-06-20',
        '2024-07-16', '2024-08-15', '2024-09-18', '2024-09-19', '2024-10-12',
        '2024-10-31', '2024-11-01', '2024-12-08', '2024-12-25',
        '2025-01-01', '2025-04-18', '2025-05-01', '2025-05-21', '2025-06-20',
        '2025-07-16', '2025-08-15', '2025-09-18', '2025-09-19', '2025-10-12',
        '2025-10-31', '2025-11-01', '2025-12-08', '2025-12-25',
        # 2026: el calendario original terminaba en 2025-12-25 y dejaba
        # CERCA_FERIADO = 0 todo 2026. Revisar vs calendario oficial.
        '2026-01-01', '2026-04-03', '2026-05-01', '2026-05-21', '2026-06-21',
        '2026-06-29', '2026-07-16', '2026-08-15', '2026-09-18', '2026-09-19',
        '2026-10-12', '2026-10-31', '2026-11-01', '2026-12-08', '2026-12-25',
    ]
)
VENTANA_FERIADO_DIAS = 3


@dataclass(frozen=True)
class ConfiguracionNivel:
    """Parametros que distinguen el tablon de marca del de subcategoria."""

    columna_origen: str  # columna en VW_DIM_PRODUCT (ej. 'BRAND_DESC')
    columna_salida: str  # alias en el tablon final (ej. 'BRAND_DESCRIPTION')
    nombre_grupo: str  # nombre de negocio, para logs ('marca', 'subcategoria')
    tabla_destino: str  # tabla final en PRECIO_PROMOCIONES
    nombre_json: str  # esquema de ingesta


# -----------------------------------------------------------------------
# SQL -- templates compartidos, parametrizados por ConfiguracionNivel
# -----------------------------------------------------------------------
def construir_query_master_table(cfg: ConfiguracionNivel) -> Template:
    """Mismo query_master_table del script SKU, agregando la columna de grupo.

    Unica diferencia real respecto al original: se agrega
    `cfg.columna_origen AS cfg.columna_salida` a `distinct_products` y a
    la consulta principal -- el resto de la logica (EAN canonico,
    filtros de VW_SALES_ITEM) es identica.

    Caso especial: si el nivel de agregacion ES subcategoria,
    `SUB_CATEGORY_DESCRIPTION` ya existe como columna nativa (GRUPO_DSC) en
    la query original -- no se duplica el alias, se reutiliza la columna
    ya presente en vez de inyectar una linea adicional.
    """  # noqa: W505
    ya_es_nativa = cfg.columna_salida == 'SUB_CATEGORY_DESCRIPTION'
    linea_columna_grupo = (
        '' if ya_es_nativa else f'{cfg.columna_origen} AS {cfg.columna_salida},\n    '
    )
    return Template(f"""
WITH distinct_products AS (
  SELECT DISTINCT
    EAN,
    CAT_DSC AS CATEGORY_DESCRIPTION,
    GRUPO_DSC AS SUB_CATEGORY_DESCRIPTION,
    {linea_columna_grupo}NM AS PRODUCT_DESCRIPTION,
    SKU_PRODUCT AS PRODUCT_ID,
    NEG_DSC,
    CONTENIDO_BRUTO,
    CONT_CONV_UMB AS sales_unit,
    UNIDAD_DE_MEDIDA AS sales_uom,
    FIRST_VALUE(EAN) OVER (
      PARTITION BY SKU_PRODUCT, UNIDAD_DE_MEDIDA
      ORDER BY CASE WHEN INDIC_EAN_PPAL = 'X' THEN 0 ELSE 1 END, EAN
    ) AS ean_default
  FROM `$proyecto.CDA_VISTAS.VW_DIM_PRODUCT`
)

SELECT
  A.CUSTOMER_KEY AS CUSTOMER_ID,
  A.STORE_ID,
  A.MARKET_BASKET_KEY,
  P.PRODUCT_DESCRIPTION,
  P.PRODUCT_ID,
  P.ean_default AS EAN,
  P.CATEGORY_DESCRIPTION,
  P.SUB_CATEGORY_DESCRIPTION,
  {'' if ya_es_nativa else f'P.{cfg.columna_salida},'}
  A.QUANTITY,
  A.VALUE,
  P.sales_uom,
  P.sales_unit,
  CAST(P.CONTENIDO_BRUTO AS NUMERIC) * CAST(P.sales_unit AS INTEGER) AS WEIGHT_UPC,
  CAST(A.WEIGHT AS NUMERIC) AS SALE_WEIGHT,
  A.TRANSACTION_DATE AS P_DATE
FROM `$proyecto.CDA_VISTAS.VW_SALES_ITEM` A
INNER JOIN distinct_products P
  ON A.EAN = P.EAN
INNER JOIN `$proyecto.CDA_VISTAS.VW_DIM_STORE` D
  ON A.STORE_ID = D.STORE_ID
WHERE
  A.TRANSACTION_DATE >= DATE('$fecha_inicial')
  AND A.TRANSACTION_DATE <= DATE('$fecha_final')
  AND A.SKU_PRODUCT IS NOT NULL
  AND A.SKU_PRODUCT != 'None'
  AND A.TRANSACTION_TYPE IN ('BX', 'BE', 'TF')
  AND A.ITM_TXN_FCN_TP_DSC = 'V'
  AND A.UNIT_PRICE > 0
  AND A.VALUE > 0
  AND P.NEG_DSC NOT IN ('SERVICIOS COMERCIALES', 'NO RETAIL', 'None')
  AND P.{cfg.columna_salida} IS NOT NULL
  AND A.MARKET_BASKET_KEY NOT IN (
    SELECT MARKET_BASKET_KEY
    FROM `cl-cda-prod.DS_CDA_VW_SMU.DW_VW_FACT_MARKET_BASKET_E_COMMERCE`
    WHERE CANAL_VENTA IN ('PEDIDOS YA','UBER EATS','RAPPI','RAPPI TURBO')
  )
  AND D.STORE_BANNER = '$store_banner'
""")  # noqa: S608


def construir_query_sku_diario(cfg: ConfiguracionNivel) -> Template:
    """Panel SKU-dia, igual formula que query_principal del script original.

    No agrega todavia al nivel de grupo -- esta es la pieza intermedia
    (precio y cantidad por SKU, cada dia) sobre la que despues se
    calculan pesos mensuales y el indice de precio del grupo. Mismas
    formulas exactas de cantidad_total/precio_promedio que el script SKU,
    solo que ahora se conserva tambien la columna de grupo.

    Caso especial: si el grupo ES subcategoria, no se duplica
    SUB_CATEGORY_DESCRIPTION (ver `construir_query_master_table`).
    """  # noqa: W505
    ya_es_nativa = cfg.columna_salida == 'SUB_CATEGORY_DESCRIPTION'
    columna_grupo_select = '' if ya_es_nativa else f'{cfg.columna_salida},\n  '
    return Template(f"""
SELECT
  P_DATE,
  CATEGORY_DESCRIPTION,
  SUB_CATEGORY_DESCRIPTION,
  {columna_grupo_select}PRODUCT_ID,
  sales_uom,
  sales_unit,
  SUM(`VALUE`) AS ventas_totales_sku,

  SUM(
    CASE
      WHEN sales_uom IN ('KG','KGV') THEN SALE_WEIGHT
      ELSE SAFE_DIVIDE(QUANTITY, CAST(sales_unit AS INT64))
    END
  ) AS cantidad_total_sku,

  AVG(
    SAFE_DIVIDE(
      `VALUE`,
      CASE
        WHEN sales_uom IN ('KG','KGV') THEN SALE_WEIGHT
        ELSE QUANTITY / CAST(sales_unit AS INT64)
      END
    )
  ) AS precio_promedio_sku

FROM $table_master
GROUP BY
  CATEGORY_DESCRIPTION,
  SUB_CATEGORY_DESCRIPTION,
  {columna_grupo_select}PRODUCT_ID,
  sales_unit,
  sales_uom,
  P_DATE
""")  # noqa: S608


QUERY_DIAS_VENTA_MAYOR = Template("""
WITH VentasPorFecha AS (
  SELECT
    CATEGORY_DESCRIPTION,
    P_DATE,
    FORMAT_DATE('%A', P_DATE) AS dia_semana,
    SUM(VALUE) AS ventas_totales_producto
  FROM $table_master
  GROUP BY CATEGORY_DESCRIPTION, P_DATE
),
Promedios AS (
  SELECT
    CATEGORY_DESCRIPTION,
    dia_semana,
    AVG(ventas_totales_producto) AS promedio_ventas_dia
  FROM VentasPorFecha
  GROUP BY CATEGORY_DESCRIPTION, dia_semana
),
Resultados AS (
  SELECT
    a.CATEGORY_DESCRIPTION,
    a.P_DATE,
    a.dia_semana,
    a.ventas_totales_producto,
    b.promedio_ventas_dia,
    CASE
      WHEN a.ventas_totales_producto >= 6 * b.promedio_ventas_dia THEN 'x6'
      WHEN a.ventas_totales_producto >= 5 * b.promedio_ventas_dia THEN 'x5'
      WHEN a.ventas_totales_producto >= 4 * b.promedio_ventas_dia THEN 'x4'
      WHEN a.ventas_totales_producto >= 3 * b.promedio_ventas_dia THEN 'x3'
      WHEN a.ventas_totales_producto >= 2 * b.promedio_ventas_dia THEN 'x2'
      WHEN a.ventas_totales_producto >= 1.5 * b.promedio_ventas_dia THEN 'x1.5'
      WHEN a.ventas_totales_producto >= 0.5 * b.promedio_ventas_dia THEN 'x1'
      ELSE 'x0.5'
    END AS Multiplicador
  FROM VentasPorFecha a
  JOIN Promedios b
    ON a.CATEGORY_DESCRIPTION = b.CATEGORY_DESCRIPTION
   AND a.dia_semana = b.dia_semana
)
SELECT
  CATEGORY_DESCRIPTION,
  P_DATE,
  dia_semana,
  ventas_totales_producto / promedio_ventas_dia AS proporcion_categoria,
  Multiplicador
FROM Resultados
WHERE Multiplicador IS NOT NULL
ORDER BY CATEGORY_DESCRIPTION, P_DATE
""")

QUERY_APOTEOSICO = Template("""
SELECT material, fecha_inicio_de_promocion, fecha_fin_de_promocion
FROM `cl-cda-prod.DS_CDA_VW_SMU.DW_VW_FACT_WORKFLOW`
WHERE FECHA_INICIO_DE_PROMOCION > DATE('$fecha_inicial_ano')
  AND FECHA_INICIO_DE_PROMOCION < DATE_ADD(DATE('$fecha_inicial_ano'),
  INTERVAL $cant_meses MONTH)
  AND descripcion_evento_promocional = 'UNI APOTEOSICO'
  AND registro_valido = 'X'
  AND organizacion_ventas = '$store_banner_codigo'
  AND canal_distribucion = '10'
ORDER BY fecha_fin_de_promocion
""")

QUERY_PROMO_DIARIA = Template("""
SELECT
  CAST(material AS STRING) AS material,
  p_date,
  atributo_promocion
FROM `$proyecto.PRECIO_PROMOCIONES.TMP_PROMOTION_DAILY`
WHERE store_banner = '$store_banner'
  AND p_date BETWEEN DATE('$fecha_inicial') AND DATE('$fecha_final')
""")



# -----------------------------------------------------------------------
# Python -- funciones de construccion, probables con datos sinteticos
# -----------------------------------------------------------------------
CLAVES_SUBCATEGORIA = ['category_description', 'sub_category_description']
CLAVES_GRUPO = [*CLAVES_SUBCATEGORIA, 'brand_description']


def normalizar_material(serie: pd.Series) -> pd.Series:
    """Codigo de producto como entero, sin ceros a la izquierda.

    El panel trae `SKU_PRODUCT` como texto con ceros a la izquierda; la
    tabla de promociones (`LTRIM(SKU_PRODUCT, '0')`) y el tablon de SKU
    (`astype(int)`) lo usan sin ceros. Los dos lados del cruce deben
    pasar por esta funcion.
    """
    return pd.to_numeric(serie, errors='coerce').astype('Int64')


def desplazar_mes(p_month: pd.Series, desfase: int) -> pd.Series:
    """Desplaza un p_month (YYYYMM, entero) hacia atras `desfase` meses.

    Misma aritmetica exacta que `generarMesesPrevios` del script
    original, extraida como funcion reutilizable (ahi estaba inline dentro
    del loop de desfases 1/2/3).
    """
    anio = p_month // 100
    mes = p_month % 100 - desfase
    anio = anio - (mes <= 0).astype(int)
    mes = (mes - 1) % 12 + 1
    return anio * 100 + mes


def construir_unidad_dominante(
    df_sku_diario: pd.DataFrame, claves: list[str]
) -> pd.DataFrame:
    """Unidad de medida dominante por grupo, fija sobre todo el historico.

    Devuelve 1 fila por grupo: la sales_uom con mayor venta historica
    (en VALUE) y `pct_valor_unidad_dominante` -- la fraccion de venta del
    grupo que efectivamente queda cubierta al quedarse solo con esa
    unidad (el resto se excluye del tablon).
    """
    valor_por_uom = df_sku_diario.groupby([*claves, 'sales_uom'])[
        'ventas_totales_sku'
    ].sum()
    valor_total_grupo = valor_por_uom.groupby(level=claves).transform('sum')
    pct = (valor_por_uom / valor_total_grupo).rename('pct_valor_unidad_dominante')
    tabla = pct.reset_index()
    idx_dominante = tabla.groupby(claves)['pct_valor_unidad_dominante'].idxmax()
    dominante = tabla.loc[idx_dominante].reset_index(drop=True)
    return dominante.rename(columns={'sales_uom': 'sales_uom_dominante'})


def construir_pesos_mensuales(
    df_sku_diario: pd.DataFrame, claves: list[str], min_dias: int = MIN_DIAS_PESO
) -> pd.DataFrame:
    """Peso de cada SKU dentro de su grupo, recalculado cada mes.

    El peso de un SKU en el mes M se calcula con su venta (VALUE) del mes
    M-1 respecto al total del grupo en M-1 -- nunca con datos del propio
    mes que se esta ponderando, para que el peso sea exogeno.

    Caso especial: el primer mes del historico no tiene mes anterior
    disponible. Para ESE mes unicamente, se usa su propio mes como
    ventana de pesos -- queda marcado en `pesos_con_fallback`.
    """
    columnas = [*claves, 'product_id', 'p_month', 'peso', 'pesos_con_fallback']
    diario = df_sku_diario.copy()
    diario['p_month'] = diario['p_date'].dt.year * 100 + diario['p_date'].dt.month

    valor_mensual = diario.groupby([*claves, 'product_id', 'p_month']).agg(
        valor_mes=('ventas_totales_sku', 'sum'),
        dias_con_venta=('ventas_totales_sku', 'size'),
    ).reset_index()
    valor_mensual = valor_mensual[valor_mensual['dias_con_venta'] >= min_dias]

    primer_mes = diario['p_month'].min()
    meses_disponibles = np.sort(diario['p_month'].unique())

    piezas = []
    for p_month_objetivo in meses_disponibles:
        if p_month_objetivo == primer_mes:
            mes_fuente, fallback = p_month_objetivo, True
        else:
            mes_fuente = desplazar_mes(pd.Series([p_month_objetivo]), 1).iloc[0]
            fallback = False
        fuente = valor_mensual[valor_mensual['p_month'] == mes_fuente].copy()
        if fuente.empty:
            continue
        valor_total_grupo_mes = fuente.groupby(claves)['valor_mes'].transform('sum')
        fuente['peso'] = fuente['valor_mes'] / valor_total_grupo_mes
        fuente['p_month'] = p_month_objetivo
        fuente['pesos_con_fallback'] = fallback
        piezas.append(fuente[columnas])

    if not piezas:
        return pd.DataFrame(columns=columnas)
    return pd.concat(piezas, ignore_index=True)


def construir_indice_precio_grupo(
    df_sku_diario: pd.DataFrame,
    pesos: pd.DataFrame,
    claves: list[str],
    cobertura_minima: float = COBERTURA_MINIMA,
) -> pd.DataFrame:
    """Calcula el indice de precio diario: suma(w_i * ln P_i) / cobertura.

    Renormaliza por la fraccion de peso que efectivamente tiene precio
    observado ese dia; NaN si la cobertura no alcanza `cobertura_minima`.
    """
    diario = df_sku_diario.copy()
    diario['p_month'] = diario['p_date'].dt.year * 100 + diario['p_date'].dt.month
    llaves = [*claves, 'product_id', 'p_month']
    con_peso = diario.merge(pesos[[*llaves, 'peso']], on=llaves, how='inner')
    con_peso['ln_p'] = np.log(con_peso['precio_promedio_sku'])
    con_peso['aporte'] = con_peso['peso'] * con_peso['ln_p']

    agregado = con_peso.groupby([*claves, 'p_date']).agg(
        suma_ponderada=('aporte', 'sum'),
        cobertura=('peso', 'sum'),
    ).reset_index()
    agregado['ln_p_grupo'] = np.where(
        agregado['cobertura'] >= cobertura_minima,
        agregado['suma_ponderada'] / agregado['cobertura'],
        np.nan,
    )
    return agregado[[*claves, 'p_date', 'ln_p_grupo', 'cobertura']]


def construir_cantidad_y_cobertura_grupo(
    df_sku_diario: pd.DataFrame, claves: list[str]
) -> pd.DataFrame:
    """Cantidad total y n_sku_activos del grupo, por dia.

    cantidad_total es la SUMA directa (no ponderada) de la cantidad de
    cada SKU -- valido porque ya se filtro a la unidad de medida
    dominante antes de llegar aca (no se mezclan kg con unidades).
    """
    return df_sku_diario.groupby([*claves, 'p_date']).agg(
        cantidad_total=('cantidad_total_sku', 'sum'),
        ventas_totales_producto=('ventas_totales_sku', 'sum'),
        n_sku_activos=('product_id', 'nunique'),
    ).reset_index()


def construir_participacion(
    df_sku_diario: pd.DataFrame, claves: list[str]
) -> pd.DataFrame:
    """% de venta de la marca en su subcategoria y categoria, por mes."""
    diario = df_sku_diario.copy()
    diario['p_month'] = diario['p_date'].dt.year * 100 + diario['p_date'].dt.month
    por_grupo = (
        diario.groupby([*claves, 'p_month'])['ventas_totales_sku']
        .sum().rename('venta_grupo_mes').reset_index()
    )
    por_subcategoria = (
        diario.groupby([*CLAVES_SUBCATEGORIA, 'p_month'])['ventas_totales_sku']
        .sum().rename('venta_subcategoria_mes').reset_index()
    )
    por_categoria = (
        diario.groupby(['category_description', 'p_month'])['ventas_totales_sku']
        .sum().rename('venta_categoria_mes').reset_index()
    )
    tabla = por_grupo.merge(por_subcategoria, on=[*CLAVES_SUBCATEGORIA, 'p_month'])
    tabla = tabla.merge(por_categoria, on=['category_description', 'p_month'])
    tabla['participacion_subcategoria'] = (
        tabla['venta_grupo_mes'] / tabla['venta_subcategoria_mes']
    ).round(4)
    tabla['participacion_categoria'] = (
        tabla['venta_grupo_mes'] / tabla['venta_categoria_mes']
    ).round(4)
    return tabla[
        [*claves, 'p_month', 'participacion_subcategoria', 'participacion_categoria']
    ]


def construir_cerca_feriado(fechas: pd.Series) -> pd.Series:
    """Flag de calendario puro: dentro de +-VENTANA_FERIADO_DIAS de un feriado."""  # noqa: W505
    dias = (fechas - pd.Timestamp('1970-01-01')).dt.days.to_numpy()
    dias_feriado = np.sort((FERIADOS - pd.Timestamp('1970-01-01')).days.to_numpy())
    idx_derecha = np.searchsorted(dias_feriado, dias + VENTANA_FERIADO_DIAS, side='right')
    idx_izquierda = np.searchsorted(dias_feriado, dias - VENTANA_FERIADO_DIAS, side='left')
    return pd.Series(idx_derecha > idx_izquierda, index=fechas.index)


def diagnosticar_cruce_material(
    df_sku_diario: pd.DataFrame, df_externo: pd.DataFrame
) -> dict:
    """Cuantos materiales del panel aparecen en una tabla externa.

    Sirve para detectar cruces rotos (por ejemplo, ceros a la izquierda)
    antes de que la columna resultante salga en 0 sin avisar.
    """
    en_panel = set(normalizar_material(df_sku_diario['product_id']).dropna().unique())
    en_externo = set(normalizar_material(df_externo['material']).dropna().unique())
    return {
        'materiales_panel': len(en_panel),
        'materiales_externos': len(en_externo),
        'materiales_comunes': len(en_panel & en_externo),
    }


def _fraccion_de_venta_marcada(
    df_sku_diario: pd.DataFrame,
    df_marcas: pd.DataFrame,
    claves: list[str],
    nombre: str,
) -> pd.DataFrame:
    """% de venta del grupo-dia en SKU presentes en `df_marcas`."""
    diario = df_sku_diario.assign(material=normalizar_material(df_sku_diario['product_id']))
    marcados = (
        df_marcas.assign(material=normalizar_material(df_marcas['material']))
        .dropna(subset=['material'])[['material', 'p_date']]
        .drop_duplicates()
        .assign(marcado=1)
    )
    diario = diario.merge(marcados, on=['material', 'p_date'], how='left')
    diario['marcado'] = diario['marcado'].fillna(0)
    diario['valor_marcado'] = diario['marcado'] * diario['ventas_totales_sku']
    agregado = diario.groupby([*claves, 'p_date']).agg(
        valor_marcado=('valor_marcado', 'sum'),
        valor_total=('ventas_totales_sku', 'sum'),
    ).reset_index()
    agregado[nombre] = (agregado['valor_marcado'] / agregado['valor_total']).round(4)
    return agregado[[*claves, 'p_date', nombre]]


def construir_frac_promo(
    df_sku_diario: pd.DataFrame, df_promo: pd.DataFrame, claves: list[str]
) -> pd.DataFrame:
    """% de venta del grupo-dia en SKU con fila en TMP_PROMOTION_DAILY.

    Ausencia en TMP_PROMOTION_DAILY = precio regular (confirmado por el
    usuario) -- por eso un LEFT JOIN simple basta.
    """
    return _fraccion_de_venta_marcada(df_sku_diario, df_promo, claves, 'frac_promo')


def construir_desc_promocional(
    df_sku_diario: pd.DataFrame, df_apoteosico_material_fecha: pd.DataFrame, claves: list[str]
) -> pd.DataFrame:
    """% de venta del grupo-dia en campañas apoteosicas.

    Reemplaza el `apo` binario del script original (0/1 por material) por
    una proporcion de venta -- mismo criterio que frac_promo.
    """
    return _fraccion_de_venta_marcada(
        df_sku_diario, df_apoteosico_material_fecha, claves, 'desc_promocional'
    )


def construir_historico_precio(
    df_grupo_diario: pd.DataFrame, claves: list[str]
) -> pd.DataFrame:
    """Precio medio de los 3 meses anteriores y variacion %, por grupo.

    Mismo patron que `generarMesesPrevios` + el bloque de historico del
    script original, re-keyado al grupo. El precio usado es
    exp(ln_p_grupo) -- el nivel del indice de precio ya construido.
    """
    grupo_diario = df_grupo_diario.copy()
    grupo_diario['p_month'] = (
        grupo_diario['p_date'].dt.year * 100 + grupo_diario['p_date'].dt.month
    )
    grupo_diario['precio_grupo'] = np.exp(grupo_diario['ln_p_grupo'])

    precio_mensual = grupo_diario.groupby([*claves, 'p_month'])['precio_grupo'].mean()
    precio_mensual = precio_mensual.rename('precio_mensual').reset_index()

    piezas = []
    for desfase in (1, 2, 3):
        temp = precio_mensual[[*claves, 'p_month']].copy()
        temp['p_month_ref'] = temp['p_month']
        temp['p_month'] = desplazar_mes(temp['p_month_ref'], desfase)
        piezas.append(temp)
    meses_previos = pd.concat(piezas, ignore_index=True)

    fusion = meses_previos.merge(precio_mensual, on=[*claves, 'p_month'], how='left')
    historico = fusion.groupby([*claves, 'p_month_ref'])['precio_mensual'].mean()
    historico = historico.rename('precio_medio_anterior').reset_index()
    historico = historico.rename(columns={'p_month_ref': 'p_month'})

    grupo_diario = grupo_diario.merge(historico, on=[*claves, 'p_month'], how='left')
    # Escala porcentaje (x100), igual convencion que el script SKU original
    grupo_diario['variacion_porcentual'] = (
        (grupo_diario['precio_grupo'] - grupo_diario['precio_medio_anterior'])
        / grupo_diario['precio_medio_anterior']
        * 100
    ).fillna(0).round(2)
    return grupo_diario


def construir_variacion_pares(
    df_grupo_diario: pd.DataFrame, claves_pares: list[str], columna: str
) -> pd.DataFrame:
    """Variacion % promedio de los PARES del grupo, sin contar al propio.

    "Leave-one-out" ponderado por venta, igual que
    `variacion_porcentual_subcategoria` del script SKU. `claves_pares`
    define quienes son los pares (subcategoria-dia o categoria-dia).
    Si el grupo no tiene pares ese dia, queda 0 (misma convencion).
    """
    grupo_diario = df_grupo_diario.copy()
    peso = grupo_diario['ventas_totales_producto'].fillna(0)
    variacion = grupo_diario['variacion_porcentual'].fillna(0)
    grupo_diario['_aporte'] = variacion * peso

    claves = [*claves_pares, 'p_date']
    numerador_total = grupo_diario.groupby(claves)['_aporte'].transform('sum')
    denominador_total = grupo_diario.groupby(claves)['ventas_totales_producto'].transform('sum')

    numerador_excl = numerador_total - grupo_diario['_aporte']
    denominador_excl = denominador_total - peso

    with np.errstate(invalid='ignore', divide='ignore'):
        grupo_diario[columna] = numerador_excl / denominador_excl
    grupo_diario.loc[denominador_excl == 0, columna] = 0.0
    grupo_diario[columna] = grupo_diario[columna].round(2)
    return grupo_diario.drop(columns='_aporte')


def aplicar_filtro_frescura(
    df_grupo_diario: pd.DataFrame, claves: list[str], meses: int = 12
) -> pd.DataFrame:
    """Descarta grupos sin venta en el ultimo año (misma regla del script original)."""  # noqa: W505
    fecha_maxima = df_grupo_diario['p_date'].max()
    ultima_venta = df_grupo_diario.groupby(claves)['p_date'].transform('max')
    limite = fecha_maxima - pd.DateOffset(months=meses)
    return df_grupo_diario[ultima_venta >= limite].copy()


CFG = ConfiguracionNivel(
    columna_origen='BRAND_DESC',
    columna_salida='BRAND_DESCRIPTION',
    nombre_grupo='marca_subcategoria',
    tabla_destino='cl-bigdata-analytics-preprod.PRECIO_PROMOCIONES.'
                  'TMP_REGRESSION_PROCESSED_DATA_ELASTICITY_MARCA_SUBCATEGORIA',
    nombre_json='ingest_regression_processed_data_elasticity_marca_subcategoria.json',
)

parser = argparse.ArgumentParser()
parser.add_argument('--project_id', type=str, help='GCP project')
parser.add_argument('--execution_date', type=str, help='DAG execution date')
parser.add_argument('--store_banner', type=str, help='Store banner')


def main() -> None:  # noqa: D103
    args = vars(parser.parse_args())
    execution_date: str = args['execution_date']
    proyecto: str = args['project_id']
    store_banner: str = args['store_banner']

    logging.info(f'Tablon agregado -- nivel: {CFG.nombre_grupo}, banner: {store_banner}')

    gbq_client = Client()
    usuario = 'pricing'
    store_banner_tabla = 'Super_10' if store_banner == 'Super 10' else store_banner
    tmp_path_table_aux = (
        f'{proyecto}.TMP.TMP_REGRESSION_DATA_ELASTICITY_{CFG.nombre_grupo}_aux_'
        f'{store_banner_tabla}'
    )
    claves = CLAVES_GRUPO

    cant_meses = 29
    fecha_ejecucion = pendulum.parse(execution_date)
    fecha_final = fecha_ejecucion.start_of('month').subtract(days=1)
    fecha_inicial = fecha_final.subtract(months=cant_meses).add(months=1).start_of('month')

    # REGION: tabla maestra (logica del script SKU + marca)
    query_master = construir_query_master_table(CFG).substitute(
        fecha_inicial=fecha_inicial, fecha_final=fecha_final, proyecto=proyecto,
        store_banner=store_banner,
    )
    createTableAsSelect(
        query=query_master, gbq_client=gbq_client,
        table_ref=tmp_path_table_aux, use_legacy_sql=False,
    )
    setTableExpiration(
        table_ref=tmp_path_table_aux,
        expiration=pendulum.now().add(minutes=200),
        gbq_client=gbq_client,
    )
    logging.info('Tabla maestra creada...')

    # REGION: panel SKU-dia (pieza intermedia, no se sube a BigQuery)
    query_sku = construir_query_sku_diario(CFG).substitute(table_master=tmp_path_table_aux)
    df_sku_diario = readBigQuery(query=query_sku, user=usuario, gbq_client=gbq_client)
    df_sku_diario.columns = df_sku_diario.columns.str.lower()
    df_sku_diario['p_date'] = pd.to_datetime(df_sku_diario['p_date'])
    df_sku_diario = df_sku_diario[df_sku_diario['precio_promedio_sku'] > 0]
    df_sku_diario = df_sku_diario.dropna(subset=claves)
    logging.info(f'Panel SKU-dia: {len(df_sku_diario):,} filas')

    # REGION: unidad de medida dominante (fija, sobre todo el historico)
    dominante = construir_unidad_dominante(df_sku_diario, claves)
    df_sku_diario = df_sku_diario.merge(
        dominante[[*claves, 'sales_uom_dominante', 'pct_valor_unidad_dominante']],
        on=claves, how='inner',
    )
    df_sku_diario = df_sku_diario[
        df_sku_diario['sales_uom'] == df_sku_diario['sales_uom_dominante']
    ].copy()
    logging.info(f'Filtrado a unidad dominante: {len(df_sku_diario):,} filas')

    # REGION: pesos mensuales (mes anterior) + indice de precio ponderado
    pesos = construir_pesos_mensuales(df_sku_diario, claves)
    indice_precio = construir_indice_precio_grupo(df_sku_diario, pesos, claves)
    cantidad_cobertura = construir_cantidad_y_cobertura_grupo(df_sku_diario, claves)
    participacion = construir_participacion(df_sku_diario, claves)
    pct_unidad = dominante[[*claves, 'pct_valor_unidad_dominante']]

    df_grupo = indice_precio.merge(cantidad_cobertura, on=[*claves, 'p_date'], how='inner')
    df_grupo['p_month'] = df_grupo['p_date'].dt.year * 100 + df_grupo['p_date'].dt.month
    df_grupo = df_grupo.merge(participacion, on=[*claves, 'p_month'], how='left')
    df_grupo = df_grupo.merge(pct_unidad, on=claves, how='left')
    logging.info(f'Indice de grupo construido: {len(df_grupo):,} filas')

    # REGION: dias de venta mayor (a nivel categoria, igual que el script original)  # noqa: W505
    query_dias = QUERY_DIAS_VENTA_MAYOR.substitute(table_master=tmp_path_table_aux)
    df_dias_especiales = readBigQuery(query=query_dias, user=usuario, gbq_client=gbq_client)
    df_dias_especiales.columns = df_dias_especiales.columns.str.lower()
    df_dias_especiales['p_date'] = pd.to_datetime(df_dias_especiales['p_date'])
    df_grupo = df_grupo.merge(
        df_dias_especiales[
            ['category_description', 'p_date', 'proporcion_categoria', 'multiplicador']
        ],
        on=['category_description', 'p_date'], how='left',
    )
    df_grupo['multiplicador'] = df_grupo['multiplicador'].fillna('x1')
    df_grupo['multiplicador_x05'] = (df_grupo['multiplicador'] == 'x0.5').astype(int)
    df_grupo = df_grupo.drop(columns=['multiplicador'])
    df_grupo['ultimo_dia_mes'] = df_grupo['p_date'].dt.is_month_end.astype(int)
    df_grupo['primer_dia_mes'] = df_grupo['p_date'].dt.is_month_start.astype(int)
    logging.info('Dias especiales y dummies de calendario agregados...')

    # REGION: cerca_feriado, frac_promo, desc_promocional
    df_grupo['cerca_feriado'] = construir_cerca_feriado(df_grupo['p_date']).astype(int)

    query_promo = QUERY_PROMO_DIARIA.substitute(
        proyecto=proyecto, store_banner=store_banner,
        fecha_inicial=fecha_inicial, fecha_final=fecha_final,
    )
    df_promo = readBigQuery(query=query_promo, user=usuario, gbq_client=gbq_client)
    df_promo.columns = df_promo.columns.str.lower()
    df_promo['p_date'] = pd.to_datetime(df_promo['p_date'])
    cruce_promo = diagnosticar_cruce_material(df_sku_diario, df_promo)
    logging.info(f'Cruce con TMP_PROMOTION_DAILY (materiales): {cruce_promo}')
    if len(df_promo) and cruce_promo['materiales_comunes'] == 0:
        msg = (
            'Ningun material del panel aparece en TMP_PROMOTION_DAILY: el cruce esta '
            f'roto (formato del codigo de producto). Diagnostico: {cruce_promo}'
        )
        raise ValueError(msg)
    if df_promo.empty:
        logging.warning('TMP_PROMOTION_DAILY no trae filas para el banner/periodo: frac_promo = 0')
    frac_promo = construir_frac_promo(df_sku_diario, df_promo, claves)
    df_grupo = df_grupo.merge(frac_promo, on=[*claves, 'p_date'], how='left')
    df_grupo['frac_promo'] = df_grupo['frac_promo'].fillna(0.0)
    logging.info(
        f'frac_promo > 0 en {(df_grupo["frac_promo"] > 0).mean():.1%} de los dias-marca'
    )

    if store_banner == 'Unimarc':
        query_apo = QUERY_APOTEOSICO.substitute(
            store_banner_codigo=1000, cant_meses=cant_meses, fecha_inicial_ano='2023-02-01',
        )
        df_apo = readBigQuery(query=query_apo, user=usuario, gbq_client=gbq_client)
        df_apo.columns = df_apo.columns.str.lower()
        df_apo['fecha_inicio_de_promocion'] = pd.to_datetime(df_apo['fecha_inicio_de_promocion'])
        df_apo['fecha_fin_de_promocion'] = pd.to_datetime(df_apo['fecha_fin_de_promocion'])
        expandido = []
        for _, fila in df_apo.iterrows():
            rango = pd.date_range(
                fila['fecha_inicio_de_promocion'], fila['fecha_fin_de_promocion']
            )
            expandido.append(pd.DataFrame({'material': fila['material'], 'p_date': rango}))
        if expandido:
            df_apo_expandido = pd.concat(expandido, ignore_index=True).drop_duplicates()
        else:
            df_apo_expandido = pd.DataFrame({'material': [], 'p_date': []})
        cruce_apo = diagnosticar_cruce_material(df_sku_diario, df_apo_expandido)
        logging.info(f'Cruce con campañas apoteosicas (materiales): {cruce_apo}')
        if len(df_apo_expandido) and cruce_apo['materiales_comunes'] == 0:
            logging.warning('Ningun material del panel aparece en las campañas apoteosicas')
        desc_promocional = construir_desc_promocional(
            df_sku_diario, df_apo_expandido, claves
        )
    else:
        desc_promocional = df_grupo[[*claves, 'p_date']].copy()
        desc_promocional['desc_promocional'] = 0.0
    df_grupo = df_grupo.merge(desc_promocional, on=[*claves, 'p_date'], how='left')
    df_grupo['desc_promocional'] = df_grupo['desc_promocional'].fillna(0.0)
    logging.info('Flags de calendario y promocion agregados...')

    # REGION: historico de precio (3 meses), variacion vs pares, frescura
    df_grupo = construir_historico_precio(df_grupo, claves)
    df_grupo = construir_variacion_pares(
        df_grupo, CLAVES_SUBCATEGORIA, 'variacion_porcentual_subcategoria'
    )
    df_grupo = construir_variacion_pares(
        df_grupo, ['category_description'], 'variacion_porcentual_categoria'
    )
    df_grupo = aplicar_filtro_frescura(df_grupo, claves)
    logging.info(f'Historico, variacion de pares y frescura aplicados: {len(df_grupo):,} filas')

    # REGION: reordenar, agregar store_banner, subir
    df_grupo['store_banner'] = store_banner
    # Int64 (anulable de pandas): PRECIO_PROMEDIO puede ser NULL cuando la
    # cobertura del dia no alcanzo COBERTURA_MINIMA (columna 'cobertura').
    df_grupo['precio_promedio'] = np.exp(df_grupo['ln_p_grupo']).round(0).astype('Int64')
    df_grupo['precio_medio_anterior'] = (
        df_grupo['precio_medio_anterior'].round(0).astype('Int64')
    )
    df_grupo['ventas_totales_producto'] = df_grupo['ventas_totales_producto'].round(0)
    df_grupo['p_week'] = df_grupo['p_date'].dt.isocalendar().week.astype(int)
    columnas_finales = [
        'store_banner', *claves,
        'p_date', 'p_week', 'p_month',
        'precio_promedio', 'cantidad_total', 'ventas_totales_producto',
        'n_sku_activos', 'cobertura', 'pct_valor_unidad_dominante',
        'participacion_subcategoria', 'participacion_categoria',
        'primer_dia_mes', 'ultimo_dia_mes',
        'multiplicador_x05', 'proporcion_categoria', 'cerca_feriado',
        'frac_promo', 'desc_promocional', 'precio_medio_anterior',
        'variacion_porcentual', 'variacion_porcentual_subcategoria',
        'variacion_porcentual_categoria',
    ]
    df_grupo.columns = df_grupo.columns.str.lower()
    columnas_existentes = [c for c in columnas_finales if c in df_grupo.columns]
    df_final = df_grupo[columnas_existentes].copy()

    duplicadas = int(df_final.duplicated([*claves, 'p_date']).sum())
    if duplicadas:
        msg = f'El tablon tiene {duplicadas:,} filas repetidas por (categoria, subcategoria, marca, dia)'  # noqa: E501
        raise ValueError(msg)

    deleteFromTable(
        table_ref=CFG.tabla_destino, where_clause=f"store_banner = '{store_banner}'",
        gbq_client=gbq_client,
    )
    uploadFrame(
        df_final,
        table_ddl_json_path=os.path.join('gbq_objects', CFG.nombre_json),
        project=proyecto, gbq_client=gbq_client, if_exists='append',
    )
    logging.info(f'Tablon de {CFG.nombre_grupo} subido: {len(df_final):,} filas')


if __name__ == '__main__':
    main()
