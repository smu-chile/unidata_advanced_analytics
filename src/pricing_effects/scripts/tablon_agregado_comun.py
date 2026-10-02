"""Lógica compartida para construir tablones de regresión agregados.

Replica la metodología de `processed_regression_data.py`, subiendo un
nivel respecto al tablón a nivel SKU: en vez de (CATEGORIA, SKU), agrega
por (CATEGORIA, MARCA) o (CATEGORIA, SUBCATEGORIA). Usado por
`processed_regression_data_marca.py` y
`processed_regression_data_subcategoria.py`, que solo difieren en el
nombre de la columna de agrupación y la tabla destino.

Decisiones metodológicas:
- Unidad de medida dominante, fija por grupo, segun % de venta historica.
  El resto de las unidades se excluye del tablon;
  `pct_valor_unidad_dominante`
  reporta cuanto quedo cubierto.
- Precio: indice ponderado ln(P_grupo) = suma(w_i * ln(P_i)), NO un AVG
  simple de precios unitarios (sesgo de mix-shift). Pesos a nivel SKU,
  recalculados CADA MES usando solo el mes inmediatamente anterior.
- Sustitutos de ML y la regla de "inicio frio" asociada se eliminan por
  completo -- no tienen equivalente a nivel marca/subcategoria.
- `precio_medio_anterior`/`variacion_porcentual` (3 meses previos) y el
  filtro de frescura (1 año sin venta) se mantienen, re-keyados al grupo.
- Flags nuevos: `cerca_feriado` (calendario puro), `frac_promo` (% venta
  con cualquier descuento, TMP_PROMOTION_DAILY), `desc_promocional`
  (% venta en campañas apoteosicas, antes `apo` binario).
- Contexto nuevo: `n_sku_activos`, `participacion_categoria`.

"""
from __future__ import annotations

from string import Template
from dataclasses import dataclass

import numpy as np
import pandas as pd


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
    """Mismo query_master_table del script SKU, agregando la columna de
    grupo.

    Unica diferencia real respecto al original: se agrega
    `cfg.columna_origen AS cfg.columna_salida` a `distinct_products` y a
    la consulta principal -- el resto de la logica (EAN canonico,
    filtros de VW_SALES_ITEM) es identica.

    Caso especial: si el nivel de agregacion ES subcategoria,
    `SUB_CATEGORY_DESCRIPTION` ya existe como columna nativa (GRUPO_DSC) en
    la query original -- no se duplica el alias, se reutiliza la columna
    ya presente en vez de inyectar una linea adicional.
    """
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
    """Panel SKU-dia, igual formula que query_principal del script
    original.

    No agrega todavia al nivel de grupo -- esta es la pieza intermedia
    (precio y cantidad por SKU, cada dia) sobre la que despues se
    calculan pesos mensuales y el indice de precio del grupo. Mismas
    formulas exactas de cantidad_total/precio_promedio que el script SKU,
    solo que ahora se conserva tambien la columna de grupo.

    Caso especial: si el grupo ES subcategoria, no se duplica
    SUB_CATEGORY_DESCRIPTION (ver `construir_query_master_table`).
    """
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
    df_sku_diario: pd.DataFrame, columna_grupo: str
) -> pd.DataFrame:
    """Unidad de medida dominante por grupo, fija sobre todo el historico.

    Devuelve 1 fila por grupo: la sales_uom con mayor venta historica
    (en VALUE) y `pct_valor_unidad_dominante` -- la fraccion de venta del
    grupo que efectivamente queda cubierta al quedarse solo con esa
    unidad (el resto se excluye del tablon).
    """
    valor_por_uom = df_sku_diario.groupby([columna_grupo, 'sales_uom'])[
        'ventas_totales_sku'
    ].sum()
    valor_total_grupo = valor_por_uom.groupby(columna_grupo).transform('sum')
    pct = (valor_por_uom / valor_total_grupo).rename('pct_valor_unidad_dominante')
    tabla = pct.reset_index()
    idx_dominante = tabla.groupby(columna_grupo)['pct_valor_unidad_dominante'].idxmax()
    dominante = tabla.loc[idx_dominante].reset_index(drop=True)
    return dominante.rename(columns={'sales_uom': 'sales_uom_dominante'})


def construir_pesos_mensuales(
    df_sku_diario: pd.DataFrame, columna_grupo: str, min_dias: int = MIN_DIAS_PESO
) -> pd.DataFrame:
    """Peso de cada SKU dentro de su grupo, recalculado cada mes.

    El peso de un SKU en el mes M se calcula con su venta (VALUE) del mes
    M-1 respecto al total del grupo en M-1 -- nunca con datos del propio
    mes que se esta ponderando, para que el peso sea exogeno.

    Caso especial (inconsistencia 1 del docstring del modulo): el primer
    mes del historico no tiene mes anterior disponible. Para ESE mes
    unicamente, se usa su propio mes como ventana de pesos -- es la unica
    excepcion, y queda marcada en la columna `pesos_con_fallback`.
    """
    diario = df_sku_diario.copy()
    diario['p_month'] = diario['p_date'].dt.year * 100 + diario['p_date'].dt.month

    valor_mensual = diario.groupby([columna_grupo, 'product_id', 'p_month']).agg(
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
        fuente = valor_mensual[
            (valor_mensual[columna_grupo].notna())
            & (valor_mensual['p_month'] == mes_fuente)
        ].copy()
        if fuente.empty:
            continue
        valor_total_grupo_mes = fuente.groupby(columna_grupo)['valor_mes'].transform('sum')
        fuente['peso'] = fuente['valor_mes'] / valor_total_grupo_mes
        fuente['p_month'] = p_month_objetivo
        fuente['pesos_con_fallback'] = fallback
        columnas = [columna_grupo, 'product_id', 'p_month', 'peso', 'pesos_con_fallback']
        piezas.append(fuente[columnas])

    if not piezas:
        return pd.DataFrame(
            columns=[columna_grupo, 'product_id', 'p_month', 'peso', 'pesos_con_fallback']
        )
    return pd.concat(piezas, ignore_index=True)


def construir_indice_precio_grupo(
    df_sku_diario: pd.DataFrame,
    pesos: pd.DataFrame,
    columna_grupo: str,
    cobertura_minima: float = COBERTURA_MINIMA,
) -> pd.DataFrame:
    """Calcula el indice de precio diario: suma(w_i * ln P_i) / cobertura.

    Misma logica que `construir_indices_grupo` del notebook de
    diagnostico de subcategoria -- renormaliza por la fraccion de peso
    que efectivamente tiene precio observado ese dia; NaN si la
    cobertura no alcanza `cobertura_minima`.
    """
    diario = df_sku_diario.copy()
    diario['p_month'] = diario['p_date'].dt.year * 100 + diario['p_date'].dt.month
    con_peso = diario.merge(
        pesos[[columna_grupo, 'product_id', 'p_month', 'peso']],
        on=[columna_grupo, 'product_id', 'p_month'],
        how='inner',
    )
    con_peso['ln_p'] = np.log(con_peso['precio_promedio_sku'])
    con_peso['aporte'] = con_peso['peso'] * con_peso['ln_p']

    agregado = con_peso.groupby([columna_grupo, 'p_date']).agg(
        suma_ponderada=('aporte', 'sum'),
        cobertura=('peso', 'sum'),
    ).reset_index()
    agregado['ln_p_grupo'] = np.where(
        agregado['cobertura'] >= cobertura_minima,
        agregado['suma_ponderada'] / agregado['cobertura'],
        np.nan,
    )
    return agregado[[columna_grupo, 'p_date', 'ln_p_grupo', 'cobertura']]


def construir_cantidad_y_cobertura_grupo(
    df_sku_diario: pd.DataFrame, columna_grupo: str
) -> pd.DataFrame:
    """Cantidad total y n_sku_activos del grupo, por dia.

    cantidad_total es la SUMA directa (no ponderada) de la cantidad de
    cada SKU -- valido porque ya se filtro a la unidad de medida
    dominante antes de llegar aca (no se mezclan kg con unidades).
    """
    return df_sku_diario.groupby([columna_grupo, 'p_date']).agg(
        cantidad_total=('cantidad_total_sku', 'sum'),
        ventas_totales_producto=('ventas_totales_sku', 'sum'),
        n_sku_activos=('product_id', 'nunique'),
    ).reset_index()


def construir_participacion_categoria(
    df_sku_diario: pd.DataFrame, columna_grupo: str
) -> pd.DataFrame:
    """% de venta del grupo dentro de su categoria, calculado por mes."""
    diario = df_sku_diario.copy()
    diario['p_month'] = diario['p_date'].dt.year * 100 + diario['p_date'].dt.month
    por_grupo_mes = diario.groupby(
        ['category_description', columna_grupo, 'p_month']
    )['ventas_totales_sku'].sum().rename('venta_grupo_mes')
    por_categoria_mes = diario.groupby(
        ['category_description', 'p_month']
    )['ventas_totales_sku'].sum().rename('venta_categoria_mes')
    tabla = por_grupo_mes.reset_index().merge(
        por_categoria_mes.reset_index(), on=['category_description', 'p_month']
    )
    tabla['participacion_categoria'] = (
        tabla['venta_grupo_mes'] / tabla['venta_categoria_mes']
    ).round(4)
    return tabla[[columna_grupo, 'p_month', 'participacion_categoria']]


def construir_cerca_feriado(fechas: pd.Series) -> pd.Series:
    """Flag de calendario puro: dentro de +-VENTANA_FERIADO_DIAS de un
    feriado."""
    dias = (fechas - pd.Timestamp('1970-01-01')).dt.days.to_numpy()
    dias_feriado = np.sort((FERIADOS - pd.Timestamp('1970-01-01')).days.to_numpy())
    idx_derecha = np.searchsorted(dias_feriado, dias + VENTANA_FERIADO_DIAS, side='right')
    idx_izquierda = np.searchsorted(dias_feriado, dias - VENTANA_FERIADO_DIAS, side='left')
    return pd.Series(idx_derecha > idx_izquierda, index=fechas.index)


def construir_frac_promo(
    df_sku_diario: pd.DataFrame, df_promo: pd.DataFrame, columna_grupo: str
) -> pd.DataFrame:
    """% de venta del grupo-dia que corresponde a SKU con fila en
    TMP_PROMOTION_DAILY.

    Ausencia en TMP_PROMOTION_DAILY = precio regular (confirmado por el
    usuario) -- por eso un LEFT JOIN simple basta, sin necesitar tratar
    los NULL de forma especial mas que como "no promocionado".
    """
    diario = df_sku_diario.rename(columns={'product_id': 'material'})
    diario['material'] = diario['material'].astype(str)
    promo_unico = df_promo[['material', 'p_date']].drop_duplicates().assign(en_promo=1)
    promo_unico['material'] = promo_unico['material'].astype(str)
    diario = diario.merge(promo_unico, on=['material', 'p_date'], how='left')
    diario['en_promo'] = diario['en_promo'].fillna(0)
    diario['valor_en_promo'] = diario['en_promo'] * diario['ventas_totales_sku']
    agregado = diario.groupby([columna_grupo, 'p_date']).agg(
        valor_en_promo=('valor_en_promo', 'sum'),
        valor_total=('ventas_totales_sku', 'sum'),
    ).reset_index()
    agregado['frac_promo'] = (agregado['valor_en_promo'] / agregado['valor_total']).round(4)
    return agregado[[columna_grupo, 'p_date', 'frac_promo']]


def construir_desc_promocional(
    df_sku_diario: pd.DataFrame, df_apoteosico_material_fecha: pd.DataFrame, columna_grupo: str
) -> pd.DataFrame:
    """% de venta del grupo-dia en campañas apoteosicas.

    Reemplaza el `apo` binario del script original (0/1 por material) por
    una proporcion de venta -- mismo criterio que frac_promo.
    """
    diario = df_sku_diario.rename(columns={'product_id': 'material'})
    diario['material'] = diario['material'].astype(str)
    en_apo = df_apoteosico_material_fecha[['material', 'p_date']].drop_duplicates()
    en_apo = en_apo.assign(en_apo=1)
    en_apo['material'] = en_apo['material'].astype(str)
    diario = diario.merge(en_apo, on=['material', 'p_date'], how='left')
    diario['en_apo'] = diario['en_apo'].fillna(0)
    diario['valor_en_apo'] = diario['en_apo'] * diario['ventas_totales_sku']
    agregado = diario.groupby([columna_grupo, 'p_date']).agg(
        valor_en_apo=('valor_en_apo', 'sum'),
        valor_total=('ventas_totales_sku', 'sum'),
    ).reset_index()
    agregado['desc_promocional'] = (agregado['valor_en_apo'] / agregado['valor_total']).round(4)
    return agregado[[columna_grupo, 'p_date', 'desc_promocional']]


def construir_historico_precio(
    df_grupo_diario: pd.DataFrame, columna_grupo: str
) -> pd.DataFrame:
    """Precio medio de los 3 meses anteriores y variacion %, por grupo.

    Mismo patron que `generarMesesPrevios` + el bloque de historico del
    script original, re-keyado a `columna_grupo` en vez de `ean`. El
    precio usado es exp(ln_p_grupo) -- el nivel del indice de precio ya
    construido, no un precio "crudo" adicional.
    """
    grupo_diario = df_grupo_diario.copy()
    grupo_diario['p_month'] = (
        grupo_diario['p_date'].dt.year * 100 + grupo_diario['p_date'].dt.month
    )
    grupo_diario['precio_grupo'] = np.exp(grupo_diario['ln_p_grupo'])

    precio_mensual = grupo_diario.groupby([columna_grupo, 'p_month'])['precio_grupo'].mean()
    precio_mensual = precio_mensual.rename('precio_mensual').reset_index()

    piezas = []
    for desfase in (1, 2, 3):
        temp = precio_mensual[[columna_grupo, 'p_month']].copy()
        temp['p_month_ref'] = temp['p_month']
        temp['p_month'] = desplazar_mes(temp['p_month_ref'], desfase)
        piezas.append(temp)
    meses_previos = pd.concat(piezas, ignore_index=True)

    fusion = meses_previos.merge(precio_mensual, on=[columna_grupo, 'p_month'], how='left')
    historico = fusion.groupby([columna_grupo, 'p_month_ref'])['precio_mensual'].mean()
    historico = historico.rename('precio_medio_anterior').reset_index()
    historico = historico.rename(columns={'p_month_ref': 'p_month'})

    grupo_diario = grupo_diario.merge(historico, on=[columna_grupo, 'p_month'], how='left')
    # Escala porcentaje (x100), igual convencion que el script SKU
    # original (ahi: variacion_porcentual en puntos, ej. 5.23, no 0.0523).
    grupo_diario['variacion_porcentual'] = (
        (grupo_diario['precio_grupo'] - grupo_diario['precio_medio_anterior'])
        / grupo_diario['precio_medio_anterior']
        * 100
    ).fillna(0).round(2)
    return grupo_diario


def construir_variacion_peer_categoria(df_grupo_diario: pd.DataFrame) -> pd.DataFrame:
    """Variacion porcentual promedio de los PARES de categoria, excluyendo
    al propio grupo.

    Replica `variacion_porcentual_subcategoria` del script SKU original
    (ahi: cada SKU se compara contra el promedio ponderado por venta de
    los demas SKU de su misma subcategoria-dia, dejandose afuera a si
    mismo -- "leave-one-out"). Aca el grupo de pares natural ya no es
    subcategoria (eso es justamente el nivel de agregacion de uno de los
    2 tablones) -- es CATEGORIA: cada marca/subcategoria se compara
    contra el promedio ponderado de las demas marcas/subcategorias de su
    misma categoria-dia.

    Requiere que `df_grupo_diario` ya tenga `variacion_porcentual`
    (propia, ver `construir_historico_precio`) y `ventas_totales_producto`.
    """
    grupo_diario = df_grupo_diario.copy()
    peso = grupo_diario['ventas_totales_producto'].fillna(0)
    variacion = grupo_diario['variacion_porcentual'].fillna(0)
    grupo_diario['_aporte'] = variacion * peso

    claves = ['category_description', 'p_date']
    numerador_total = grupo_diario.groupby(claves)['_aporte'].transform('sum')
    denominador_total = grupo_diario.groupby(claves)['ventas_totales_producto'].transform('sum')

    numerador_excl = numerador_total - grupo_diario['_aporte']
    denominador_excl = denominador_total - peso

    with np.errstate(invalid='ignore', divide='ignore'):
        grupo_diario['variacion_porcentual_categoria'] = numerador_excl / denominador_excl
    grupo_diario.loc[denominador_excl == 0, 'variacion_porcentual_categoria'] = 0.0
    grupo_diario['variacion_porcentual_categoria'] = grupo_diario[
        'variacion_porcentual_categoria'
    ].round(2)
    return grupo_diario.drop(columns='_aporte')


def aplicar_filtro_frescura(
    df_grupo_diario: pd.DataFrame, columna_grupo: str, meses: int = 12
) -> pd.DataFrame:
    """Descarta grupos sin venta en el ultimo año (misma regla del script
    original)."""
    fecha_maxima = df_grupo_diario['p_date'].max()
    fecha_maxima_por_grupo = df_grupo_diario.groupby(columna_grupo)['p_date'].max()
    grupos_validos = fecha_maxima_por_grupo[
        fecha_maxima_por_grupo >= (fecha_maxima - pd.DateOffset(months=meses))
    ].index
    return df_grupo_diario[df_grupo_diario[columna_grupo].isin(grupos_validos)].copy()
