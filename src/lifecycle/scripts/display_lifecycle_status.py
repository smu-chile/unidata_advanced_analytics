# Default
from __future__ import annotations

import io
import logging
import argparse
from logging import config
from datetime import datetime

# Pip
# Pip
import numpy as np
import pandas as pd
import pendulum
import matplotlib.pyplot as plt
from google.cloud import bigquery  # noqa: F401
from matplotlib.figure import Figure  # noqa: TC002
from matplotlib.ticker import FuncFormatter
from matplotlib.patches import Patch, FancyBboxPatch
from reportlab.platypus import (
    Image,
    Spacer,
    Flowable,
    PageBreak,
    Paragraph,
    SimpleDocTemplate,
)
from reportlab.lib.enums import TA_LEFT, TA_CENTER, TA_JUSTIFY
from reportlab.lib.units import cm
from reportlab.lib.styles import ParagraphStyle, getSampleStyleSheet
from google.cloud.bigquery import Client
from dateutil.relativedelta import relativedelta
from reportlab.lib.pagesizes import A4

# Own
import common.gcp_extended.bigquery as gbq_extended  # noqa: F401
import common.office365_extended.sharepoint as sp_extended
from common.constants import LOGGING_CONFIG
from common.databases.queries import QueryDict
from common.gcp_extended.bigquery import (  # noqa: E402
    readBigQuery,
)
from common.gcp_extended.secretsmanager import getSecret  # noqa: E402


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
    # ------------------------------------------------------------------
    # Query principal: extrae información de gasto y canastas compradas
    # por cliente en un periodo definido, filtrando por canal de venta,
    # tipo de documento y banner de tienda.
    # Parámetros:
    #   - ${last_month}: Último mes del que se tienen datos.
    #   - ${formato}: banner de tienda (ej. 'Unimarc').
    # Retorna:
    #   - yyyymm: mes de la transacción (YYYYMM).
    #   - customer_key: identificador del cliente.
    #   - canastas_compradas: número de canastas únicas.
    #   - gasto_total: gasto total asociado.
    # ------------------------------------------------------------------
    'query_lc':
    """

DECLARE periodo STRING DEFAULT '${last_monthid}';  -- formato YYYYMM
DECLARE fecha_final DATE DEFAULT LAST_DAY(PARSE_DATE('%Y%m', periodo), MONTH);

WITH monthid_considerar AS (
  SELECT DISTINCT CAST(monthid AS STRING) AS monthid
  FROM `${path_tabla}`
  WHERE store_banner = '${formato}'
  ORDER BY monthid DESC
  LIMIT 13
),

-- "vista" auxiliar con monthid desplazado +1
unidata_mes_desfasado AS (
  SELECT
    FORMAT_DATE(
      '%Y%m',
      DATE_ADD(PARSE_DATE('%Y%m', CAST(monthid AS STRING)), INTERVAL 1 MONTH)
    ) AS monthid,
    customer_key,
    nivel_informado,
    org_ip_id
  FROM `${tabla_shabits}`
  WHERE org_ip_id = '${formato_id}'
),

ventas_mensuales AS (
  SELECT
    FORMAT_DATE('%Y%m', transaction_date) AS monthid,
    customer_key,
    COUNT(DISTINCT market_basket_key) AS canastas_compradas,
    SUM(basket_value)                 AS gasto_total
  FROM `${proyecto}.ML_LAB.VW_SALES_BASKET` A
  INNER JOIN `${proyecto}.ML_LAB.VW_DIM_STORE` D
    ON A.store_id = D.store_id
  WHERE transaction_date BETWEEN DATE_TRUNC(DATE_SUB(fecha_final, INTERVAL 12 MONTH), MONTH)
                             AND fecha_final
    AND fnc_doc_tp_dsc IN ('BX', 'BE', 'TF')
    AND itm_txn_fcn_tp_dsc = 'V'
    AND market_basket_key NOT IN (
      SELECT market_basket_key
      FROM `cl-cda-prod.DS_CDA_VW_SMU.DW_VW_FACT_MARKET_BASKET_E_COMMERCE`
      WHERE canal_venta IN ('PEDIDOS YA','CORNER SHOP','RAPPI','RAPPI TURBO')
    )
    AND D.store_banner = '${formato}'
  GROUP BY monthid, customer_key
)

SELECT
  A.*,
  IFNULL(C.canastas_compradas, 0) AS canastas_compradas,
  IFNULL(C.gasto_total, 0)        AS gasto_total,
  IFNULL(U.nivel_informado, 'Sin nivel') AS nivel
FROM `${path_tabla}` A
JOIN monthid_considerar B
  ON CAST(A.monthid AS STRING) = B.monthid
LEFT JOIN ventas_mensuales C
  ON CAST(A.monthid AS STRING) = C.monthid
 AND A.customer_key = C.customer_key
LEFT JOIN unidata_mes_desfasado U
  ON U.customer_key = A.customer_key
 AND U.monthid     = CAST(A.monthid AS STRING)
LEFT JOIN
`cl-cda-unidata-prod.DS_PROD_CLIENTES_IC.VW_FACT_MONTH_CUSTOMER_ORGANIZATION_OUTLIER_MART` O
  ON O.month_id = CAST(A.monthid AS INT64)   -- normaliza solo el mes
  AND O.customer_key = A.customer_key         -- mismo tipo/formato
  AND O.org_ip_id = '${formato_id}'
WHERE A.store_banner = '${formato}'
  AND O.customer_key IS NULL;                -- anti-join: excluye outliers
    """
})


# -------------------------------------------------------------------------
# Functions and Classes
# -------------------------------------------------------------------------

#--------------------------------------------------------------------------
# FUNCTIONS
#--------------------------------------------------------------------------

def print2(*mensajes:str) -> None:
    """Imprime mensajes proporcionados con un sello de tiempo.

    Esta función recibe un número indeterminado de mensajes,
    los concatena con un espacio como separador, y los imprime precedidos
    por la hora actual en formato HH:mm:ss.

    Args:
    *mensajes (str): Argumentos variables de tipo string,

    Ejemplo:
    >>> print2("hola", "chao")
    [16:32:20] - hola chao
    """
    # Se obtiene la hora actual (con precisión de segundos)
    hora_actual = pendulum.now()
    hora_formateada = hora_actual.format('HH:mm:ss')
    # Concatena todos los mensajes con un espacio entre ellos
    mensaje_completo = ' '.join(mensajes)
    # Imprime en formato deseado
    print(f'[{hora_formateada}] - {mensaje_completo}')  # noqa: T201


def calcular_mes_anterior(año_mes_final: str) -> str:
    """Calcula el año-mes inicial restando una cantidad de meses.

    Args:
        año_mes_final (str): Fecha final en formato 'YYYYMM'.
        meses (int): Cantidad de meses que cubre el periodo
            (por defecto 12).

    Returns
    -------
        str: Fecha inicial en formato 'YYYYMM' después de restar meses - 1.

    Ejemplo:
        año_mes_final = '202407', meses = 12 → año_mes_inicial = '202308'
    """
    fecha_final = datetime.strptime(año_mes_final, '%Y%m') # noqa: DTZ007
    fecha_inicial = fecha_final - relativedelta(months=1)
    return fecha_inicial.strftime('%Y%m')  # YYYYMM


#--------------------------------------------------------------------------
# GRAFICOS
#--------------------------------------------------------------------------

def grafico_torta_estado_mes(
    df_lc: pd.DataFrame,
    monthid: int | str,
    considerar_gasto: bool = False,
    mostrar_monto_ventas: bool = False,  # False => solo % en ventas
    umbral_pct_inside: float = 3.0,       # >=5% dentro, <5% fuera (ventas)
    titulo: str | None = None,
    show: bool = True
) -> Figure:
    """Genera torta(s) por estado para un monthid y retorna la Figure.

    - considerar_gasto=False: 1 torta (conteo con valor+% dentro).
    - considerar_gasto=True : 2 tortas. Izq: conteo con valor+% dentro.
      Der: ventas con mix. % >= umbral dentro; % < umbral fuera con guía.
    - La leyenda (solo nombres) queda a la derecha y centrada.

    Requiere:
      - 'customer_key', 'monthid', 'status'
      - 'gasto_total' si considerar_gasto=True
    """

    # --- Configuración de categorías y colores ---
    orden_status = [
        'nuevo', 'esporadico', 'crecimiento', 'estable',
        'decrecimiento', 'fugado', 'recuperado'
    ]
    colores = {
        'nuevo': '#8BC34A',
        'esporadico': '#9E9E9E',
        'crecimiento': '#2E7D32',
        'estable': '#1976D2',
        'decrecimiento': '#FB8C00',
        'fugado': '#E53935',
        'recuperado': '#4FC3F7'
    }

    # --- Validaciones ---
    req = {'customer_key', 'monthid', 'status'}
    faltan = req - set(df_lc.columns)
    if faltan:
        msg = f'Faltan columnas requeridas: {sorted(faltan)}'
        raise ValueError(msg)
    if considerar_gasto and 'gasto_total' not in df_lc.columns:
        msg = "Para considerar_gasto=True se requiere 'gasto_total'."
        raise ValueError(msg)

    # --- Filtrar monthid ---
    monthid_str = str(monthid)
    cols = ['customer_key', 'monthid', 'status'] + (
        ['gasto_total'] if considerar_gasto else []
    )
    df_aux = df_lc.loc[
        df_lc['monthid'].astype(str) == monthid_str, cols
    ].copy()
    if df_aux.empty:
        msg = f'No hay datos para monthid={monthid_str}.'
        raise ValueError(msg)

    # --- Normalizar status ---
    df_aux['status'] = (
        df_aux['status'].astype(str).str.lower().str.strip().replace({
            'nuevo1': 'nuevo', 'nuevo 1': 'nuevo',
            'nuevo2': 'nuevo', 'nuevo 2': 'nuevo'
        })
    )
    df_aux = df_aux[df_aux['status'] != 'sin estado']
    df_aux = df_aux[df_aux['status'].isin(orden_status)]
    if df_aux.empty:
        msg = 'No hay datos válidos tras normalizar/excluir estados.'
        raise ValueError(msg)

    # --- Métricas ---
    s_conteo = (
        df_aux.groupby('status')['customer_key']
        .nunique()
        .reindex(orden_status)
        .fillna(0)
        .astype(int)
    )
    total_clientes = df_aux['customer_key'].nunique()

    s_ventas = None
    if considerar_gasto:
        df_aux['gasto_total'] = pd.to_numeric(
            df_aux['gasto_total'], errors='coerce'
        ).fillna(0.0)
        s_ventas = (
            df_aux.groupby('status')['gasto_total']
            .sum()
            .reindex(orden_status)
            .fillna(0.0)
            .astype(float)
        )
        if s_ventas.sum() == 0:
            considerar_gasto = False  # evita torta vacía

    # --- Helpers ---
    def _filtrar_pos(series: pd.Series):
        # Devuelve valores, colores y claves con valores > 0
        mask = series > 0
        vals = series[mask].values.tolist()  # noqa: PD011
        cols_local = [colores[s] for s in series.index[mask]]
        keys = list(series.index[mask])
        return vals, cols_local, keys

    def _fmt_millones(v: float) -> str:
        return f'${round(v / 1_000_000):,}M'

    def _autopct_conteo(valores):
        total = sum(valores)

        def inner(pct):
            v = round(pct * total / 100.0)
            return f'{v:,}\n({pct:.1f}%)'

        return inner

    # Proxy legend (solo nombres)
    presentes = {s for s, v in s_conteo.items() if v > 0}
    if s_ventas is not None:
        presentes |= {s for s, v in s_ventas.items() if v > 0}
    presentes = [s for s in orden_status if s in presentes]
    handles_legend = [
        Patch(facecolor=colores[s], edgecolor='white', label=s.capitalize())
        for s in presentes
    ]
    labels_legend = [s.capitalize() for s in presentes]

    # --- Datos para tortas ---
    vals_c, cols_c, keys_c = _filtrar_pos(s_conteo)
    if considerar_gasto:
        vals_v, cols_v, keys_v = _filtrar_pos(s_ventas)

    # --- Construcción de figura(s) ---
    if considerar_gasto:
        fig, (ax_c, ax_v) = plt.subplots(1, 2, figsize=(14, 7))
        fig.suptitle(
            titulo or f'Participación por Estado - {monthid_str}',
            fontsize=16,
            fontweight='bold'
        )

        # Conteo (valor + % dentro)
        wedges_c, texts_c, autotexts_c = ax_c.pie(
            vals_c,
            autopct=_autopct_conteo(vals_c),
            startangle=90,
            counterclock=False,
            colors=cols_c,
            wedgeprops={'edgecolor': 'white'}
        )
        ax_c.set_title('Conteo de clientes', fontsize=13, fontweight='bold')
        for at in autotexts_c:
            at.set_fontsize(9)
            at.set_weight('bold')

        # Ventas: % >= umbral dentro; % < umbral fuera con guía
        wedges_v, texts_v = ax_v.pie(
            vals_v,
            startangle=90,
            counterclock=False,
            colors=cols_v,
            wedgeprops={'edgecolor': 'white'}
        )
        ax_v.set_title('Ventas', fontsize=13, fontweight='bold')

        total_v = sum(vals_v)
        if total_v <= 0:
            msg = 'Las ventas totales son cero; no es posible etiquetar.'
            raise ValueError(msg)

        for wedge, val in zip(wedges_v, vals_v):
            if val <= 0:
                continue
            pct = val / total_v * 100.0
            ang = (wedge.theta2 + wedge.theta1) / 2.0
            ang_rad = np.deg2rad(ang)
            x, y = np.cos(ang_rad), np.sin(ang_rad)

            if pct >= umbral_pct_inside:
                # Etiqueta DENTRO
                r_in = 0.6
                tx, ty = r_in * x, r_in * y
                if mostrar_monto_ventas:
                    label = f'{_fmt_millones(val)}\n({pct:.1f}%)'
                else:
                    label = f'{pct:.1f}%'
                ax_v.text(
                    tx, ty, label, ha='center', va='center',
                    fontsize=9, fontweight='bold'
                )
            else:
                # Etiqueta FUERA con línea guía
                r_text = 1.22
                tx, ty = r_text * x, r_text * y
                ha = 'left' if x >= 0 else 'right'
                if mostrar_monto_ventas:
                    label = f'{_fmt_millones(val)} ({pct:.1f}%)'
                else:
                    label = f'{pct:.1f}%'
                ax_v.annotate(
                    label,
                    xy=(0.98 * x, 0.98 * y),
                    xytext=(tx, ty),
                    textcoords='data',
                    ha=ha,
                    va='center',
                    fontsize=9,
                    fontweight='bold',
                    arrowprops={
                        'arrowstyle': '-',
                        'color': '#666',
                        'lw': 1.0,
                        'shrinkA': 0,
                        'shrinkB': 0,
                        'connectionstyle': 'angle3,angleA=0,angleB=90'
                    }
                )

        # Leyenda única (solo nombres)
        if handles_legend:
            fig.legend(
                handles=handles_legend,
                labels=labels_legend,
                title='Estados',
                loc='center right',
                bbox_to_anchor=(1.03, 0.5),
                fontsize=10,
                title_fontsize=11
            )

        plt.tight_layout(rect=[0, 0, 0.88, 1])
        if show:
            plt.show()
        return fig

    # Solo conteo
    fig, ax = plt.subplots(figsize=(6, 6))
    fig.suptitle(
        titulo or f'Participación por Estado - {monthid_str}',
        fontsize=16,
        fontweight='bold'
    )
    wedges, texts, autotexts = ax.pie(
        vals_c,
        autopct=_autopct_conteo(vals_c),
        startangle=90,
        counterclock=False,
        colors=cols_c,
        wedgeprops={'edgecolor': 'white'}
    )
    ax.set_title(
        f'Conteo de clientes {total_clientes:,}',
        fontsize=13,
        fontweight='bold'
    )

    if handles_legend:
        fig.legend(
            handles=handles_legend,
            labels=labels_legend,
            title='Estados',
            loc='center right',
            bbox_to_anchor=(1.08, 0.5),
            fontsize=10,
            title_fontsize=11
        )

    for at in autotexts:
        at.set_fontsize(10)
        at.set_weight('bold')

    plt.tight_layout(rect=[0, 0, 0.86, 1])
    if show:
        plt.show()
    return fig


def grafico_clientes_por_estado(
    df_lc: pd.DataFrame,
    n_meses: int = 6,
    tipo: int = 1,
    ultimo_mes: bool = True
) -> Figure:
    """Barras apiladas por monthid y status (conteo o gasto)."""
    # --- Configuración de categorías y colores ---
    orden_status = [
        'nuevo', 'esporadico', 'crecimiento', 'estable',
        'decrecimiento', 'fugado', 'recuperado'
    ]
    colores = {
        'nuevo': '#8BC34A', 'esporadico': '#9E9E9E', 'crecimiento': '#2E7D32',
        'estable': '#1976D2', 'decrecimiento': '#FB8C00', 'fugado': '#E53935',
        'recuperado': '#4FC3F7'
    }

    # --- Helpers ---
    def fmt_conteo_y(x, _):
        if x >= 1_000_000:
            return f'{x/1_000_000:.1f}M'
        if x >= 1_000:
            return f'{x/1_000:.0f}K'
        return f'{int(x)}'

    def fmt_money_m_int(val: float) -> str:
        m = round(val / 1_000_000)
        return f'${m:,}M'

    def fmt_money_y(x, _):
        m = round(x / 1_000_000)
        return f'${m:,}M'

    # 1) FILTRAR MESES PRIMERO
    if 'gasto_total' in df_lc.columns:
        df_aux = df_lc[['customer_key', 'monthid', 'status',
                        'gasto_total']].copy()
    else:
        df_aux = df_lc[['customer_key', 'monthid', 'status']].copy()
    df_aux['monthid'] = df_aux['monthid'].astype(str)
    if df_aux['monthid'].empty:
        msg = 'El DataFrame no contiene monthid.'
        raise ValueError(msg)

    if not ultimo_mes:
        max_mes = df_aux['monthid'].max()
        df_aux = df_aux[df_aux['monthid'] != max_mes]
        if df_aux.empty:
            msg = 'No quedan datos después de excluir el último monthid.'
            raise ValueError(msg)

    meses_ordenados = sorted(df_aux['monthid'].unique())
    if not meses_ordenados:
        msg = 'No hay monthid disponibles para graficar.'
        raise ValueError(msg)
    meses_seleccion = meses_ordenados[-n_meses:] if n_meses > 0 else meses_ordenados
    df_aux = df_aux[df_aux['monthid'].isin(meses_seleccion)]

    # 2) TRANSFORMACIONES
    df_aux['status'] = df_aux['status'].astype(str).replace(
        {'nuevo1': 'nuevo', 'nuevo2': 'nuevo'}
    )
    df_aux = df_aux[df_aux['status'] != 'sin estado']

    if tipo == 2:
        if 'gasto_total' not in df_aux.columns:
            msg = (
                'Para tipo=2 (gasto) se requiere la columna '
                "'gasto_total' en df_lc."
            )
            raise ValueError(msg)
        df_aux['gasto_total'] = pd.to_numeric(
            df_aux['gasto_total'], errors='coerce'
        ).fillna(0)

    # Agrupar
    if tipo == 2:
        metrica = 'gasto_total'
        df_status_mes = (
            df_aux.groupby(['monthid', 'status'])[metrica]
            .sum()
            .reset_index(name='valor')
        )
        ylabel = 'Ventas (millones $)'
        titulo = 'Ventas por estado y mes'
        yformatter = FuncFormatter(fmt_money_y)
    else:
        metrica = 'customer_key'
        df_status_mes = (
            df_aux.groupby(['monthid', 'status'])[metrica]
            .nunique()
            .reset_index(name='valor')
        )
        ylabel = 'Número de clientes únicos'
        titulo = 'Clientes únicos por estado y mes (conteo)'
        yformatter = FuncFormatter(fmt_conteo_y)

    # Pivot
    df_pivot = (
        df_status_mes.pivot(index='monthid', columns='status',  # noqa: PD010
                            values='valor').fillna(0)  # noqa: PD010
    )
    for st in orden_status:
        if st not in df_pivot.columns:
            df_pivot[st] = 0
    df_pivot = df_pivot[orden_status].sort_index()

    # Plot
    colores_en_orden = [colores[s] for s in df_pivot.columns]
    ancho_fig = max(12, 1.8 * len(df_pivot))
    ax = df_pivot.plot(
        kind='bar', stacked=True, color=colores_en_orden,
        width=0.9, figsize=(ancho_fig, 8)
    )
    fig = ax.figure  # obtener la Figure creada por pandas

    ax.set_title(titulo, fontsize=15, fontweight='bold')
    ax.set_xlabel('Mes (monthid)', fontsize=12)
    ax.set_ylabel(ylabel, fontsize=12)
    ax.tick_params(axis='x', rotation=45)
    ax.yaxis.set_major_formatter(yformatter)

    # Leyenda
    handles, labels = ax.get_legend_handles_labels()
    ax.legend(
        handles, [lbl.capitalize() for lbl in labels],
        title='Status', bbox_to_anchor=(1.02, 0.5),
        loc='center left', borderaxespad=0.5
    )

    # Etiquetas internas
    totales = df_pivot.sum(axis=1).values  # noqa: PD011
    totales_safe = [t if t != 0 else 1 for t in totales]
    for col_idx, container in enumerate(ax.containers):
        valores_col = df_pivot.iloc[:, col_idx].values  # noqa: PD011
        etiquetas = []
        for i, bar in enumerate(container):
            alto = bar.get_height()
            if alto <= 0:
                etiquetas.append('')
            else:
                pct = valores_col[i] / totales_safe[i] * 100
                if tipo == 2:
                    etiquetas.append(f'{fmt_money_m_int(alto)} ({pct:.1f}%)')
                else:
                    etiquetas.append(f'{alto:,.0f} ({pct:.1f}%)')
        ax.bar_label(container, labels=etiquetas,
                     label_type='center', fontsize=9)

    # Totales arriba
    first_container = ax.containers[0]
    for i, bar in enumerate(first_container):
        x = bar.get_x() + bar.get_width() / 2
        y = totales[i]
        total_txt = (
            f'{fmt_money_m_int(y)} (100%)' if tipo == 2 else f'{y:,.0f} (100%)'
        )
        ax.annotate(
            total_txt, xy=(x, y), xytext=(0, 10),
            textcoords='offset points', ha='center', va='bottom',
            fontsize=10, fontweight='bold'
        )

    plt.tight_layout()
    plt.show()
    return fig


def grafico_lineas_clientes_por_estado(
    df_lc: pd.DataFrame,
    n_meses: int = 6
) -> Figure:
    """Serie temporal (líneas) de clientes únicos por estado y mes.
    Primero filtra los meses y luego aplica transformaciones.
    """
    # --- Configuración de categorías y colores ---
    orden_status = [
        'nuevo', 'esporadico', 'crecimiento', 'estable',
        'decrecimiento', 'fugado', 'recuperado'
    ]
    colores = {
        'nuevo': '#8BC34A',
        'esporadico': '#9E9E9E',
        'crecimiento': '#2E7D32',
        'estable': '#1976D2',
        'decrecimiento': '#FB8C00',
        'fugado': '#E53935',
        'recuperado': '#4FC3F7'
    }

    # =========================
    # 1) FILTRAR MESES PRIMERO
    # =========================
    df_aux = df_lc[['customer_key', 'monthid', 'status']].copy()
    df_aux['monthid'] = df_aux['monthid'].astype(str)

    if df_aux['monthid'].empty:
        msg = 'El DataFrame no contiene monthid.'
        raise ValueError(msg)

    meses_ordenados = sorted(df_aux['monthid'].unique())
    if not meses_ordenados:
        msg = 'No hay monthid disponibles para graficar.'
        raise ValueError(msg)

    meses_sel = meses_ordenados[-n_meses:] if n_meses > 0 else meses_ordenados
    df_aux = df_aux[df_aux['monthid'].isin(meses_sel)]

    # =========================
    # 2) TRANSFORMACIONES SOBRE MESES SELECCIONADOS
    # =========================
    df_aux['status'] = df_aux['status'].astype(str).replace(
        {'nuevo1': 'nuevo', 'nuevo2': 'nuevo'}
    )
    df_aux = df_aux[df_aux['status'] != 'sin estado']

    # --- Agrupar clientes únicos por mes y estado ---
    df_status_mes = (
        df_aux.groupby(['monthid', 'status'])['customer_key']
        .nunique()
        .reset_index(name='n_clientes')
    )

    # --- Pivot a formato ancho ---
    df_pivot = (
        df_status_mes.pivot(  # noqa: PD010
            index='monthid', columns='status', values='n_clientes'
        )
        .fillna(0)
        .sort_index()
    )
    for st in orden_status:
        if st not in df_pivot.columns:
            df_pivot[st] = 0
    df_pivot = df_pivot[orden_status]

    # --- Ajuste dinámico de tamaño ---
    escala = 1 + (len(df_pivot) - 6) * 0.08
    fig_w = max(10, 10 * escala)
    fig_h = max(6, 6 * escala)
    fuente_base = max(10, 12 * escala * 0.8)

    fig, ax = plt.subplots(figsize=(fig_w, fig_h))

    # --- Plot de líneas por estado ---
    for st in orden_status:
        ax.plot(
            df_pivot.index,
            df_pivot[st].values,
            marker='o',
            linewidth=2,
            label=st.capitalize(),
            color=colores[st]
        )

    # --- Estética de ejes y formato ---
    ax.set_title(
        'Clientes únicos por estado y mes (serie temporal)',
        fontsize=fuente_base * 1.2,
        fontweight='bold'
    )
    ax.set_xlabel('Mes (monthid)', fontsize=fuente_base)
    ax.set_ylabel('Número de clientes únicos', fontsize=fuente_base)
    ax.tick_params(axis='x', rotation=45, labelsize=fuente_base * 0.9)
    ax.tick_params(axis='y', labelsize=fuente_base * 0.9)

    def millones(x, _):
        if x >= 1_000_000:
            return f'{x/1_000_000:.1f}M'
        if x >= 1_000:
            return f'{x/1_000:.0f}K'
        return f'{int(x)}'

    ax.yaxis.set_major_formatter(FuncFormatter(millones))

    # Leyenda a la derecha, centrada verticalmente
    ax.legend(
        title='Status',
        bbox_to_anchor=(1.02, 0.5),
        loc='center left',
        borderaxespad=0.5,
        fontsize=fuente_base * 0.9,
        title_fontsize=fuente_base
    )

    plt.tight_layout()
    plt.grid(True, alpha=0.3)  # noqa: FBT003
    plt.show()
    return fig



def matriz_status_vs_nivel_graficos(
    df_lc: pd.DataFrame,
    monthid: int | str,
    titulo: str | None = None,
    mostrar: bool = True,
    modo_porcentaje: str = 'fila',  # 'fila' o 'columna'
    colores_status: dict[str, str] | None = None,
    considerar_gasto: bool = True
) -> Figure:
    """Dibuja heatmap(s) por monthid.
       - considerar_gasto=True: 2 heatmaps (conteo izq, ventas der).
       - considerar_gasto=False: 1 heatmap (conteo).
       Retorna la Figure (para guardar en PDF, etc.).
    """
    # --------- Validaciones ---------
    req_base = {'monthid', 'status', 'nivel'}
    faltan = req_base - set(df_lc.columns)
    if faltan:
        msg = f'falta(n) columna(s): {sorted(faltan)}'
        raise ValueError(msg)
    if considerar_gasto and 'gasto_total' not in df_lc.columns:
        msg = "Para considerar_gasto=True se requiere 'gasto_total'."
        raise ValueError(msg)
    if modo_porcentaje not in {'fila', 'columna'}:
        msg = "`modo_porcentaje` debe ser 'fila' o 'columna'."
        raise ValueError(msg)

    # --------- Filtros y normalizaciones ---------
    monthid_str = str(monthid)
    df_mes = df_lc.loc[df_lc['monthid'].astype(str) == monthid_str].copy()
    if df_mes.empty:
        msg = 'Sin datos para el monthid indicado.'
        raise ValueError(msg)

    # Excluir 'sin estado'
    df_mes = df_mes[df_mes['status'].astype(str).str.lower() != 'sin estado']

    def _norm_status(s: str) -> str:
        if pd.isna(s):
            return 'desconocido'
        t = str(s).strip().lower()
        t = (t.replace('á', 'a').replace('é', 'e')
               .replace('í', 'i').replace('ó', 'o').replace('ú', 'u'))
        if t in {'nuevo1', 'nuevo 1', 'nuevo2', 'nuevo 2'}:
            return 'nuevo'
        if t == 'crecimiento':
            return 'creciente'
        if t == 'decrecimiento':
            return 'decreciente'
        return t

    def _norm_nivel(x: str) -> str:
        if pd.isna(x) or str(x).strip().lower() in {'sn', 'sin nivel', ''}:
            return 'Sin Nivel'
        t = str(x).strip().lower()
        t = (t.replace('á', 'a').replace('é', 'e')
               .replace('í', 'i').replace('ó', 'o').replace('ú', 'u'))
        if t == 'socio club':
            return 'Socio Club'
        if t == 'socio oro':
            return 'Socio Oro'
        if t == 'socio platino':
            return 'Socio Platino'
        if t in {'sn', 'sin nivel'}:
            return 'Sin Nivel'
        return str(x).strip().title()

    df_mes['status'] = df_mes['status'].map(_norm_status)
    df_mes['nivel'] = df_mes['nivel'].apply(_norm_nivel)

    if considerar_gasto:
        df_mes['gasto_total'] = pd.to_numeric(
            df_mes.get('gasto_total', 0), errors='coerce'
        ).fillna(0)

    orden_status = [
        'nuevo', 'esporadico', 'creciente',
        'estable', 'decreciente', 'fugado', 'recuperado'
    ]
    df_mes = df_mes[df_mes['status'].isin(orden_status)].copy()
    if df_mes.empty:
        msg = 'Sin datos válidos tras normalización/filtrado.'
        raise ValueError(msg)
    df_mes['status'] = pd.Categorical(
        df_mes['status'], categories=orden_status, ordered=True
    )

    # --------- Matriz de CONTEOS ---------
    conteos = pd.crosstab(index=df_mes['status'], columns=df_mes['nivel'])
    conteos = conteos.reindex(index=orden_status, fill_value=0)
    conteos = conteos.sort_index(axis=1).astype('int64')
    if conteos.empty:
        msg = 'Sin datos de conteos para el monthid indicado.'
        raise ValueError(msg)

    # --------- (Opcional) Matriz de VENTAS ---------
    if considerar_gasto:
        ventas = pd.pivot_table(
            df_mes,
            index='status',
            columns='nivel',
            values='gasto_total',
            aggfunc='sum',
            fill_value=0,
            observed=False  # silencia FutureWarning
        )
        ventas = ventas.reindex(index=orden_status, fill_value=0)
        ventas = ventas.reindex(columns=conteos.columns).astype('float64')
        if ventas.empty:
            msg = 'Sin datos de ventas para el monthid indicado.'
            raise ValueError(msg)

    # --------- Totales ---------
    tot_conteo_rows = conteos.sum(axis=1)
    tot_conteo_cols = conteos.sum(axis=0)
    if considerar_gasto:
        tot_venta_rows = ventas.sum(axis=1)
        tot_venta_cols = ventas.sum(axis=0)

    # --------- Porcentajes ---------
    if modo_porcentaje == 'fila':
        p_conteos = conteos.div(
            conteos.sum(axis=1).replace(0, np.nan), axis=0
        ).fillna(0.0) * 100.0
        if considerar_gasto:
            p_ventas = ventas.div(
                ventas.sum(axis=1).replace(0.0, np.nan), axis=0
            ).fillna(0.0) * 100.0
    else:
        p_conteos = conteos.div(
            conteos.sum(axis=0).replace(0, np.nan), axis=1
        ).fillna(0.0) * 100.0
        if considerar_gasto:
            p_ventas = ventas.div(
                ventas.sum(axis=0).replace(0.0, np.nan), axis=1
            ).fillna(0.0) * 100.0

    # --------- Paletas ---------
    if colores_status is None:
        colores_status = {
            'nuevo': '#8BC34A',
            'esporadico': '#9E9E9E',
            'creciente': '#2E7D32',
            'estable': '#1976D2',
            'decreciente': '#FB8C00',
            'fugado': '#E53935',
            'recuperado': '#4FC3F7'
        }
    colores_nivel = {
        'Sin Nivel': '#9E9E9E',
        'Socio Club': '#1976D2',
        'Socio Oro': '#F9A825',
        'Socio Platino': '#4FC3F7'
    }

    def _hex_to_rgb01(h: str) -> tuple[float, float, float]:
        h = h.lstrip('#')
        return (
            int(h[0:2], 16) / 255.0,
            int(h[2:4], 16) / 255.0,
            int(h[4:6], 16) / 255.0
        )

    def _build_img(pct: pd.DataFrame, esquema: str) -> np.ndarray:
        n_f, n_c = pct.shape
        img = np.ones((n_f, n_c, 3), dtype=float)
        if esquema == 'fila':
            for i, st in enumerate(pct.index.tolist()):
                cr, cg, cb = _hex_to_rgb01(colores_status.get(st, '#9E9E9E'))
                p = pct.iloc[i].to_numpy() / 100.0
                img[i, :, 0] = (1.0 - p) + p * cr
                img[i, :, 1] = (1.0 - p) + p * cg
                img[i, :, 2] = (1.0 - p) + p * cb
        else:
            for j, nivel in enumerate(pct.columns.tolist()):
                cr, cg, cb = _hex_to_rgb01(colores_nivel.get(nivel, '#BDBDBD'))
                p = pct.iloc[:, j].to_numpy() / 100.0
                img[:, j, 0] = (1.0 - p) + p * cr
                img[:, j, 1] = (1.0 - p) + p * cg
                img[:, j, 2] = (1.0 - p) + p * cb
        return img

    modo = 'fila' if modo_porcentaje == 'fila' else 'columna'
    img_conteos = _build_img(p_conteos, modo)
    if considerar_gasto:
        img_ventas = _build_img(p_ventas, modo)

    # --------- Formateadores y etiquetas ---------
    def _fmt_monto(v: float) -> str:
        sign = '-' if v < 0 else ''
        av = abs(v)
        if av >= 1_000_000:
            m = round(av / 1_000_000)
            return f'{sign}{m:,}M'
        return f'{sign}{round(av):,}'

    x_labels_conteo = [
        f'{str(col).strip().title()}\n{tot_conteo_cols[col]:,}'
        for col in conteos.columns
    ]
    y_labels_conteo = [
        f'{st.capitalize()}\n{tot_conteo_rows[st]:,}'
        for st in conteos.index
    ]
    if considerar_gasto:
        x_labels_venta = [
            f'{str(col).strip().title()}\n{_fmt_monto(tot_venta_cols[col])}'
            for col in conteos.columns
        ]
        y_labels_venta = [
            f'{st.capitalize()}\n{_fmt_monto(tot_venta_rows[st])}'
            for st in conteos.index
        ]

    # --------- Figura(s) ---------
    n_filas, n_cols = conteos.shape
    if considerar_gasto:
        ancho = max(12.0, 1.6 * n_cols + 4.0)
        alto = max(5.0, 0.7 * n_filas + 2.0)
        fig, (ax, ax2) = plt.subplots(
            1, 2, figsize=(ancho, alto), constrained_layout=True
        )
    else:
        # Solo conteos: figura compacta
        ancho = max(9.0, 1.2 * n_cols + 3.0)
        alto = max(5.0, 0.7 * n_filas + 2.0)
        fig, ax = plt.subplots(
            1, 1, figsize=(ancho, alto), constrained_layout=True
        )

    # --- Título general ---
    fig.suptitle(
        titulo or f'Matriz Ciclo de Vida - Niveles {monthid_str}',
        fontsize=16,
        fontweight='bold'
    )

    # Izquierda: CONTEOS solamente
    ax.imshow(img_conteos, aspect='auto')
    ax.set_xticks(range(n_cols))
    ax.set_xticklabels(x_labels_conteo, rotation=45, ha='right')
    ax.set_yticks(range(n_filas))
    ax.set_yticklabels(y_labels_conteo)
    ax.set_xlabel('Nivel')
    ax.set_ylabel('Marca Ciclo de Vida')
    ax.set_title('Conteo')
    for i in range(n_filas):
        for j in range(n_cols):
            n = conteos.iloc[i, j]
            p = p_conteos.iloc[i, j]
            ax.text(j, i, f'{n:,}\n({p:.0f}%)',
                    ha='center', va='center')

    # Derecha: VENTAS (solo si corresponde)
    if considerar_gasto:
        ax2.imshow(img_ventas, aspect='auto')
        ax2.set_xticks(range(n_cols))
        ax2.set_xticklabels(x_labels_venta, rotation=45, ha='right')
        ax2.set_yticks(range(n_filas))
        ax2.set_yticklabels(y_labels_venta)
        ax2.set_xlabel('Nivel')
        ax2.set_ylabel('')
        ax2.set_title('Ventas Totales')
        for i in range(n_filas):
            for j in range(n_cols):
                g = ventas.iloc[i, j]
                p = p_ventas.iloc[i, j]
                ax2.text(
                    j, i, f'{_fmt_monto(g)}\n({p:.0f}%)',
                    ha='center', va='center'
                )

    if mostrar:
        plt.show()

    return fig


def resumen_clientes_ciclo_extendido(df_lc: pd.DataFrame, monthid: str):
    """Dashboard de KPIs para crecimiento, estable y decrecimiento."""

    def obtener_mes_anterior_str(monthid_str: str) -> str:
        year = int(monthid_str[:4])
        month = int(monthid_str[4:6])
        if month == 1:
            year -= 1
            month = 12
        else:
            month -= 1
        return f'{year}{month:02d}'

    def obtener_mes_ano_pasado_str(monthid_str: str) -> str:
        year = int(monthid_str[:4]) - 1
        month = int(monthid_str[4:6])
        return f'{year}{month:02d}'

    estados = ['crecimiento', 'estable', 'decrecimiento']
    colores_estados = {
        'nuevo': '#8BC34A',
        'esporadico': '#9E9E9E',
        'crecimiento': '#2E7D32',
        'estable': '#1976D2',
        'decrecimiento': '#FB8C00',
        'fugado': '#E53935',
        'recuperado': '#4FC3F7'
    }
    colores_niveles = {
        'Socio Club': '#1976D2',
        'Socio Oro': '#F9A825',
        'Socio Platino': '#4FC3F7'
    }

    mes_actual = str(monthid).strip()
    mes_menos_1 = obtener_mes_anterior_str(mes_actual)
    mes_ano_anterior = obtener_mes_ano_pasado_str(mes_actual)

    # --- Filtro temprano ---
    cols_nec = [
        'customer_key', 'monthid', 'status', 'gasto_total',
        'canastas_compradas', 'nivel'
    ]
    df_aux = df_lc[cols_nec].copy()
    df_aux['monthid'] = df_aux['monthid'].astype(str).str.strip()
    df_aux = df_aux[
        df_aux['monthid'].isin([mes_actual, mes_menos_1, mes_ano_anterior]) &
        df_aux['status'].isin(estados) &
        df_aux['customer_key'].notna()
    ]
    if df_aux.empty:
        print('No hay datos para los meses/estados solicitados.')
        return None

    resumen = (
        df_aux.groupby(['monthid', 'status'])
        .agg(
            clientes=('customer_key', 'nunique'),
            gasto_prom=('gasto_total', 'mean'),
            canastas_prom=('canastas_compradas', 'mean')
        )
        .reset_index()
    )
    act_tbl = resumen[resumen['monthid'] == mes_actual].set_index('status')
    m1_tbl = resumen[resumen['monthid'] == mes_menos_1].set_index('status')
    a1_tbl = resumen[resumen['monthid'] == mes_ano_anterior].set_index('status')

    total_actual = df_aux.loc[
        df_aux['monthid'] == mes_actual, 'customer_key'
    ].nunique()

    # --- Figura ---
    fig, axes = plt.subplots(1, 3, figsize=(18, 7))
    plt.subplots_adjust(wspace=0.36)
    fig.suptitle(
        (
            'Resumen clientes leales ciclo de vida - '
            f'{mes_actual}  |  Total clientes: {total_actual:,}'
        ),
        fontsize=15,
        weight='bold',
        y=0.98
    )
    fig.patch.set_facecolor('white')

    def var_pct_str(actual, base):
        if base is None or pd.isna(base) or base == 0:
            return 'S/I'
        return f'{((actual - base) / base * 100):+.1f}%'

    for ax, estado in zip(axes, estados):
        color = colores_estados[estado]

        clientes = (
            act_tbl.loc[estado, 'clientes'] if estado in act_tbl.index else 0
        )
        gasto_prom = (
            act_tbl.loc[estado, 'gasto_prom']
            if estado in act_tbl.index else 0.0
        )
        canastas_prom = (
            act_tbl.loc[estado, 'canastas_prom']
            if estado in act_tbl.index else 0.0
        )

        clientes_m1 = (
            m1_tbl.loc[estado, 'clientes'] if estado in m1_tbl.index else None
        )
        gasto_m1 = (
            m1_tbl.loc[estado, 'gasto_prom'] if estado in m1_tbl.index else None
        )
        canastas_m1 = (
            m1_tbl.loc[estado, 'canastas_prom']
            if estado in m1_tbl.index else None
        )

        clientes_a1 = (
            a1_tbl.loc[estado, 'clientes'] if estado in a1_tbl.index else None
        )
        gasto_a1 = (
            a1_tbl.loc[estado, 'gasto_prom'] if estado in a1_tbl.index else None
        )
        canastas_a1 = (
            a1_tbl.loc[estado, 'canastas_prom']
            if estado in a1_tbl.index else None
        )

        clientes_var_m1 = var_pct_str(clientes, clientes_m1)
        gasto_var_m1 = var_pct_str(gasto_prom, gasto_m1)
        canastas_var_m1 = var_pct_str(canastas_prom, canastas_m1)

        clientes_var_a1 = var_pct_str(clientes, clientes_a1)
        gasto_var_a1 = var_pct_str(gasto_prom, gasto_a1)
        canastas_var_a1 = var_pct_str(canastas_prom, canastas_a1)

        # --- Tarjeta ---
        card = FancyBboxPatch(
            (0.02, 0.02), 0.96, 0.96,
            boxstyle='round,pad=0.02',
            transform=ax.transAxes,
            fc='white',
            ec='lightgray',
            lw=1.2
        )
        ax.add_patch(card)

        header = FancyBboxPatch(
            (0.02, 0.84), 0.96, 0.13,
            boxstyle='round,pad=0.02',
            transform=ax.transAxes,
            fc=color,
            ec='none'
        )
        ax.add_patch(header)

        ax.text(
            0.5, 0.895, estado.capitalize(),
            ha='center', va='center',
            fontsize=16, weight='bold', color='white'
        )

        # --- KPIs ---
        ax.text(
            0.5, 0.77, f'Clientes: {clientes:,}',
            ha='center', fontsize=12, weight='bold'
        )
        ax.text(
            0.3, 0.70, r'$\Delta_{mes\ ant}$: ' + clientes_var_m1,
            ha='center', fontsize=11, color=color
        )
        ax.text(
            0.7, 0.70, r'$\Delta_{año\ ant}$: ' + clientes_var_a1,
            ha='center', fontsize=11, color=color
        )

        ax.text(
            0.5, 0.58, f'Gasto promedio mensual: ${gasto_prom:,.0f}',
            ha='center', fontsize=11
        )
        ax.text(
            0.3, 0.51, r'$\Delta_{mes\ ant}$: ' + gasto_var_m1,
            ha='center', fontsize=11, color=color
        )
        ax.text(
            0.7, 0.51, r'$\Delta_{año\ ant}$: ' + gasto_var_a1,
            ha='center', fontsize=11, color=color
        )

        ax.text(
            0.5, 0.42,
            f'Cantidad canastas mensuales: {canastas_prom:,.1f}',
            ha='center', fontsize=11
        )
        ax.text(
            0.3, 0.35, r'$\Delta_{mes\ ant}$: ' + canastas_var_m1,
            ha='center', fontsize=11, color=color
        )
        ax.text(
            0.7, 0.35, r'$\Delta_{año\ ant}$: ' + canastas_var_a1,
            ha='center', fontsize=11, color=color
        )

        # --- Bloque inferior: torta + leyenda ---
        cont = ax.inset_axes([0.06, 0.05, 0.88, 0.28])
        cont.set_axis_off()

        ax_pie = cont.inset_axes([0.04, 0.10, 0.52, 0.80])
        ax_leg = cont.inset_axes([0.60, 0.18, 0.36, 0.64])
        ax_leg.set_axis_off()

        subdf = df_aux[
            (df_aux['monthid'] == mes_actual) & (df_aux['status'] == estado)
        ]
        if not subdf.empty and subdf['nivel'].notna().any():
            dist = (
                subdf['nivel']
                .value_counts(normalize=True)
                .reindex(colores_niveles.keys())
                .dropna()
            )
            wedges, texts, autotexts = ax_pie.pie(
                dist,
                colors=[colores_niveles[k] for k in dist.index],
                startangle=90,
                autopct='%1.0f%%',
                pctdistance=0.72,
                textprops={'fontsize': 8, 'color': 'black'}
            )

            ax_leg.legend(
                wedges,
                list(dist.index),
                loc='center',
                frameon=False,
                fontsize=9,
                title='Nivel',
                title_fontsize=9,
                borderaxespad=0.0
            )
        else:
            cont.text(
                0.5, 0.5, 'Sin datos de nivel',
                ha='center', va='center', fontsize=8, color='gray'
            )

        ax.axis('off')

    plt.show()
    return fig



#--------------------------------------------------------------------------
# GENERAR PDF
#--------------------------------------------------------------------------


def _construir_estilos() -> dict:
    styles = getSampleStyleSheet()
    return {
        'titulo': ParagraphStyle(
            name='titulo', parent=styles['Heading1'],
            fontName='Helvetica-Bold', fontSize=16, leading=18,
            alignment=TA_LEFT, spaceAfter=8
        ),
        'parrafo': ParagraphStyle(
            name='parrafo', parent=styles['BodyText'],
            fontName='Helvetica', fontSize=11, leading=14,
            alignment=TA_JUSTIFY,   # <-- por defecto justificado
            spaceAfter=8
        ),
        'centrado': ParagraphStyle(
            name='centrado', parent=styles['BodyText'],
            fontName='Helvetica', fontSize=11, leading=14,
            alignment=TA_CENTER, spaceAfter=8
        ),
        'nota': ParagraphStyle(
            name='nota', parent=styles['BodyText'],
            fontName='Helvetica-Oblique', fontSize=9, leading=12,
            textColor='#555555',
            alignment=TA_JUSTIFY,   # <-- justificado
            spaceBefore=4, spaceAfter=6
        ),
        # --- NUEVOS ---
        'subtitulo': ParagraphStyle(
            name='subtitulo', parent=styles['Heading2'],
            fontName='Helvetica-Bold', fontSize=13, leading=16,
            textColor='#333333',
            alignment=TA_LEFT,       # subtítulo usualmente a la izquierda
            spaceBefore=4, spaceAfter=6
        ),
        'subtitulo_centrado': ParagraphStyle(
            name='subtitulo_centrado', parent=styles['Heading2'],
            fontName='Helvetica-Bold', fontSize=13, leading=16,
            alignment=TA_CENTER, textColor='#333333',
            spaceBefore=4, spaceAfter=6
        ),
    }


def _fig_a_flowable(
    fig,
    max_ancho: float,
    max_alto: float,
    dpi: int = 200,
    bbox_inches: str = 'tight',
    pad_inches: float = 0.2,
    close_fig: bool = True
) -> Image:
    """Convierte una figura de Matplotlib en Image escalada a caja dada."""
    buf = io.BytesIO()
    fig.savefig(buf, format='png', dpi=dpi, bbox_inches=bbox_inches, pad_inches=pad_inches)
    if close_fig:
        plt.close(fig)
    buf.seek(0)
    img = Image(buf)
    img._restrictSize(max_ancho, max_alto)  # noqa: SLF001
    return img


def _encabezado_pie(canvas, doc, titulo_doc: str | None):
    canvas.saveState()
    # Pie con número
    canvas.setFont('Helvetica', 9)
    canvas.setFillColorRGB(0.3, 0.3, 0.3)
    canvas.drawRightString(doc.pagesize[0] - doc.rightMargin,
                           doc.bottomMargin * 0.6,
                           f'Página {canvas.getPageNumber()}')
    # Encabezado opcional
    if titulo_doc:
        canvas.setFont('Helvetica', 10)
        canvas.setFillColorRGB(0.2, 0.2, 0.2)
        canvas.drawString(doc.leftMargin,
                          doc.pagesize[1] - doc.topMargin * 0.7,
                          titulo_doc)
    canvas.restoreState()


# ---------- Builder ----------
class PDFDoc:
    def __init__(
        self,
        ruta_salida: str = 'reporte.pdf',
        tam_pagina=A4,                 # o letter
        margen_izq: float = 2.0*cm,
        margen_der: float = 2.0*cm,
        margen_sup: float = 1.8*cm,
        margen_inf: float = 1.8*cm,
        titulo_doc: str | None = None,
        justificar_todos: bool = True   # forzar justificado automático
    ):
        self.ruta_salida = ruta_salida
        self.tam_pagina = tam_pagina
        self.margen_izq = margen_izq
        self.margen_der = margen_der
        self.margen_sup = margen_sup
        self.margen_inf = margen_inf
        self.titulo_doc = titulo_doc
        self.justificar_todos = justificar_todos

        self._doc = SimpleDocTemplate(
            ruta_salida,
            pagesize=tam_pagina,
            leftMargin=margen_izq,
            rightMargin=margen_der,
            topMargin=margen_sup,
            bottomMargin=margen_inf,
            title=titulo_doc or ''
        )
        self._bloques: list[Flowable] = []
        self.estilos = _construir_estilos()

        # Área útil (para escalar imágenes)
        self.ancho_contenido = tam_pagina[0] - margen_izq - margen_der
        self.alto_contenido  = tam_pagina[1] - margen_sup - margen_inf

    # --- Agregadores ---
    def add_text(self, texto: str, estilo: str = 'parrafo', justificar: bool | None = None):
        """Agrega un párrafo. Soporta etiquetas <b>, <i>, <u>, <br/>."""
        base_style = self.estilos.get(estilo, self.estilos['parrafo'])

        # Decidir si se justifica
        must_justify = self.justificar_todos if justificar is None else justificar

        # Evitar justificar estilos centrados explícitos
        if must_justify and getattr(base_style, 'alignment', None) != TA_CENTER:
            style = ParagraphStyle(
                name=f'{base_style.name}_J',
                parent=base_style,
                alignment=TA_JUSTIFY
            )
        else:
            style = base_style

        self._bloques.append(Paragraph(texto, style))

    def add_spacer(self, altura_pts: float = 6):
        """Espaciador vertical (puntos tipográficos)."""
        self._bloques.append(Spacer(1, altura_pts))

    def add_page_break(self):
        """Salto de página."""
        self._bloques.append(PageBreak())

    def add_figure(
        self,
        fig,
        max_ancho: float | None = None,
        max_alto: float | None = None,
        dpi: int = 200,
        close_fig: bool = True,
        bbox_inches: str = 'tight',
        pad_inches: float = 0.2
    ):
        """Inserta una figura de Matplotlib como imagen."""
        if max_ancho is None:
            max_ancho = self.ancho_contenido
        if max_alto is None:
            # por defecto quepa cómodo dejando espacio para texto
            max_alto = self.alto_contenido * 0.78
        img = _fig_a_flowable(
            fig,
            max_ancho=max_ancho,
            max_alto=max_alto,
            dpi=dpi,
            bbox_inches=bbox_inches,
            pad_inches=pad_inches,
            close_fig=close_fig
        )
        self._bloques.append(img)

    # --- Callbacks compartidos ---
    def _on_page(self, canvas, doc):
        _encabezado_pie(canvas, doc, self.titulo_doc)

    # --- Construcción a archivo ---
    def build(self) -> str:
        self._doc.build(
            self._bloques,
            onFirstPage=self._on_page,
            onLaterPages=self._on_page
        )
        return self.ruta_salida

    # --- Construcción a memoria: bytes ---
    def build_bytes(self) -> bytes:
        buffer = io.BytesIO()
        mem_doc = SimpleDocTemplate(
            buffer,
            pagesize=self.tam_pagina,
            leftMargin=self.margen_izq,
            rightMargin=self.margen_der,
            topMargin=self.margen_sup,
            bottomMargin=self.margen_inf,
            title=self.titulo_doc or ''
        )
        mem_doc.build(
            self._bloques,
            onFirstPage=self._on_page,
            onLaterPages=self._on_page
        )
        buffer.seek(0)
        return buffer.getvalue()

    # --- Construcción a memoria: BytesIO ---
    def build_buffer(self) -> io.BytesIO:
        buffer = io.BytesIO()
        mem_doc = SimpleDocTemplate(
            buffer,
            pagesize=self.tam_pagina,
            leftMargin=self.margen_izq,
            rightMargin=self.margen_der,
            topMargin=self.margen_sup,
            bottomMargin=self.margen_inf,
            title=self.titulo_doc or ''
        )
        mem_doc.build(
            self._bloques,
            onFirstPage=self._on_page,
            onLaterPages=self._on_page
        )
        buffer.seek(0)
        return buffer





# -------------------------------------------------------------------------
# Main function
# -------------------------------------------------------------------------
def main() -> None:  # noqa: D103
    usuario = 'lifecyle'
    # Parse input variables
    args = vars(parser.parse_args())
    execution_date: str = args['execution_date']
    proyecto: str = args['project_id']  # noqa: F841
    formato:str = args['store_banner']
    logging.info(f'execution_date: {execution_date}')


    # Set gbq client for all subsequent queries
    gbq_client = Client()


    # REGION: Inputs del proceso
    #----------------------------------------------------------------------


    # Convertir a objeto datetime
    fecha_dt = pendulum.parse(execution_date)

    # Formatear a 'YYYYMM'
    last_monthid = fecha_dt.strftime('%Y%m')

    # Nombre PDF
    nombre_pdf = 'reporte_lc_airflow'

    #----------------------------------------------------------------------
    # ENDREGION


    # REGION: Configuracion inicial
    #----------------------------------------------------------------------

    #---
    # Principales
    #---

    # Usuario
    usuario = 'lifecycle_display'

    # Proyecto en que se almacena
    esquema = 'CONOCIMIENTO_CLIENTE'
    tabla = 'LIFECYCLE_STATUS'



    #----------------------------------------------------------------------
    # ENDREGION

    # REGION: Variables intermedias
    #----------------------------------------------------------------------

    # Base tabla Shabits
    base = 'cl-cda-unidata-prod.DS_PROD_CLIENTES_IC.'


    # ID del formato
    match formato:
        case 'Unimarc':
            formato_id = '01'
            tabla_shabits = f'{base}VW_FACT_MONTH_CUSTOMER_ORGANIZATION_SHABITS_UNIDATA'
        case 'Alvi':
            formato_id = '08'
            tabla_shabits = f'{base}VW_FACT_MONTH_CUSTOMER_ORGANIZATION_SHABITS_UNIDATA_ALVI'
        case 'Super 10':
            formato_id = '15'
        case _:
            msg = f'formato desconocido: {formato!r}'
            raise ValueError(msg)

    # Mes anterior
    prev_monthid = calcular_mes_anterior(last_monthid)

    # Path tabla
    path_tabla = f'{proyecto}.{esquema}.{tabla}'


    #----------------------------------------------------------------------
    # ENDREGION


    # REGION: Parametros iniciales
    #----------------------------------------------------------------------
    # Formato en MAYUSCULAS
    formato_mayusculas = formato.upper()

    logging.info(' ')
    logging.info('--------------------')
    logging.info(f'Se inicia el proceso para {formato_mayusculas} en monthid {last_monthid}')
    logging.info('--------------------')


    #----------------------------------------------------------------------
    # ENDREGION


    # REGION: Consulta GCP
    #----------------------------------------------------------------------

    # Se crea la query
    query_lc = SQL_QUERIES['query_lc'].substitute(
                                                proyecto = proyecto,
                                                path_tabla = path_tabla,
                                                tabla_shabits = tabla_shabits,
                                                formato = formato,
                                                formato_id = formato_id,
                                                last_monthid = last_monthid)
    logging.info('Parte el proceso...')
    logging.info('Inicia la consulta de los últimos 13 meses de la tabla de LC...')

    # Se realiza la consulta
    df_lc = readBigQuery(
        query=query_lc,
        user=usuario,
        gbq_client=gbq_client
    )

    # Nombres de columnas a minusculas
    df_lc.columns = df_lc.columns.str.lower()

    logging.info('Termina la consulta de los últimos 13 meses de la tabla de LC...')
    #----------------------------------------------------------------------
    # ENDREGION

    import gc
    # REGION: Creacion de graficos
    #----------------------------------------------------------------------




    logging.info('Fig1 lista')


    logging.info('Fig2 lista')


    logging.info('Fig3 lista')


    logging.info('Fig4 lista')


    logging.info('Fig5 lista')


    logging.info('Fig6 lista')


    logging.info('Fig7 lista')
    #----------------------------------------------------------------------
    # ENDREGION

    dpi = 40

    # REGION: Creacion del documento
    #----------------------------------------------------------------------

    # 2) Construyes el PDF agregando texto/figuras/saltos
    doc = PDFDoc(ruta_salida=f'{nombre_pdf}.pdf', titulo_doc='')
    logging.info('Documento vacio creado')
    #-----
    # Primera página: Última segmentación
    #-----
    doc.add_text('<b>Resultados de la última segmentación</b>', estilo='titulo')

    doc.add_text('<b>Distribución de clientes</b>', estilo='subtitulo')
    doc.add_text(
        'A continuación se presenta la distribución de clientes por estado de ciclo de vida, '
        'correspondiente a la última ejecución de la segmentación.'
    )
    doc.add_spacer(2)


    fig1 = grafico_torta_estado_mes(df_lc, monthid=last_monthid,
                                    considerar_gasto=False, show=False)

    doc.add_figure(fig1, dpi=dpi, max_ancho=320)

    plt.close(fig1)
    del fig1
    gc.collect()



    doc.add_spacer(10)

    doc.add_text('<b>Matriz Ciclo de Vida vs. Niveles</b>', estilo='subtitulo')
    doc.add_text(
        'El cruce entre los estados de ciclo de vida y '
        'los niveles del programa muestra la siguiente matriz. '
        'Las etiquetas incluyen totales por eje, '
        'y cada celda presenta el valor y su porcentaje relativo.'
    )
    doc.add_spacer(2)


    fig4 = matriz_status_vs_nivel_graficos(df_lc, last_monthid, modo_porcentaje='fila',
                                        considerar_gasto=False)
    doc.add_figure(fig4, dpi=dpi, max_ancho=320)

    plt.close(fig4)
    del fig4
    gc.collect()


    doc.add_page_break()

    #-----
    # Segunda página: KPI segmentación anterior
    #-----
    doc.add_text('<b>Resultados del ciclo anterior</b>', estilo='titulo')

    doc.add_text('<b>Distribución de clientes y ventas</b>', estilo='subtitulo')
    doc.add_text(
        'Se presenta la distribución de clientes y su participación en ventas correspondiente '
        'al mes inmediatamente anterior.'
    )
    doc.add_spacer(2)

    fig2 = grafico_torta_estado_mes(df_lc, monthid=prev_monthid,
                                    considerar_gasto=True,
                                    mostrar_monto_ventas=False, umbral_pct_inside=3.0, show=False)


    doc.add_figure(fig2, dpi=dpi)



    doc.add_spacer(10)

    doc.add_text('<b>Matriz Ciclo de Vida vs. Niveles</b>', estilo='subtitulo')
    doc.add_text(
        'La matriz siguiente resume el cruce de estados '
        'de ciclo de vida con niveles para el ciclo anterior, '
        'considerando las ventas de cada uno de los segmentos.'
    )
    doc.add_spacer(2)


    fig5 = matriz_status_vs_nivel_graficos(df_lc, prev_monthid, modo_porcentaje='fila',
                                        considerar_gasto=True)

    doc.add_figure(fig5, dpi=dpi)

    plt.close(fig5)
    del fig5
    gc.collect()

    doc.add_page_break()

    doc.add_text('<b>Variaciones de clientes constantes</b>', estilo='subtitulo')
    doc.add_text(
        'Los clientes <i>crecientes</i>, <i>estables</i> y <i>decrecientes</i> '
        'son aquellos que han realizado compras superiores a $10.000 en, '
        'al menos, 3 de los últimos 4 meses. '
        'A continuación, se muestran las variaciones en número de '
        'clientes, canastas y ventas para dichos segmentos.'
    )


    fig3 = resumen_clientes_ciclo_extendido(df_lc, monthid=prev_monthid)

    doc.add_figure(fig3, dpi=dpi)

    plt.close(fig3)
    del fig3
    gc.collect()


    doc.add_page_break()

    #-----
    # Tercera página: Análisis en el tiempo
    #-----
    doc.add_text('<b>Resultados en el tiempo</b>', estilo='titulo')

    doc.add_text('<b>Resultados en el <u>mediano plazo</u></b>', estilo='subtitulo')
    doc.add_text('Evolución de la distribución de clientes '
                'por estado durante los últimos 6 meses.')
    doc.add_spacer(2)

    fig6 = grafico_clientes_por_estado(df_lc, n_meses=6, tipo=1)

    doc.add_figure(fig6, dpi=dpi, max_ancho=330)

    plt.close(fig6)
    del fig6
    gc.collect()

    doc.add_spacer(10)

    doc.add_text('<b>Resultados en el <u>largo plazo</u></b>', estilo='subtitulo')
    doc.add_text('Evolución de la distribución de clientes '
                'por estado durante los últimos 12 meses.')
    doc.add_spacer(2)

    fig7 = grafico_lineas_clientes_por_estado(df_lc, 12)

    doc.add_figure(fig7, dpi=dpi, max_ancho=330)

    plt.close(fig7)
    del fig7
    gc.collect()

    logging.info('Se agrega contenido.')

    # 3) En memoria (BytesIO)
    pdf_buf = doc.build_buffer()

    logging.info('Se crea el BytesIO.')
    pdf_buf.seek(0)  # por si acaso SharePoint lo requiere en posición 0


    gcp_project_id = 'cl-bigdata-analytics-preprod'

    sp_extended.SharePointFile(
        **getSecret(
            'bdaa_sharepoint_credentials',
            gcp_project_id,
        ),
        server_relative_path=(
            '/sites/'
            'BigDatayAdvancedAnalytics/'
            'Documentos%20compartidos/'
            'Power%20Automate/'
            'Correos Ciclo de Vida/'
            f'{last_monthid}_{nombre_pdf}_{formato}.pdf'
        )
    ).upload(pdf_buf)

    logging.info('Se manda pdf a SharePoint.')
    #----------------------------------------------------------------------
    # ENDREGION






if __name__ == '__main__':
    main()
