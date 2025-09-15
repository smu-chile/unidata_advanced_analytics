# Default
from __future__ import annotations

import os
import logging
import argparse
from logging import config
from datetime import datetime

# Pip
import numpy as np
import pandas as pd
from google.cloud import bigquery  # noqa: F401
from google.cloud.bigquery import Client
from dateutil.relativedelta import relativedelta

# Own
import common.gcp_extended.bigquery as gbq_extended  # noqa: F401
from common.constants import LOGGING_CONFIG
from common.databases.queries import QueryDict
from common.gcp_extended.bigquery import (  # noqa: E402
    uploadFrame,  # noqa: F401
    readBigQuery,
    deleteFromTable,  # noqa: F401
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
    'query_principal':
"""
    DECLARE meses INT64 DEFAULT 12;
    DECLARE periodo STRING DEFAULT '${last_month}';  -- formato YYYYMM

    DECLARE fecha_final DATE DEFAULT LAST_DAY(PARSE_DATE('%Y%m', periodo), MONTH);

    SELECT
    FORMAT_DATE('%Y%m', TRANSACTION_DATE) AS yyyymm,
    customer_key,
    COUNT(DISTINCT MARKET_BASKET_KEY) AS canastas_compradas,
    SUM(BASKET_VALUE) AS gasto_total
    FROM `cl-bigdata-analytics-preprod.ML_LAB.VW_SALES_BASKET` A
    INNER JOIN `cl-bigdata-analytics-preprod.ML_LAB.VW_DIM_STORE` D
    ON A.STORE_ID = D.STORE_ID
    WHERE TRANSACTION_DATE BETWEEN DATE_TRUNC(DATE_SUB(fecha_final, INTERVAL meses-1 MONTH), MONTH)
                            AND fecha_final
    AND FNC_DOC_TP_DSC IN ('BX', 'BE', 'TF')
    AND ITM_TXN_FCN_TP_DSC = 'V'
    AND MARKET_BASKET_KEY NOT IN (
        SELECT MARKET_BASKET_KEY
        FROM `cl-cda-prod.DS_CDA_VW_SMU.DW_VW_FACT_MARKET_BASKET_E_COMMERCE`
        WHERE CANAL_VENTA IN ('PEDIDOS YA','CORNER SHOP','RAPPI','RAPPI TURBO')
    )
    AND D.STORE_BANNER = '${formato}'
    GROUP BY yyyymm, customer_key
    ORDER BY yyyymm, customer_key;
""",

    # ---------------------------------------------------------------------
    # Query estado anterior: obtiene la segmentación de clientes registrada
    # en la tabla lifecycle_status (parámetro ${path_table_lc}) para el
    # último periodo.
    #
    # Parámetros:
    #   - ${path_table_lc}: ruta completa de la tabla lifecycle_status.
    #   - ${last_month}: periodo de segmentación en formato YYYYMM.
    #   - ${formato}: banner de tienda (ej. 'Unimarc').
    #
    # Retorna:
    #   - Todas las columnas almacenadas en la tabla lifecycle_status
    #     correspondientes al periodo y banner indicados.
    # ---------------------------------------------------------------------
    'query_estado_anterior':
"""
    SELECT *
    FROM `${path_table_lc}`
    WHERE monthid = '${last_month}'
      AND store_banner = '${formato}'

"""
})


# -------------------------------------------------------------------------
# Functions and Classes
# -------------------------------------------------------------------------
def segmentarVentanaOptimizada(
    periodo_final: int,
    df_procesado: pd.DataFrame,
    umbral_tasa: float = 0.15,
    df_segmentacion_anterior: pd.DataFrame | None = None
) -> pd.DataFrame:
    """Segmenta clientes mirando siempre el estado del ciclo anterior.

    Reglas base (siempre respecto al ciclo anterior):
      - Primer mes (sin segmentación previa):
          * Compran en p-1 => 'onboard1'
          * Resto visto en ventana => 'sin estado'
      - Con segmentación previa:
          * 'sin estado' o nuevos que compran en p-1 => 'onboard1'
          * 'onboard1' previo => 'onboard2'
          * 'onboard2' previo:
              - actividad últimos 3 meses (p-3..p-1):
                  1 => 'sin estado'
                  2 => 'esporadico'
                  3 => candidato G/R/R (vía OLS)
          * 'grow'/'reward'/'retain' o 'esporadico' previos:
              - en p-4..p-1:
                  >=3 meses => candidato G/R/R (vía OLS)
                  1-2 meses => 'winback'
                  si p-2 y p-1 son 0; si no => 'esporadico'
          * 'winback' previo:
              - sólo compra en p-1 (y 0 en p-4..p-2) => 'recuperado'
              - 0 compras en p-4..p-1 => 'sin estado'
              - p-1==0 y p-2==0 => 'winback'
              - otro caso => 'esporadico'
          * 'recuperado' previo => 'esporadico'

    Clasificación G/R/R (OLS con z_offset):
      - z_offset ~ a + beta1 * t, con t=0..3 en p-4..p-1
      - tasa_mensual = expm1(beta1)
      - grow si tasa >=  umbral_tasa
        reward si |tasa| < umbral_tasa
        retain si tasa <= -umbral_tasa
      - Si no hay datos suficientes => 'esporadico'

    Restricción pedida:
      - No puede existir 'sin estado' en dos ciclos consecutivos.
        Si la propuesta actual es 'sin estado' y el ciclo anterior
        fue 'sin estado', el cliente se EXCLUYE del dataframe resultante
        (sale de la segmentación en ese ciclo).

    Parámetros
    ----------
    periodo_final : int
        Corte de segmentación YYYYMM.
    df_procesado : pd.DataFrame
        Requiere columnas: 'periodo' (YYYYMM/int/str o Period[M]),
        'customer_key', 'gasto_total', 'z_offset'.
    umbral_tasa : float
        Umbral absoluto para G/R/R.
    df_segmentacion_anterior : pd.DataFrame | None
        Segmentación previa con ['customer_key','monthid','status'].

    Retorno
    -------
    pd.DataFrame
        ['customer_key','monthid','beta1','tasa_mensual',
         'meses_activos','status'].
    """
    # -------- util print compatible con 'print' si existe ----------
    try:
        _printer = print  # type: ignore[name-defined]
    except Exception:  # noqa: BLE001
        _printer = print

    def _p(msg: str) -> None:
        _printer(msg)

    # ---------------------- validación ------------------------------
    req = {'periodo', 'customer_key', 'gasto_total', 'z_offset'}
    faltan = req - set(df_procesado.columns)
    if faltan:
        msg = f'Faltan columnas en df_procesado: {faltan}'
        raise ValueError(msg)

    # ---------------------- normalizar customer_key -----------------
    def _norm_key(x):
        if isinstance(x, memoryview):
            return x.tobytes()
        if isinstance(x, bytearray):
            return bytes(x)
        return x

    df_procesado['customer_key'] = df_procesado['customer_key'].map(_norm_key)
    if df_segmentacion_anterior is not None and not df_segmentacion_anterior.empty:
        df_segmentacion_anterior['customer_key'] = (
            df_segmentacion_anterior['customer_key'].map(_norm_key)
        )

    # Normalizar periodo (anti-DeprecationWarning)
    p_final = pd.Period(str(periodo_final), 'M')
    if not isinstance(df_procesado['periodo'].dtype, pd.PeriodDtype):
        df_procesado = df_procesado.copy()
        df_procesado['periodo'] = pd.PeriodIndex(
            df_procesado['periodo'].astype(str), freq='M'
        )

    # Ventanas
    p_m1 = p_final - 1
    ult_3 = pd.period_range(p_final - 3, p_final - 1, freq='M')  # p-3..p-1
    ult_4 = pd.period_range(p_final - 4, p_final - 1, freq='M')  # p-4..p-1

    # Subconjuntos
    df_m1 = df_procesado[df_procesado['periodo'] == p_m1].copy()
    df_ult3 = df_procesado[df_procesado['periodo'].isin(ult_3)].copy()
    df_ult4 = df_procesado[df_procesado['periodo'].isin(ult_4)].copy()

    # Actividad binaria
    df_m1['activo'] = (df_m1['gasto_total'] > 0).astype('int8')
    df_ult3['activo'] = (df_ult3['gasto_total'] > 0).astype('int8')
    df_ult4['activo'] = (df_ult4['gasto_total'] > 0).astype('int8')

    # Pivot actividad
    piv3 = (
        df_ult3.pivot_table(index='customer_key', columns='periodo',
                            values='activo', aggfunc='max', fill_value=0)
        .reindex(columns=list(ult_3), fill_value=0)
        .rename(columns={ult_3[0]: 'p_m3', ult_3[1]: 'p_m2', ult_3[2]: 'p_m1'})
        .reset_index()
    )
    piv4 = (
        df_ult4.pivot_table(index='customer_key', columns='periodo',
                            values='activo', aggfunc='max', fill_value=0)
        .reindex(columns=list(ult_4), fill_value=0)
        .rename(columns={ult_4[0]: 'p_m4', ult_4[1]: 'p_m3',
                         ult_4[2]: 'p_m2', ult_4[3]: 'p_m1'})
        .reset_index()
    )
    act_m1 = (
        df_m1[['customer_key', 'activo']]
        .drop_duplicates('customer_key')
        .rename(columns={'activo': 'act_m1'})
    )

    piv3['act_3m'] = piv3[['p_m3', 'p_m2', 'p_m1']].sum(axis=1).astype(int)
    piv4['act_4m'] = piv4[['p_m4', 'p_m3', 'p_m2', 'p_m1']].sum(axis=1).astype(int)

    # Universo: solo p-4..p-1 o p-1
    universe_keys = pd.Index(
        pd.concat([piv4['customer_key'], df_m1['customer_key']],
                  ignore_index=True).drop_duplicates()
    )

    # + incluir quienes tenían estado previo distinto de 'sin estado'
    if df_segmentacion_anterior is not None and not df_segmentacion_anterior.empty:
        prev_periodo = df_segmentacion_anterior['monthid'].astype(str).max()
        prev = df_segmentacion_anterior.loc[
            df_segmentacion_anterior['monthid'] == prev_periodo,
            ['customer_key', 'status']
        ].copy()
        prev['status'] = prev['status'].astype('string').fillna('sin estado')
        prev_no_sin = pd.Index(prev.loc[prev['status'] != 'sin estado', 'customer_key'])
        universe_keys = universe_keys.union(prev_no_sin)

    # Segmentación previa
    prev_map = {}
    if df_segmentacion_anterior is not None and not df_segmentacion_anterior.empty:
        prev_periodo = df_segmentacion_anterior['monthid'].astype(str).max()
        prev = (
            df_segmentacion_anterior[
                df_segmentacion_anterior['monthid'] == prev_periodo
            ][['customer_key', 'status']]
            .copy()
        )
        prev['status'] = prev['status'].astype('string').fillna('sin estado')
        prev_map = dict(zip(prev['customer_key'], prev['status']))

    # Base
    base = pd.DataFrame({'customer_key': universe_keys})
    base = (
        base.merge(piv3, on='customer_key', how='left')
            .merge(piv4, on='customer_key', how='left', suffixes=('', '_4'))
            .merge(act_m1, on='customer_key', how='left')
            .fillna(0)
    )
    for c in ['p_m4', 'p_m3', 'p_m2', 'p_m1', 'act_3m', 'act_4m', 'act_m1']:
        base[c] = base[c].astype(int)

    base['prev_clasif'] = base['customer_key'].map(prev_map).fillna('no_existia')

    # ----------------- propuesta (antes de OLS) ---------------------
    def _proponer(row: pd.Series) -> str:
        prev_c = row['prev_clasif']
        act_m1_ = row['act_m1']
        act_3m_ = row['act_3m']
        act_4m_ = row['act_4m']
        pm1, pm2, pm3, pm4 = row['p_m1'], row['p_m2'], row['p_m3'], row['p_m4']

        # Primer mes: sin previa
        if not prev_map:
            return 'onboard1' if act_m1_ == 1 else 'sin estado'

        # Nuevos o 'sin estado' que compran en p-1
        if prev_c in ('sin estado', 'no_existia') and act_m1_ == 1:
            return 'onboard1'

        # Onboard1 -> Onboard2
        if prev_c == 'onboard1':
            return 'onboard2'

        # Onboard2: últimos 3 meses
        if prev_c == 'onboard2':
            if act_3m_ <= 1:
                return 'sin estado'
            if act_3m_ == 2:
                return 'esporadico'
            return 'GRR_CANDIDATO'  # 3/3

        # G/R/R o esporádico previos
        if prev_c in ('grow', 'reward', 'retain', 'esporadico'):
            if act_4m_ >= 3:
                return 'GRR_CANDIDATO'
            if (pm2 == 0) and (pm1 == 0):
                return 'winback'
            return 'esporadico'

        # Winback previo
        if prev_c == 'winback':
            if (pm1 == 1) and (pm2 == 0) and (pm3 == 0) and (pm4 == 0):
                return 'recuperado'
            if act_4m_ == 0:
                return 'sin estado'
            if (pm1 == 0) and (pm2 == 0):
                return 'winback'
            return 'esporadico'

        # Recuperado previo
        if prev_c == 'recuperado':
            return 'esporadico'

        return 'sin estado'

    base['propuesta'] = base.apply(_proponer, axis=1)

    # ------------------ OLS para candidatos G/R/R -------------------


    def _norm_key(x):
        if isinstance(x, memoryview):
            return x.tobytes()
        if isinstance(x, bytearray):
            return bytes(x)
        return x  # bytes/str quedan igual

    # Normaliza SIEMPRE antes de usar base/cand_keys/seg_last4:
    df_procesado['customer_key'] = df_procesado['customer_key'].map(_norm_key)
    base['customer_key'] = base['customer_key'].map(_norm_key)
    if df_segmentacion_anterior is not None and not df_segmentacion_anterior.empty:
        df_segmentacion_anterior['customer_key'] = (
            df_segmentacion_anterior['customer_key'].map(_norm_key)
        )


    cand_keys = base.loc[base['propuesta'] == 'GRR_CANDIDATO', 'customer_key'].unique()


    if cand_keys.size > 0:
        seg_last4 = df_procesado[
            (df_procesado['periodo'].isin(ult_4)) &
            (df_procesado['customer_key'].isin(cand_keys))
        ][['customer_key', 'periodo', 'z_offset']].copy()

        t_map = {p: i for i, p in enumerate(ult_4)}  # p-4:0 ... p-1:3
        seg_last4['t'] = seg_last4['periodo'].map(t_map).astype(float)
        seg_last4['t_mul_z'] = seg_last4['t'] * seg_last4['z_offset']

        stats = seg_last4.groupby('customer_key', as_index=False).agg(
            n_obs=('t', 'count'),
            sum_x=('t', 'sum'),
            sum_x2=('t', lambda s: np.square(s).sum()),
            sum_y=('z_offset', 'sum'),
            sum_xy=('t_mul_z', 'sum')
        )


        den = stats['n_obs'] * stats['sum_x2'] - np.square(stats['sum_x'])
        stats['beta1'] = np.where(
            den != 0,
            (stats['n_obs'] * stats['sum_xy'] - stats['sum_x'] * stats['sum_y']) / den,
            np.nan
        )
        stats['tasa_mensual'] = np.expm1(stats['beta1'])

        def _cls_row(r: pd.Series) -> str:
            # Si no hay base suficiente, cae a 'esporadico'
            if (r['n_obs'] or 0) < 2 or pd.isna(r['beta1']):
                return 'ERROR'
            if r['tasa_mensual'] >= umbral_tasa:
                return 'grow'
            if r['tasa_mensual'] <= -umbral_tasa:
                return 'retain'
            return 'reward'

        stats['cls_grr'] = stats.apply(_cls_row, axis=1)
        grr = stats[['customer_key', 'beta1', 'tasa_mensual', 'cls_grr']].copy()

    else:
        grr = pd.DataFrame(columns=['customer_key', 'beta1', 'tasa_mensual', 'cls_grr'])

    # ------------------ materializar y aplicar restricción -----------
    base = base.merge(grr, on='customer_key', how='left')

    def _final_cls(row: pd.Series) -> str:
        if row['propuesta'] == 'GRR_CANDIDATO':
            return row['cls_grr'] if pd.notna(row.get('cls_grr')) else 'esporadico'
        return row['propuesta']

    base['status'] = base.apply(_final_cls, axis=1)

    # Regla: NO permitir 'sin estado' consecutivo -> excluirlos
    elim_consec = 0
    if prev_map:
        mask_consec = (base['status'] == 'sin estado') & (
            base['prev_clasif'] == 'sin estado')
        elim_consec = int(mask_consec.sum())
        base = base.loc[~mask_consec].copy()

    # meses_activos en 4 meses
    base['meses_activos'] = base['act_4m'].astype(int)

    # beta1/tasa solo para G/R/R; resto NaN
    is_grr = base['status'].isin(['grow', 'reward', 'retain'])
    base.loc[~is_grr, ['beta1', 'tasa_mensual']] = np.nan

    # Salida
    out = base[['customer_key', 'meses_activos', 'status', 'beta1', 'tasa_mensual']].copy()
    out['monthid'] = str(p_final).replace('-', '')
    out = out[['customer_key', 'monthid',
               'beta1', 'tasa_mensual', 'meses_activos', 'status']]

    # ------------------ prints de control ---------------------------
    vc = out['status'].astype('string').value_counts().to_dict()
    def _get(d, k):
        return int(d.get(k, 0))

    _p(f'Seg {p_final} listo. Dist:')
    _p(f"  onboard1: {_get(vc,'onboard1'):,} "
       f"| onboard2: {_get(vc,'onboard2'):,}")
    _p(f"  grow: {_get(vc,'grow'):,} "
       f"| reward: {_get(vc,'reward'):,} "
       f"| retain: {_get(vc,'retain'):,}")
    _p(f"  esporadico: {_get(vc,'esporadico'):,} "
       f"| winback: {_get(vc,'winback'):,} "
       f"| recuperado: {_get(vc,'recuperado'):,}")
    _p(f"  sin estado: {_get(vc,'sin estado'):,}")

    if prev_map:
        _p(f"  eliminados por 'sin estado' "
           f"consecutivo: {elim_consec:,}")
    else:
        _p('')

    return out


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




# -------------------------------------------------------------------------
# Main function
# -------------------------------------------------------------------------
def main() -> None:  # noqa: D103
    usuario = 'lifecyle'
    # Parse input variables
    args = vars(parser.parse_args())
    execution_date: str = args['execution_date']
    proyecto: str = args['project_id']  # noqa: F841
    logging.info(f'execution_date: {execution_date}')


    # Set gbq client for all subsequent queries
    gbq_client = Client()


    # REGION: Inputs del proceso
    #----------------------------------------------------------------------

    # Formato a calcular
    formato = 'Unimarc'

    # Parámetros de clasificación
    umbral_tasa = 0.15

    # Mes 0
    mes0 = '202411'


    #----------------------------------------------------------------------
    # ENDREGION


    # REGION: Configuracion inicial
    #----------------------------------------------------------------------

    #---
    # Principales
    #---

    # Usuario
    usuario = 'lifecycle'

    # Proyecto en que se almacena
    esquema = 'CONOCIMIENTO_CLIENTE'
    tabla = 'LIFECYCLE_STATUS'

    # Ruta completa
    path_table_lc = f'{proyecto}.{esquema}.{tabla}'


    #----------------------------------------------------------------------
    # ENDREGION


    # REGION: Parametros iniciales
    #----------------------------------------------------------------------
    # Formato en MAYUSCULAS
    formato_mayusculas = formato.upper()

    logging.info(' ')
    logging.info('--------------------')
    logging.info(f'Se inicia el proceso para {formato_mayusculas}')
    logging.info('--------------------')


    #----------------------------------------------------------------------
    # ENDREGION


    # REGION: Query de tabla principal
    #----------------------------------------------------------------------

    lista_monthid = ['202412']



    for monthid in lista_monthid:
        logging.info('------------------------------')
        logging.info(f'El mes a calcular es {monthid}')
        logging.info('------------------------------')
        logging.info(' ')
        # Año movil: Primer mes
        last_month = calcular_mes_anterior(monthid)
        logging.info(f'El ultimo mes del que se tendran transacciones es {last_month}')



        # Se genera query definitiva
        query_principal = SQL_QUERIES['query_principal'].substitute(formato=formato,
                                                                    last_month = last_month)


        logging.info('Inicia la consulta de principal ...')

        df_datos = readBigQuery(
            query=query_principal,
            user=usuario,
            gbq_client=gbq_client
        )

        # Nombres columnas a minusculas
        df_datos.columns = df_datos.columns.str.lower()

        logging.info('Termina la consulta de principal ...')

        #----------------------------------------------------------------------
        # ENDREGION


        # REGION: Procesamiento de data
        #----------------------------------------------------------------------

        # =========================
        # 1) Copia y estandarización
        # =========================
        df_procesado = df_datos.copy()
        logging.info('Copia de df_datos -> df_procesado')

        # Asegurar columnas esperadas
        cols_necesarias = {'yyyymm', 'customer_key', 'gasto_total'}
        faltantes = cols_necesarias - set(df_procesado.columns.str.lower())
        if faltantes:
            msg = f'Faltan columnas requeridas: {faltantes}'
            raise ValueError(msg)

        # Homogeneizar nombres por si vienen en otra capitalización
        df_procesado.columns = df_procesado.columns.str.lower()

        # yyyymm -> Period[M]
        df_procesado['yyyymm'] = df_procesado['yyyymm'].astype(str)
        df_procesado['periodo'] = pd.to_datetime(
            df_procesado['yyyymm'], format='%Y%m').dt.to_period('M')

        # tipos numéricos y nulos
        df_procesado['gasto_total'] = pd.to_numeric(
            df_procesado['gasto_total'], errors='coerce').fillna(0.0)

        # Se recorta columna
        df_procesado = df_procesado[df_procesado['yyyymm'] >= mes0]


        logging.info('Columnas estandarizadas y periodo creado (Period[M])')

        # =========================
        # 1.1) Filtro: eliminar meses con gasto < 10.000
        # =========================


        # Aplicar filtro de gasto >= 10.000
        df_procesado = df_procesado[df_procesado['gasto_total'] >= 10_000].copy()


        # =========================
        # 2) Baseline mensual
        # =========================
        # Baseline = promedio de gasto por cliente activo por mes
        activos = df_procesado[df_procesado['gasto_total'] > 0]
        baseline_mensual = (
            activos.groupby('periodo', as_index=False)['gasto_total']
            .mean()
            .rename(columns={'gasto_total': 'baseline_mensual'})
        )
        logging.info('Baseline mensual calculado (promedio por cliente activo)')

        # Unir baseline y calcular offset log
        df_procesado = df_procesado.merge(baseline_mensual, on='periodo', how='left')
        df_procesado['baseline_mensual'] = df_procesado['baseline_mensual'].fillna(0.0)

        df_procesado['z_offset'] = np.log1p(df_procesado['gasto_total']
                                            ) - np.log1p(df_procesado['baseline_mensual'])
        logging.info('Offset log aplicado: z_offset = log1p(gasto) - log1p(baseline)')

        #----------------------------------------------------------------------
        # ENDREGION




        # REGION: Calculo de los estados
        #----------------------------------------------------------------------


        # Si el mes anterior es el mes0 entonces no hay mes anterior
        if last_month == mes0:
            # PRIMER periodo: todos -> onboard
            seg_monthid = segmentarVentanaOptimizada(
                int(monthid), df_procesado,
                umbral_tasa=umbral_tasa
            )


        # Si el mes anterior no es el mes0, entonces hay que obtener la
        # segmentación anterior.
        else:
            # Se genera query de segmentacion anterior
            query_anterior = SQL_QUERIES['query_estado_anterior'].substitute(
                                                                    last_month = last_month,
                                                                    formato = formato,
                                                                    path_table_lc = path_table_lc
                                                                    )

            logging.info('Inicia la consulta de de la segmentación anterior ...')

            seg_monthid_anterior = readBigQuery(
                query=query_anterior,
                user=usuario,
                gbq_client=gbq_client
            )

            seg_monthid_anterior.columns = seg_monthid_anterior.columns.str.lower()

            # Se genera segmentacion mes actual
            seg_monthid = segmentarVentanaOptimizada(
            int(monthid), df_procesado,
            umbral_tasa=umbral_tasa,
            df_segmentacion_anterior=seg_monthid_anterior
            )


        # Se elimina particion anterior si es que existia
        deleteFromTable(
        table_ref=path_table_lc,
        where_clause=f"monthid = '{monthid}' and store_banner = '{formato}'",
        gbq_client=gbq_client,
        )
        logging.info(f'Se borra la partición actual de {monthid}')


        # Se agrega el formato y se sube a GCP

        seg_monthid['store_banner'] = formato

        uploadFrame(
            seg_monthid[['customer_key','monthid','status','store_banner']],
            table_ddl_json_path=os.path.join('..','..','gbq_objects', 'ingest_lifecycle.json'),
            project=proyecto,
            gbq_client=gbq_client,
            if_exists='append')

        logging.info(f'Se escribe la nueva particion de {monthid}')

        #plotDistribucionClientes(seg_monthid)  # noqa: ERA001
        logging.info('\n\n')



        #----------------------------------------------------------------------
        # ENDREGION



if __name__ == '__main__':
    main()
