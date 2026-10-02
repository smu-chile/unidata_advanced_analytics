"""Construye el tablón de regresión agregado a nivel (CATEGORIA, SUBCATEGORIA).

Replica la metodología de `processed_regression_data.py` (nivel SKU),
subiendo un nivel de agregación. Ver `tablon_agregado_comun.py` para la
lógica compartida y las decisiones metodológicas documentadas ahí.

"""  # noqa: W505
from __future__ import annotations  # noqa: I001

import os
import logging
import argparse
from logging import config

import numpy as np
import pandas as pd
import pendulum
from common.constants import LOGGING_CONFIG
from google.cloud.bigquery import Client
from common.gcp_extended.bigquery import (
    uploadFrame,
    readBigQuery,
    deleteFromTable,
    setTableExpiration,
    createTableAsSelect,
)

from tablon_agregado_comun import (
    QUERY_APOTEOSICO,
    QUERY_PROMO_DIARIA,
    QUERY_DIAS_VENTA_MAYOR,
    ConfiguracionNivel,
    construir_frac_promo,
    aplicar_filtro_frescura,
    construir_cerca_feriado,
    construir_pesos_mensuales,
    construir_desc_promocional,
    construir_historico_precio,
    construir_query_sku_diario,
    construir_unidad_dominante,
    construir_query_master_table,
    construir_indice_precio_grupo,
    construir_participacion_categoria,
    construir_variacion_peer_categoria,
    construir_cantidad_y_cobertura_grupo,
)


config.dictConfig(LOGGING_CONFIG)

CFG = ConfiguracionNivel(
    columna_origen='GRUPO_DSC',  # ya nativa en query_master_table, ver docstring
    columna_salida='SUB_CATEGORY_DESCRIPTION',
    nombre_grupo='subcategoria',
    tabla_destino='cl-bigdata-analytics-preprod.PRECIO_PROMOCIONES.'
                  'TMP_REGRESSION_PROCESSED_DATA_ELASTICITY_SUBCATEGORIA',
    nombre_json='ingest_regression_processed_data_elasticity_subcategoria.json',
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

    cant_meses = 29
    fecha_ejecucion = pendulum.parse(execution_date)
    fecha_final = fecha_ejecucion.start_of('month').subtract(days=1)
    fecha_inicial = fecha_final.subtract(months=cant_meses).add(months=1).start_of('month')

    # REGION: tabla maestra (misma logica que el script SKU, + columna de
    # grupo)
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
    grupo_col = CFG.columna_salida.lower()  # convencion lowercase, igual que el script original
    df_sku_diario = readBigQuery(query=query_sku, user=usuario, gbq_client=gbq_client)
    df_sku_diario.columns = df_sku_diario.columns.str.lower()
    df_sku_diario['p_date'] = pd.to_datetime(df_sku_diario['p_date'])
    df_sku_diario = df_sku_diario[df_sku_diario['precio_promedio_sku'] > 0]
    df_sku_diario = df_sku_diario[df_sku_diario[grupo_col].notna()]
    logging.info(f'Panel SKU-dia: {len(df_sku_diario):,} filas')

    # REGION: unidad de medida dominante (fija, sobre todo el historico)
    dominante = construir_unidad_dominante(df_sku_diario, grupo_col)
    df_sku_diario = df_sku_diario.merge(
        dominante[[grupo_col, 'sales_uom_dominante', 'pct_valor_unidad_dominante']],
        on=grupo_col, how='inner',
    )
    df_sku_diario = df_sku_diario[
        df_sku_diario['sales_uom'] == df_sku_diario['sales_uom_dominante']
    ].copy()
    logging.info(f'Filtrado a unidad dominante: {len(df_sku_diario):,} filas')

    # REGION: pesos mensuales (mes anterior) + indice de precio ponderado
    pesos = construir_pesos_mensuales(df_sku_diario, grupo_col)
    indice_precio = construir_indice_precio_grupo(df_sku_diario, pesos, grupo_col)
    cantidad_cobertura = construir_cantidad_y_cobertura_grupo(df_sku_diario, grupo_col)
    participacion = construir_participacion_categoria(df_sku_diario, grupo_col)
    pct_unidad = dominante[[grupo_col, 'pct_valor_unidad_dominante']]

    df_grupo = indice_precio.merge(
        cantidad_cobertura, on=[grupo_col, 'p_date'], how='inner'
    )
    df_grupo['p_month'] = df_grupo['p_date'].dt.year * 100 + df_grupo['p_date'].dt.month
    df_grupo = df_grupo.merge(participacion, on=[grupo_col, 'p_month'], how='left')
    df_grupo = df_grupo.merge(pct_unidad, on=grupo_col, how='left')
    logging.info(f'Indice de grupo construido: {len(df_grupo):,} filas')

    # REGION: dias de venta mayor (a nivel categoria, igual que el script
    # original)
    categoria_de_grupo = df_sku_diario[['category_description', grupo_col]].drop_duplicates()
    query_dias = QUERY_DIAS_VENTA_MAYOR.substitute(table_master=tmp_path_table_aux)
    df_dias_especiales = readBigQuery(query=query_dias, user=usuario, gbq_client=gbq_client)
    df_dias_especiales.columns = df_dias_especiales.columns.str.lower()
    df_dias_especiales['p_date'] = pd.to_datetime(df_dias_especiales['p_date'])
    df_grupo = df_grupo.merge(categoria_de_grupo, on=grupo_col, how='left')
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
    frac_promo = construir_frac_promo(df_sku_diario, df_promo, grupo_col)
    df_grupo = df_grupo.merge(frac_promo, on=[grupo_col, 'p_date'], how='left')
    df_grupo['frac_promo'] = df_grupo['frac_promo'].fillna(0.0)

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
        df_apo_expandido = pd.concat(expandido, ignore_index=True).drop_duplicates()
        desc_promocional = construir_desc_promocional(df_sku_diario, df_apo_expandido, grupo_col)
    else:
        desc_promocional = df_grupo[[grupo_col, 'p_date']].copy()
        desc_promocional['desc_promocional'] = 0.0
    df_grupo = df_grupo.merge(desc_promocional, on=[grupo_col, 'p_date'], how='left')
    df_grupo['desc_promocional'] = df_grupo['desc_promocional'].fillna(0.0)
    logging.info('Flags de calendario y promocion agregados...')

    # REGION: historico de precio (3 meses previos), variacion vs. pares de
    # categoria, y filtro de frescura
    df_grupo = construir_historico_precio(df_grupo, grupo_col)
    df_grupo = construir_variacion_peer_categoria(df_grupo)
    df_grupo = aplicar_filtro_frescura(df_grupo, grupo_col)
    logging.info(f'Historico, variacion de pares y frescura aplicados: {len(df_grupo):,} filas')

    # REGION: reordenar, agregar store_banner, subir
    df_grupo['store_banner'] = store_banner
    df_grupo['precio_promedio'] = np.exp(df_grupo['ln_p_grupo']).round(0)
    df_grupo['precio_medio_anterior'] = df_grupo['precio_medio_anterior'].round(0)
    df_grupo['ventas_totales_producto'] = df_grupo['ventas_totales_producto'].round(0)
    df_grupo['p_week'] = df_grupo['p_date'].dt.isocalendar().week.astype(int)
    # p_month ya existia (calculado antes para el merge de participacion)
    columnas_finales = [
        'store_banner', 'category_description', grupo_col,
        'p_date', 'p_week', 'p_month',
        'precio_promedio', 'cantidad_total', 'ventas_totales_producto',
        'n_sku_activos', 'cobertura', 'pct_valor_unidad_dominante',
        'participacion_categoria', 'primer_dia_mes', 'ultimo_dia_mes',
        'multiplicador_x05', 'proporcion_categoria', 'cerca_feriado',
        'frac_promo', 'desc_promocional', 'precio_medio_anterior',
        'variacion_porcentual', 'variacion_porcentual_categoria',
    ]
    df_grupo.columns = df_grupo.columns.str.lower()
    columnas_existentes = [c for c in columnas_finales if c in df_grupo.columns]
    df_final = df_grupo[columnas_existentes].copy()

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
