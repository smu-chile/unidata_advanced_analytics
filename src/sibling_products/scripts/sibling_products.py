
# Default
import os
import re
import logging
import argparse
from logging import config
from datetime import datetime, timedelta

import numpy as np

# pip
import pandas as pd
import pendulum
from google.cloud.bigquery import Client

# Own
import common.gcp_extended.bigquery as gbq_extended
from common.constants import LOGGING_CONFIG
from common.databases.queries import QueryDict


# Logging config
config.dictConfig(LOGGING_CONFIG)

# Parser config
parser = argparse.ArgumentParser()
parser.add_argument('--project_name', type=str,
                    help='Name of the Advanced Analytics project executed')

parser.add_argument('--project_id', type=str,
                    help='GCP project in which the script will be executed')

parser.add_argument('--execution_date', type=str, help='DAG execution date')

parser.add_argument('--start_date', type=str,
                    help='Fecha inicial para filtrar datos (YYYY-MM-DD)')

parser.add_argument('--end_date', type=str, help='Fecha final para filtrar datos (YYYY-MM-DD)')

# -------------------------------------------------------------------------
# SQL Querie
# -------------------------------------------------------------------------
WORKFLOW_QUERIES = QueryDict({
    'nuevos_productos': """
    SELECT
        A.SKU_PRODUCT AS SKU,
        A.transaction_date AS fecha,
        P.UNIDAD_DE_MEDIDA,
        P.EAN,
        P.NM,
        P.CAT_DSC,
        P.GRUPO_DSC,
        P.BRAND_DESC,
        P.CONTENIDO_BRUTO,
        ENVA.descripcion AS tipo_envase,
        COUNT(DISTINCT A.TXN_KEY) AS cantidad_transacciones
    FROM `${gcp_project}.CDA_VISTAS.VW_SALES_ITEM` A
    JOIN (
    SELECT DISTINCT
        SKU_PRODUCT,
        EAN,
        UNIDAD_DE_MEDIDA,
        NM,
        CAT_DSC,
        GRUPO_DSC,
        BRAND_DESC,
        CONTENIDO_BRUTO
    FROM `${gcp_project}.CDA_VISTAS.VW_DIM_PRODUCT`
    ) P ON A.EAN = P.EAN
    JOIN `cl-cda-prod.DS_CDA_VW_SMU.DW_VW_DIM_SKU_ATTR` ATTR ON A.SKU_PRODUCT =
        REPLACE(ATTR.SKU_NK,'SKU^CL^SMC^','')
    JOIN `cl-cda-prod.DS_CDA_VW_SMU.DW_VW_DIM_ENVASE` ENVA ON ATTR.envase = ENVA.codigo
    WHERE
        A.transaction_date BETWEEN '${start_date}' AND '${end_date}'
        AND A.SKU_PRODUCT <> 'None'
        AND A.transaction_type IN ('BX', 'BE', 'TF')
        AND A.itm_txn_fcn_tp_dsc = 'V'
        AND A.unit_price > 0
        AND A.value > 0
    GROUP BY
        A.SKU_PRODUCT,
        A.TRANSACTION_DATE,
        P.NM,
        P.UNIDAD_DE_MEDIDA,
        P.EAN,
        P.CAT_DSC,
        P.GRUPO_DSC,
        P.BRAND_DESC,
        P.CONTENIDO_BRUTO,
        ENVA.descripcion
    ORDER BY
        A.TRANSACTION_DATE,
        A.SKU_PRODUCT
        """,
    'extraer_confirmados': """
    SELECT
        SKU_ANTIGUO,
        SKU_NUEVO
    FROM `${gcp_project}.DATOS_GENERALES.SIBLING_PRODUCTS`
    """ # noqa: E501
})

# -------------------------------------------------------------------------
# Algoritmo de productos hermanos
# -------------------------------------------------------------------------
class GraficadorProductosHermanos:  # noqa: F811
    def __init__(self, df_ventas: pd.DataFrame, hermanos_confirmados_df: pd.DataFrame |
                 None = None, diferencia_peso_maxima: float = 0.25):
        self.df = df_ventas.copy()
        self.df['fecha'] = pd.to_datetime(self.df['fecha'])
        self._verificar_columnas()
        self._preparar_datos()
        self.resultados = []
        self.graficas_generadas = []
        self.hermanos_confirmados = []
        self.diferencia_peso_maxima = diferencia_peso_maxima

        # Cargar confirmados desde DataFrame si se proporciona
        if hermanos_confirmados_df is not None and not hermanos_confirmados_df.empty:
            self.hermanos_confirmados = hermanos_confirmados_df.to_dict('records')
            print(f'Subidos {len(self.hermanos_confirmados)} hermanos confirmados desde DataFrame')
        else:
            print('No se proporcionaron hermanos confirmados, inicializando vacío')

    def _verificar_columnas(self):
        columnas_requeridas = ['SKU', 'fecha', 'UNIDAD_DE_MEDIDA', 'EAN', 'NM',
                            'CAT_DSC', 'GRUPO_DSC', 'BRAND_DESC',
                            'CONTENIDO_BRUTO', 'cantidad_transacciones', 'tipo_envase']
        for col in columnas_requeridas:
            if col not in self.df.columns:
                raise ValueError(f"Columna requerida '{col}' no encontrada")  # noqa: EM102
        print('Todas las columnas están presentes')

    def _preparar_datos(self):
        print('Preparando datos...')

        self.df = self.df.dropna(subset=['SKU', 'fecha',
                                         'UNIDAD_DE_MEDIDA', 'cantidad_transacciones'])
        self.df['EAN'] = self.df['EAN'].fillna('').astype(str)
        self.df['BRAND_DESC'] = self.df['BRAND_DESC'].fillna('SIN_MARCA')
        self.df['cantidad_transacciones'] = self.df['cantidad_transacciones'].fillna(0)
        self.df['CONTENIDO_BRUTO'] = self.df['CONTENIDO_BRUTO'].fillna(0).astype(float)
        self.df['SKU'] = self.df['SKU'].astype(str)

        self._calcular_ventas_estandarizadas()
        print(f'Datos preparados: {len(self.df):,} registros')

    def _calcular_ventas_estandarizadas(self):
        #Calcula ventas estandarizadas según unidad_de_medida
        print('Calculando ventas estandarizadas...')

        # Convertir todo a unidades (ST)
        self.df['ventas_estandarizadas'] = np.where(
            self.df['UNIDAD_DE_MEDIDA'].str.upper().isin(['ST', 'UNIDAD', 'UNIT', 'U']),
            self.df['cantidad_transacciones'],
            np.where(
                self.df['UNIDAD_DE_MEDIDA'].str.upper().isin(['CS', 'CASE', 'CAJA']),
                self.df['cantidad_transacciones'] * 24, # Asumiendo 24 unidades por caja
                self.df['cantidad_transacciones'] # Por defecto, asumir unidades
            )
        )

        print('Ventas estandarizadas calculadas')

    def _obtener_categoria_desde_sku(self, sku):
        # Obtener categoría desde datos iniciales
        try:
            producto = self.df[self.df['SKU'] == sku]
            if not producto.empty:
                categoria = producto['CAT_DSC'].iloc[0]
                if pd.notna(categoria) and categoria != '':
                    return categoria
        except:  # noqa: E722, S110
            pass
        return 'SIN_CATEGORIA'

    def limpiar_nombre(self, nombre):
        # Convertir a minúsculas para uniformidad
        nombre = nombre.lower()

        # Quitar patrones como "130 gr", "500 ml", "1.5 lt"
        nombre = re.sub(r'\b\d+(\.\d+)?\s*(gr|g|ml|lt|l|kg)\b', '', nombre)

        # Quitar números aislados (por si quedan)
        nombre = re.sub(r'\d+', '', nombre)

        # Quitar espacios extra
        nombre = ' '.join(nombre.split())

        # Normalizar plurales simples (quitar 's' final)
        palabras = nombre.split()
        palabras_normalizadas = [p[:-1] if p.endswith('s') else p for p in palabras]#noqa:FURB188
        nombre = ' '.join(palabras_normalizadas)

        return nombre  # noqa: RET504

    def ordenar_palabras(self, nombre):
        # Separar en palabras
        palabras = nombre.split()
        # Ordenar alfabéticamente
        palabras.sort()
        # Unir nuevamente en una cadena
        return ' '.join(palabras)

    def encontrar_candidatos_hermanos(self, diferencia_peso_maxima = 0.25,
                                      emparejar_mismo_peso = False):
        # Encuentra candidatos con misma subcategoria y marca
        print('Buscando candidatos para hermanos')
        candidatos = []

        # Obtener SKUs de hermanos confirmados para excluirlos
        skus_confirmados = set()
        for par in self.hermanos_confirmados:
            skus_confirmados.add(par['sku_1'])
            skus_confirmados.add(par['sku_2'])

        # Agrupa por subcategoría y marca
        grouped = self.df.groupby(['CAT_DSC', 'BRAND_DESC'])

        for (subcat, marca), group in grouped:
            if len(group) > 1 and marca != 'SIN_MARCA':
                skus = group['SKU'].unique()
                nombres = group.set_index('SKU')['NM'].to_dict()
                pesos = group.set_index('SKU')['CONTENIDO_BRUTO'].to_dict()
                categorias = group.set_index('SKU')['CAT_DSC'].to_dict()
                envases = group.set_index('SKU')['tipo_envase'].to_dict()

                for i in range(len(skus)):
                    for j in range(i + 1, len(skus)):
                        sku_i, sku_j = skus[i], skus[j]

                        # Verificar diferencia de peso
                        peso_i = pesos.get(sku_i, 0)
                        peso_j = pesos.get(sku_j, 0)

                        # Si alguno no tiene peso, omitir
                        if peso_i == 0 or peso_j == 0:
                            continue
                        diferencia = abs(peso_i - peso_j) / min(peso_i, peso_j)

                        # Verificar igualdad de envase
                        envase_i = envases.get(sku_i, '')
                        envase_j = envases.get(sku_j, '')

                        if envase_i != envase_j:
                            continue

                        # Solo emparejar si son pesos diferentes
                        pesos_son_diferentes = peso_i != peso_j

                        # Limpiar y ordenar nombres
                        nombre_i_limpio = self.ordenar_palabras(
                            self.limpiar_nombre(nombres.get(sku_i, '')))
                        nombre_j_limpio = self.ordenar_palabras(
                            self.limpiar_nombre(nombres.get(sku_j, '')))

                        # Comparación exacta de nombres normalizados
                        if nombre_i_limpio != nombre_j_limpio:
                            continue  # descartar si no son iguales después de limpiar y ordenar

                        # Comparación exacta de nombres normalizados
                        if nombre_i_limpio != nombre_j_limpio:
                            continue  # descartar si no son iguales después de limpiar y ordenar

                        if (sku_i != sku_j and
                            sku_i not in skus_confirmados and
                            sku_j not in skus_confirmados and
                            diferencia <= diferencia_peso_maxima and
                            (pesos_son_diferentes or emparejar_mismo_peso)):

                            candidatos.append({
                                'sku_original' : skus[i],
                                'sku_nuevo' : skus[j],
                                'nombre_original' : nombres[skus[i]],
                                'nombre_nuevo' : nombres[skus[j]],
                                'category' : categorias[sku_i],
                                'sub_category' : subcat,
                                'brand_desc' : marca,
                                'peso_original': peso_i,
                                'peso_nuevo': peso_j,
                                'diferencia_peso_%': round(diferencia * 100, 1),
                                'mismo_peso' : peso_i == peso_j,
                                'tipo_envase' : envase_i == envase_j
                            })

        print(f'Encontrados {len(candidatos)} pares válidos')
        return pd.DataFrame(candidatos)

    def _agregar_confirmados(self, df_confirmados: pd.DataFrame):
        nuevos_confirmados = []

        for _, row in df_confirmados.iterrows():
            categoria = self._obtener_categoria_desde_sku(row['sku_1'])
            peso_1 = row.get('peso_1', 1)
            peso_2 = row.get('peso_2', 1)
            dif_peso = row.get('diferencia_peso_%', 0)

            par_confirmado = {
                'sku_1': row['sku_1'],
                'sku_2': row['sku_2'],
                'producto_1': row['producto_1'],
                'producto_2': row['producto_2'],
                'category': categoria,
                'sub_category': row['sub_category'],
                'brand_desc': row['brand_desc'],
                'peso_1': peso_1,
                'peso_2': peso_2,
                'diferencia_peso_%': dif_peso,
                'fecha_confirmacion': datetime.now().strftime('%Y-%m-%d'),  # noqa: DTZ005
                'tipo_envase': row.get('tipo_envase', None)
            }

            # Evitar duplicados
            if not any(p['sku_1'] == par_confirmado['sku_1'] and p['sku_2']
                       == par_confirmado['sku_2']
                    for p in self.hermanos_confirmados):
                nuevos_confirmados.append(par_confirmado)

        # Actualizar lista en memoria
        if nuevos_confirmados:
            self.hermanos_confirmados.extend(nuevos_confirmados)
            print(f'Añadidos {len(nuevos_confirmados)} nuevos hermanos confirmados')
        else:
            print('No se añadieron nuevos hermanos (todos eran duplicados)')

    def obtener_hermanos_confirmados(self):
    # Obtener la lista de hermanos confirmados
        if self.hermanos_confirmados:
            hermanos_df = pd.DataFrame(self.hermanos_confirmados)
            if 'category' in hermanos_df.columns:
                hermanos_df = hermanos_df.sort_values(['category', 'sub_category', 'brand_desc'])
            return hermanos_df
        else:  # noqa: RET505
            return pd.DataFrame()

    def exportar_hermanos_confirmados(self) -> pd.DataFrame:
        # Exporta DataFrame listo para BigQuery
        try:
            confirmados_df = pd.DataFrame(self.hermanos_confirmados)
            if confirmados_df.empty:
                print('No hay hermanos confirmados para exportar')
                return pd.DataFrame()

            # Agregar fecha de introducción del SKU nuevo
            if 'sku_2' in confirmados_df.columns:
                confirmados_df['fecha_introduccion'] = confirmados_df['sku_2'].apply(
                    lambda sku: self.df[self.df['SKU'] == str(sku)]['fecha'].min().date()
                    if not self.df[self.df['SKU'] == str(sku)].empty and pd.notna(self.df[
                        self.df['SKU'] == str(sku)]['fecha'].min())
                    else pd.NaT
                )

            # Convertir fecha de confirmación a solo fecha
            if 'fecha_confirmacion' in confirmados_df.columns:
                confirmados_df['fecha_confirmacion'] = pd.to_datetime(
                    confirmados_df['fecha_confirmacion'], errors='coerce'
                ).dt.date

            # Renombrar columnas para GCP
            confirmados_df.rename(columns={
                'sku_1': 'SKU_ANTIGUO',
                'sku_2': 'SKU_NUEVO',
                'producto_1': 'NOMBRE_ANTIGUO',
                'producto_2': 'NOMBRE_NUEVO',
                'category': 'CATEGORIA',
                'sub_category': 'SUB_CATEGORIA',
                'brand_desc': 'MARCA',
                'peso_1': 'PESO_ANTIGUO',
                'peso_2': 'PESO_NUEVO',
                'diferencia_peso_%': 'DIFF_PESO',
                'tipo_envase': 'TIPO_ENVASE',
                'fecha_confirmacion': 'FECHA_CONFIRMACION',
                'fecha_introduccion': 'FECHA_INTRODUCCION'
            }, inplace=True)  # noqa: PD002

            # Orden final
            column_order = [
                'SKU_ANTIGUO', 'SKU_NUEVO', 'NOMBRE_ANTIGUO', 'NOMBRE_NUEVO',
                'CATEGORIA', 'SUB_CATEGORIA',
                'PESO_ANTIGUO', 'PESO_NUEVO', 'DIFF_PESO',
                'FECHA_CONFIRMACION', 'FECHA_INTRODUCCION'
            ]
            existing_columns = [col for col in column_order if col in confirmados_df.columns]
            confirmados_df = confirmados_df.reindex(columns=existing_columns)

            print(f'Hermanos confirmados listos para BigQuery: {len(confirmados_df)} filas')
            return confirmados_df

        except Exception as e:  # noqa: BLE001
            print(f'Error preparando hermanos confirmados: {e}')
            return pd.DataFrame()

    def analizar_par_ventas(self, sku_original, sku_nuevo, ventana_dias=700):
        #Analiza el comportamiento de ventas de un par de productos
        print(f' Analizando: {sku_original} → {sku_nuevo}')

        # Verificar que ambos SKUs existen
        if sku_original not in self.df['SKU'].unique():
            print(f' SKU {sku_original} no encontrado')
            return None
        if sku_nuevo not in self.df['SKU'].unique():
            print(f' SKU {sku_nuevo} no encontrado')
            return None

        # Obtener EANs compartidos
        eans_original = self.df[self.df['SKU'] == sku_original]['EAN'].unique()
        eans_nuevo = self.df[self.df['SKU'] == sku_nuevo]['EAN'].unique()
        eans_compartidos = set(eans_original) & set(eans_nuevo)

        # Fecha de introducción del nuevo producto
        fecha_introduccion = self.df[self.df['SKU'] == sku_nuevo]['fecha'].min()

        # Período de análisis
        inicio_analisis = fecha_introduccion - timedelta(days=ventana_dias)
        fin_analisis = fecha_introduccion + timedelta(days=ventana_dias)

        # Preparar datos agrupados por fecha
        df_agrupado = (
            self.df.groupby(['fecha', 'SKU'])['ventas_estandarizadas']
            .sum()
            .reset_index()
        )

        # Filtrar y completar datos
        datos_original = df_agrupado[(df_agrupado['SKU'] == sku_original) &
            (df_agrupado['fecha'].between(inicio_analisis, fin_analisis))].copy()

        datos_nuevo = df_agrupado[(df_agrupado['SKU'] == sku_nuevo) &
            (df_agrupado['fecha'].between(inicio_analisis, fin_analisis))].copy()

        # Completar fechas faltantes
        fechas_completas = pd.date_range(inicio_analisis, fin_analisis, freq='D')

        datos_original_completo = pd.DataFrame({'fecha': fechas_completas})
        datos_original_completo = datos_original_completo.merge(
            datos_original[['fecha', 'ventas_estandarizadas']],
            on='fecha', how='left'
        ).fillna(0)

        datos_nuevo_completo = pd.DataFrame({'fecha': fechas_completas})
        datos_nuevo_completo = datos_nuevo_completo.merge(
            datos_nuevo[['fecha', 'ventas_estandarizadas']],
            on='fecha', how='left'
        ).fillna(0)

        # Calcular métricas
        ventas_antes = datos_original_completo[
            (datos_original_completo['fecha'] < fecha_introduccion) &
            (datos_original_completo['ventas_estandarizadas'] > 0)
        ]['ventas_estandarizadas'].mean()

        ventas_despues_original = datos_original_completo[
            (datos_original_completo['fecha'] >= fecha_introduccion) &
            (datos_original_completo['ventas_estandarizadas'] > 0)
        ]['ventas_estandarizadas'].mean()

        ventas_despues_nuevo = datos_nuevo_completo[
            (datos_nuevo_completo['fecha'] >= fecha_introduccion) &
            (datos_nuevo_completo['ventas_estandarizadas'] > 0)
        ]['ventas_estandarizadas'].mean()

        reduccion_porcentual = ((ventas_antes - ventas_despues_original) /
                                ventas_antes * 100) if ventas_antes > 0 else 0

        # Calcular correlación
        ventas_conjuntas = pd.merge(  # noqa: PD015
            datos_original_completo[datos_original_completo['fecha'] >= fecha_introduccion],
            datos_nuevo_completo[datos_nuevo_completo['fecha'] >= fecha_introduccion],
            on='fecha', suffixes=('_original', '_nuevo')
        )

        correlacion = ventas_conjuntas['ventas_estandarizadas_original'].corr(
            ventas_conjuntas['ventas_estandarizadas_nuevo']
        ) if len(ventas_conjuntas) > 1 else 0

        # Promedio histórico de ventas
        ventas_promedio = datos_original_completo['ventas_estandarizadas'].mean()
        umbral_porcentaje = ventas_promedio * 0.05  #5% promedio historico  # noqa: F841
        ventas_combinadas_despues = ventas_despues_original + ventas_despues_nuevo

        return {
        'fecha_introduccion': fecha_introduccion,
        'reduccion_porcentual': reduccion_porcentual,
        'correlacion': correlacion,
        'ventas_antes': ventas_antes,
        'ventas_despues_original': ventas_despues_original,
        'ventas_despues_nuevo': ventas_despues_nuevo,
        'eans_compartidos': list(eans_compartidos),
        'cantidad_eans_compartidos': len(eans_compartidos),
        'datos_original': datos_original_completo,
        'datos_nuevo': datos_nuevo_completo,
        'ventas_promedio': ventas_promedio,
        'ventas_combinadas_despues': ventas_combinadas_despues,
        'sku_original': sku_original
        }

    def determinar_si_son_hermanos(self, analisis_ventas):
        if not analisis_ventas:
            return False

        # Extraer métricas
        ventas_antes = analisis_ventas.get('ventas_antes', 0)
        ventas_despues_nuevo = analisis_ventas.get('ventas_despues_nuevo', 0)
        ventas_despues_original = analisis_ventas.get('ventas_despues_original', 0)
        correlacion = analisis_ventas.get('correlacion', 0)
        fecha_introduccion = analisis_ventas.get('fecha_introduccion')

        # Regla: verificar historial previo del producto antiguo (mínimo 60 días)  # noqa: W505
        fecha_inicio_original = self.df[self.df['SKU'] == analisis_ventas.get(
            'sku_original', '')]['fecha'].min()
        dias_historial = (fecha_introduccion - fecha_inicio_original).days if pd.notna(
            fecha_inicio_original) else 0
        if dias_historial < 60:  # menos de 2 meses de historial
            return False

        # Calcular diferencia porcentual entre promedios
        diferencia_porcentual = ((ventas_despues_nuevo - ventas_antes)
                                 / ventas_antes) * 100 if ventas_antes > 0 else 0

        # Nueva regla: reducción del 50% en ventas del producto antiguo
        if ventas_antes > 0:
            reduccion = ((ventas_antes - ventas_despues_original) / ventas_antes) * 100
            if reduccion < 50:
                return False

        # Regla 1: Si el nuevo vende más del doble que el antiguo NO hermanos  # noqa: W505
        if diferencia_porcentual >= 100:
            return False

        # Regla 2: Basado en correlación
        # Si correlación > 0.3 entonces hermanos (relación positiva)
        if correlacion > 0.3:
            return True

        # Regla 3: Basado en correlación
        # Si correlación < 0 entonces hermanos (indica reemplazo)
        if correlacion < 0:  # noqa: SIM103
            return True

        # Si correlación está entre -0.3 y 0.3 = no hay relación clara = no hermanos # noqa: W505
        return False

    def corregir_categorias_existentes(self):
        # Corregir categorias en hermanos confirmados existentes
        print('Corrigiendo categorias en hermanos confirmados existentes')

        cambios = 0
        for i, par in enumerate(self.hermanos_confirmados):
            if par.get('category', 'SIN_CATEGORIA') == 'SIN_CATEGORIA':
                categoria_corregida = self._obtener_categoria_desde_sku(par['sku_1'])
                if categoria_corregida != 'SIN_CATEGORIA':
                    self.hermanos_confirmados[i]['category'] = categoria_corregida
                    cambios += 1
                    print(f"Corregido: {par['sku_1']} -> {categoria_corregida}")

        if cambios > 0:
            self._guardar_confirmados()
            print(f'Categorias corregidas: {cambios} hermanos confirmados')
        else:
            print('Todas las categorias correctas')
        return cambios

    def ejecutar_modo_automatico(self):
        print('EJECUTANDO MODO AUTOMÁTICO')
        print('=' * 60)
        print(f'Hermanos confirmados actuales: {len(self.hermanos_confirmados)}')

        candidatos_df = self.encontrar_candidatos_hermanos()
        if candidatos_df.empty:
            print('No se encontraron candidatos nuevos')
            return

        confirmados = []
        for _, row in candidatos_df.iterrows():
            sku1 = row['sku_original']
            sku2 = row['sku_nuevo']
            analisis = self.analizar_par_ventas(sku1, sku2)
            if self.determinar_si_son_hermanos(analisis):
                confirmados.append({
                    'sku_1': sku1,
                    'sku_2': sku2,
                    'producto_1': row['nombre_original'],
                    'producto_2': row['nombre_nuevo'],
                    'category': row['category'],
                    'sub_category': row['sub_category'],
                    'brand_desc': row['brand_desc'],
                    'peso_1': row['peso_original'],
                    'peso_2': row['peso_nuevo'],
                    'diferencia_peso_%': row['diferencia_peso_%'],
                    'fecha_confirmacion': datetime.now().strftime('%Y-%m-%d'),  # noqa: DTZ005
                    'tipo_envase': row['tipo_envase']
                })

        if confirmados:
                df_confirmados = pd.DataFrame(confirmados)
                self._agregar_confirmados(df_confirmados)
                print(f'Confirmados automáticamente: {len(confirmados)} pares')

                # Exportar Excel final con formato completo
                self.exportar_hermanos_confirmados()
        else:
            print('No se confirmaron hermanos automáticamente')

# -------------------------------------------------------------------------
# Main function
# -------------------------------------------------------------------------
def main() -> None:
    # Parse input variables
    args = vars(parser.parse_args())
    user: str = args['project_name']
    gcp_project: str = args['project_id']
    execution_date: str = args['execution_date']
    start_date = '2023-01-01'  # noqa: F841
    end_date = pendulum.parse(execution_date).format('YYYY-MM-DD')

    # BigQuery client
    gbq_client = Client()
    logging.info(f'Execution date: {execution_date}')

    # 1. Leer datos desde BigQuery
    logging.info('Reading ventas data from BigQuery...')

    query1 = WORKFLOW_QUERIES['nuevos_productos'].substitute(
            gcp_project=gcp_project,
            start_date=start_date,
            end_date=end_date
        )

    logging.info(query1)

    ventas_df = gbq_extended.readBigQuery(
        query=query1,
        user=user,
        gbq_client=gbq_client,
    )

    # 2. Leer hermanos confirmados existentes
    confirmados_df = gbq_extended.readBigQuery(
        query=WORKFLOW_QUERIES['extraer_confirmados'].substitute(gcp_project=gcp_project),
        user=user,
        gbq_client=gbq_client,
    )

    # 3. Ejecutar algoritmo con confirmados previos
    graficador = GraficadorProductosHermanos(ventas_df, hermanos_confirmados_df=confirmados_df)
    graficador.ejecutar_modo_automatico()

    # 4. Exportar DataFrame final
    logging.info('Preparando DataFrame final para BigQuery...')
    df_confirmados_final = graficador.exportar_hermanos_confirmados()

    if df_confirmados_final.empty:
        logging.warning('No se generaron hermanos confirmados')
        return

    # 4. Subir resultado a BigQuery
    logging.info('Subiendo tabla SIBLING_PRODUCTS a BigQuery...')
    gbq_extended.uploadFrame(
        df=df_confirmados_final,
        table_ddl_json_path=os.path.join('gbq_objects', 'sibling_products.json'),
        project=gcp_project,
        if_exists='replace',
        gbq_client=gbq_client,
    )
    logging.info('Proceso completado con éxito!')

if __name__ == '__main__':
    main()
