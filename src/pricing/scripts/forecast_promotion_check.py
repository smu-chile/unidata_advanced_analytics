# Default
from __future__ import annotations

import io
import os
import re
import sys
import time
import logging
import argparse
import warnings
import posixpath
import threading
from logging import config

# Pip
import numpy as np
import pandas as pd  # type: ignore[import]
import statsmodels.api as sm
from google.cloud.bigquery import Client


warnings.filterwarnings(
    'ignore',
    message='Your application has authenticated using end user credentials*',
    category=UserWarning
)

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s | %(levelname)s | %(message)s')

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
import common.office365_extended.sharepoint as sp  # noqa: E402
from common.constants import LOGGING_CONFIG  # noqa: E402
from common.databases.queries import QueryDict  # noqa: E402
from common.gcp_extended.bigquery import (  # noqa: E402
    uploadFrame,  # noqa: F401
    readBigQuery,
    deleteFromTable,  # noqa: F401
    createTableAsSelect,  # noqa: F401
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
# Globals and inputs
# -------------------------------------------------------------------------

#For 1.2
PATRON_INPUT = re.compile(
    r'^\d{4}_\d{2}_\d{2}_v\d+_input_proyeccion\.xlsx$',
    re.IGNORECASE
)

PATRON_OUTPUT = re.compile(
    r'^\d{4}_\d{2}_\d{2}_v\d+_resultado_proyeccion\.xlsx$',
    re.IGNORECASE
)

MESES = [
    'enero', 'febrero', 'marzo', 'abril', 'mayo', 'junio',
    'julio', 'agosto', 'septiembre', 'octubre', 'noviembre', 'diciembre'
]

DIAS_SEMANA = {
    'lunes': 0,
    'martes': 1,
    'miercoles': 2,
    'jueves': 3,
    'viernes': 4,
    'sabado': 5,
    'domingo': 6
}

DTYPES_DICT = {
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
    'multiplicador_x05': 'int32',
    'apo': 'int32',
    'proporcion_categoria': 'float64',
    'variacion_porcentual_subcategoria': 'float64',
    'variacion_top1_sustituto': 'float64',
    'variacion_top3_sustitutos': 'float64'
}

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

#1.1
def get_sharepoint_folders(
    sp_cred: dict,
    file_site: str
) -> tuple[sp.SharePointFolder, sp.SharePointFolder, str]:
    '''P1.F1: Obtiene SharePointFolder para carpetas input y output

    Inputs:
    sp_cred [Dict] Credenciales SharePoint access
    file_site [str] Dir. donde se encuentra Inputs-Outputs en SharePoint

    Return:
    (tupla): SharePointFolders para carpetas "Inputs" y "Outputs" y ruta
    output_dir respectivamente.

    Nota: la ruta se usa psoteriormente para la subida de resultados
    a Sharepoint.
       '''

    inputs_dir = posixpath.join(file_site, 'Inputs')
    outputs_dir = posixpath.join(file_site, 'Outputs')

    return (
        sp.SharePointFolder(
            **sp_cred,
            server_relative_folder=inputs_dir
        ),
        sp.SharePointFolder(
            **sp_cred,
            server_relative_folder=outputs_dir
        ), outputs_dir)

#1.2
def filtrar_excels_validos(
    archivos: list[str],
    patron: re.Pattern
) -> list[str]:
    """P1.F2: Filtra y ordena los archivos entregados por patrón

    Inputs:
    archivos [list]: Contiene el listado de archivos a ordenar
    patron [re.Pattern] Patrón específico para búsqueda de archivos

    Outputs:
    [list] Listado de archivos que cumplen con patrón dado
     """
    return sorted(
        n for n in archivos
        if (
            n.lower().endswith('.xlsx')
            and not n.startswith('~$')
            and patron.match(n)
        )
    )

#1.3.1
def clave_base_input(nombre_input: str) -> str:
    """P1.F3 Aux 1: Elimina texto específico del string dado.

    Inputs:
    nombre_input [str]: Nombre del archivo dado

    Outputs:
    [str] Retorna el archivo limpio

    Ejemplo:
    2025_11_05_v5_input_proyeccion.xlsx -> 2025_11_05_v5
    """
    return nombre_input.replace('_input_proyeccion.xlsx', '')

#1.3.2
def nombre_output_desde_clave(clave: str) -> str:
    """P1.F3 Aux 2: Agrega string específico al nombre dado

    Inputs:
    clave [str]: Extracto string numérico del archivo en input
    Ejemplo:
      clave: "2025_11_05_v5"

    Outputs:
    [str] String completo para búsqueda en Outputs

    Ejemplo:
    2025_11_05_v5 -> 2025_11_05_v5_resultado_proyeccion.xlsx

    """
    return f'{clave}_resultado_proyeccion.xlsx'

#1.4
def encontrar_inputs_pendientes(
    inputs_validos: list[str],
    outputs_validos: set[str],
) -> list[str]:
    """P1.F4: Retorna lista de archivos cuya "clave"
    no se encuentra en ambas carpetas Inputs y Outputs

    Inputs:
    inputs_validos [list] Lista de archivos validos para Inputs
    outputs_validos [set] Set de archivos validos para Outputs

    Outputs:
    (list) Lista de archivos cuya clave no aparece en el output

    Nota: La gestión de pendiente = [] se incluye afuera de la función
    """

    pendientes = []

    for nombre_input in inputs_validos:
        clave = clave_base_input(nombre_input)
        nombre_output = nombre_output_desde_clave(clave)

        if nombre_output not in outputs_validos:
            pendientes.append(nombre_input)

    return pendientes

#1.5
def cargar_input_sharepoint(
    sp_cred: dict,
    inputs_dir: str,
    nombre_input: str
) -> pd.DataFrame:
    """P1.F5 Cargado de archivo .xlsx desde el SharePoint

    Inputs:
    sp_cred [dict] Credenciales SharePoint access
    inputs_dir [str] Path del archivo dado
    nombre_input [str] Nombre del archivo a cargar

    Outputs:
    [pd.DataFrame] Dataframe con la data del .xlsx importado

    """
    input_file_path = posixpath.join(inputs_dir, nombre_input)

    logging.info('Entrando a: cargar_input_sharepoint...')
    archivo = sp.SharePointFile(
        **sp_cred,
        server_relative_path=input_file_path
    )

    return archivo.toFrame()

#1.6
def validar_input_promociones(
    df_inp: pd.DataFrame
) -> list[str]:
    """P1.F6 Valida columnas s y cantidad de promos > 0.

    Inputs:
    df_inp [pd.DataFrame] Dataframe con la data del .xlsx importado

    Outputs:
    [list] Lista de promociones marcadas para generar proyección.
    """

    cols_requeridas = {'n_promocion', 'generar_proyeccion'}
    faltantes = cols_requeridas - set(df_inp.columns)

    text_val_1 = f'Faltan columnas en el Excel: {sorted(faltantes)}'
    text_val_2 = (
        'No se encontraron promociones con generar_proyeccion = Si.'
    )

    if faltantes:
        raise ValueError(text_val_1)

    promociones = (
        df_inp.loc[
            df_inp['generar_proyeccion']
            .astype(str)
            .str.strip()
            .str.lower()
            .eq('si'),
            'n_promocion',
        ]
        .dropna()
        .astype(str)
        .unique()
        .tolist()
    )

    if not promociones:
        raise ValueError(text_val_2)

    return promociones

#Main 1
def preparar_input_promociones(
    sp_cred,
    file_site: str,
    patron_input: str,
    patron_output: str,
    abortar_si_no_pendientes: bool = True
) -> tuple[pd.DataFrame | None, list[str], str | None, str | None]:

    """Orquesta el flujo completo de carga y validación de inputs de
    promocionesdesde SharePoint.

    Flujo:
    1. Obtiene carpetas Inputs / Outputs
    2. Lista archivos disponibles
    3. Filtra archivos válidos según patrón
    4. Identifica inputs pendientes de procesamiento
    5. Carga el último input válido
    6. Valida y extrae la lista de promociones contenidas

    Parameters
    ----------
    sp_cred : object
        Credenciales de SharePoint.
    file_site : str
        Ruta base del sitio en SharePoint.
    patron_input : str
        Patrón regex para identificar archivos de input válidos.
    patron_output : str
        Patrón regex para identificar archivos de output válidos.
    abortar_si_no_pendientes : bool, default True
        Si True, lanza excepción cuando no hay inputs pendientes.
        Si False, retorna (None, []).

    Returns
    -------
    tuple
        df_input : pd.DataFrame
            DataFrame del input cargado desde SharePoint.
        lista_promos : list
            Listado de promociones validadas desde el input.
        outputs_dir : str
            Ruta de la carpeta de outputs en SharePoint, para uso posterior
        nombre_output : str
            Nombre del archivo de output esperado, para uso posterior.


    Raises
    ------
    ValueError
        Si no hay inputs pendientes y abortar_si_no_pendientes=True.
    """

    # -----------------------------
    # P1.F1 Obtener carpetas SP
    # -----------------------------
    sp_inputs, sp_outputs, outputs_dir = get_sharepoint_folders(
        sp_cred,
        file_site
    )

    inputs_raw = sp_inputs.fileList()
    outputs_raw = sp_outputs.fileList()

    # -----------------------------
    # P1.F2 Filtrar archivos válidos
    # -----------------------------
    inputs_validos = filtrar_excels_validos(
        inputs_raw,
        patron_input
    )

    outputs_validos = set(
        filtrar_excels_validos(
            outputs_raw,
            patron_output
        )
    )

    # -----------------------------
    # P1.F3 Identificar pendientes
    # -----------------------------
    pendientes = encontrar_inputs_pendientes(
        inputs_validos,
        outputs_validos
    )

    if not pendientes:
        msg = 'No hay inputs pendientes por procesar.'
        logging.info(msg)
        logging.info(file_site)

        if abortar_si_no_pendientes:
            raise ValueError(msg)

        return None, [], None, None

    # Política actual: tomar el último input válido
    nombre_input = pendientes[-1]
    clave = clave_base_input(nombre_input)
    nombre_output = nombre_output_desde_clave(clave)

    logging.info(f'Procesando input: {nombre_input}')
    logging.info(f'Output esperado: {nombre_output}')

    # -----------------------------
    # P1.F4 Cargar input
    # -----------------------------
    df_input = cargar_input_sharepoint(
        sp_cred,
        posixpath.join(file_site, 'Inputs'),
        nombre_input
    )
    # -----------------------------
    # P1.F5 Validar promociones
    # -----------------------------
    lista_promos = validar_input_promociones(df_input)

    return df_input, lista_promos,outputs_dir, nombre_output


#2.1
def ejecutar_query(
    query: str,
    store_banner: str,
    categorias: str,
    path_table: str,
    usuario: str,
    gbq_client: Client,
    log_interval: int = 30
) -> pd.DataFrame:
    """P2.F1: Ejecuta la query recibida y devuelve el dataframe respectivo.

    Inputs:
    query [str]: Nombre de la query. Se espera que los valores sean
    'query_data_procesada' o 'query_promos_forecasting'.
    store_banner [str]: Se espera 'Unimarc'.
    categorias [str]: String con las promociones.
    path_table [str]: Ruta a la tabla
    'TMP_REGRESSION_PROCESSED_DATA_FORECAST'
    usuario [str]: Se trabaja con 'pricing'
    gbq_client [Client]: Cliente de BigQuery apuntando al proyecto GCP.

    Outputs:
    (pd.DataFrame) DataFrame con el resultado de la query

    """

    query = SQL_QUERIES[query].substitute(
        store_banner=store_banner,
        path_table=path_table,
        categorias=categorias
    )

    print('QUERY: ', query)

    logging.info('Ejecutando query en BigQuery...')
    ejecutando = True

    def log_espera():
        while ejecutando:
            logging.info('⏳ Query en ejecución...')
            time.sleep(log_interval)

    hilo_logger = threading.Thread(
        target=log_espera,
        daemon=True
    )
    hilo_logger.start()

    try:
        df_read = readBigQuery(
            query=query,
            user=usuario,
            gbq_client=gbq_client
        )
    finally:
        ejecutando = False  # detiene el log periódico

    logging.info('✅ Query finalizada correctamente')

    print('[2.1] Ejecutar query df shape: ', df_read.shape)
    return df_read

#2.2
def limpiar_datos_base(df_clean: pd.DataFrame) -> pd.DataFrame:
    """P2.F2: Función de tratamiento de los datos.
    (1) Formatea nombres y dtypes
    (2) Crea la columna 'product_description_ean'

    Inputs:
    df [pd.DataFrame]: DataFrame con el resultado de la query

    Outputs:
    (pd.DataFrame): DataFrame tratado,
    """
    df_clean = df_clean.copy()

    df_clean.columns = df_clean.columns.str.lower()

    # EAN y descripción
    df_clean['ean'] = df_clean['ean'].astype(str)
    df_clean['product_description_ean'] = (
        df_clean['product_description'] + ' - ' + df_clean['ean']
    )

    df_clean['multiplicador_x05'] = df_clean['multiplicador_x05'].astype(int)

    logging.info('Limpieza base aplicada')
    return df_clean

#2.3
def agregar_features_temporales(df_temp: pd.DataFrame) -> pd.DataFrame:
    """P2.F3: Función que incluye el tratamiento e inclusión
    de variables temporables a nivel de día, mes y año.

    Inputs:
    df_temp [pd.DataFrame]: DataFrame con los resultados de la query.

    Outputs:
    (pd.DataFrame) DataFrame con la inclusión de los nuevos features
    temporales

    Nota 1: 'l_m_w' refiere a Lun, Mar y Miércoles.
    Nota 2: Esta función incorpora 25 features temporales:
    - 24 dummies: 9 x días, 12 x meses y 3 x año
    - 1 variable [p_year]

    """
    df_temp = df_temp.copy()

    df_temp['p_date'] = pd.to_datetime(df_temp['p_date'])
    dow = df_temp['p_date'].dt.dayofweek #Extra el día de la semana codigicado 0 al 6

    # Agrupaciones
    df_temp['fds'] = dow.isin([4, 5, 6]).astype(int) #1 para fds, 0 resto
    df_temp['l_m_w'] = dow.isin([0, 1, 2]).astype(int) #1 para l_m_w, 0 resto

    # Dummies por día
    for nombre, valor in DIAS_SEMANA.items():
        df_temp[nombre] = (dow == valor).astype(int)

    logging.info('Dummies de días creados')

    # Dummies por mes
    mes_num = df_temp['p_month'].astype(str).str[4:].astype(int) #Extrae num de mes
    for i, nombre in enumerate(MESES, start=1):
        df_temp[nombre] = (mes_num == i).astype(int)

    logging.info('Dummies de meses creados')

    # Dummies por año
    df_temp['p_year'] = df_temp['p_date'].dt.year.astype(str)
    dummies_year = pd.get_dummies(df_temp['p_year'], prefix='', prefix_sep='').astype(int)
    df_temp = pd.concat([df_temp, dummies_year], axis=1)

    logging.info('Dummies de años creados')
    return df_temp

#2.4
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

    Nota: Esta función incorpora 29 variables dummies.
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

#2.5
def reordenar_columnas(df_cols: pd.DataFrame) -> pd.DataFrame:
    """P2.F4: Función auxiliar encargada de reordenadar las columnas
    existentes según orden dado.

    Inputs:
    df [DataFrae]: DataFrame a ordenar. Se recomienda que venga ya tratado
    y con los features temporables incluídos.

    Outputs:
    (pd.DataFrame) DataFrame con las columnas reordenadas.

    Nota: Si se ejecuta en el orden correcto, esta función excluye
    8 variables
    """

    columnas_deseadas = [
        'store_banner','product_description_ean',
        'category_description','sub_category_description',
        'material','product_description','ean',
        'sales_uom','sales_unit',
        'p_date','p_week','p_month',
        'ventas_totales_producto','cantidad_total','precio_promedio',
        '2023','2024','2025','2026',
        'l_m_w','lunes','martes','miercoles','jueves',
        'viernes','sabado','domingo',
        'invierno','otono','primavera','verano',
        *MESES,
        'primer_dia_mes','ultimo_dia_mes',
        'jueves_santo','viernes_santo','30_abril',
        '20_mayo','21_mayo','19_junio','20_junio',
        '15_julio','16_julio','14_agosto','15_agosto',
        '14_septiembre','15_septiembre','16_septiembre','17_septiembre',
        'pre_halloween','halloween','pre_navidad','navidad',
        'pre_ano_nuevo','ano_nuevo','feriado','pre_feriado',
        'multiplicador_x05','apo','proporcion_categoria',
        'ean_sustituto_1','ean_sustituto_2','ean_sustituto_3',
        'ean_sustituto_4','ean_sustituto_5',
        'variacion_porcentual_subcategoria',
        'variacion_top1_sustituto','variacion_top3_sustitutos'
    ]

    columnas_existentes = [c for c in columnas_deseadas if c in df_cols.columns]
    df_cols = df_cols[columnas_existentes]

    logging.info('Columnas reordenadas')
    return df_cols

#2.6
def ordenar_por_ventas(df_ord: pd.DataFrame) -> pd.DataFrame:
    """P2.F5: Ordena el DataFrame colocando primero los EAN con mayor venta
    total histórica. No se agregan ni eliminan filas; solo se reorganiza
    el orden.

    inputs:
    df_ord [pd.DataFrame]: Dataframe ya tratado.

    Outputs:
    (pd.DataFrame) DataFrame reorganizado según venta total
    histórica por EAN.

    """
    df_ord = df_ord.copy()

    suma_ventas = df_ord.groupby('ean')['ventas_totales_producto'].sum()
    df_ord['__suma_ventas'] = df_ord['ean'].map(suma_ventas)

    df_ord = df_ord.sort_values('__suma_ventas', ascending=False)
    df_ord = df_ord.drop(columns='__suma_ventas')

    logging.info('Ordenado por ventas')
    return df_ord

#2.7
def tipar_columnas(df_tip: pd.DataFrame) -> pd.DataFrame:
    """P2.F6: Función auxiliar encargada de aisgnar explícitamente el tipo
    a cada columna de interés.

    Inputs:
    df_tip [pd.DataFrame]: DataFrame ya tratado en su última fase.

    Outputs:
    (pd.DataFrame) Dataframe con los dtypes deseados.
    """
    df_tip = df_tip.copy()

    for col, dtype in DTYPES_DICT.items():
        if col in df_tip.columns:
            df_tip[col] = df_tip[col].astype(dtype)

    logging.info('Tipos asignados correctamente')
    return df_tip

# Main Parte 2
def generarDataFrame(
    query: str,
    store_banner: str,
    categorias: str,
    gbq_client: Client,
    path_table: str,
    usuario: str = 'pricing'
) -> pd.DataFrame:
    """ Parte 2: Genera el DataFrame final a partir de BigQuery, aplicando
    limpieza, creación de variables temporales, feriados, orden lógico y
    tipado.

    Inputs:
    store_banner [str]: Se espera 'Unimarc'.
    categorias [str]: String con las promociones.
    gbq_client [Client]: Cliente de BigQuery apuntando al proyecto GCP.
    path_table [str]: Ruta a la tabla
    'TMP_REGRESSION_PROCESSED_DATA_FORECAST'
    usuario [str]: Se espera 'pricing'

    Outputs:
    (pd.DataFrame) Retorna el dataframe final (shape: n,74)
    """



    df_p2 = ejecutar_query(query,store_banner, categorias, path_table, usuario, gbq_client)
    df_p2 = limpiar_datos_base(df_p2)
    df_p2 = agregar_features_temporales(df_p2)
    df_p2 = agregarFeriados(df_p2)
    df_p2 = reordenar_columnas(df_p2)
    df_p2 = ordenar_por_ventas(df_p2)
    return tipar_columnas(df_p2)

#3.1
def cargar_promos_forecasting(
    string_promos: str,
    gbq_client: Client
) -> pd.DataFrame:
    """P3.F1: Función encargado de ejecutar la query recibida
    y de entregar el DataFrame correspondiente.

    Inputs:
    string_promos [str]: String con las promociones.
    gbq_client [Client]: Cliente de BigQuery apuntando al proyecto GCP.

    Outputs:
    (pd.DataFrame):  DataFrame resultante

    """

    logging.info('[3.1] Procesando 2da query')
    query_sql = SQL_QUERIES['query_promos_forecasting'].substitute(
        string_promos=string_promos
    )

    return readBigQuery(
        query=query_sql,
        user='sales_forecast',
        gbq_client=gbq_client
    )

#3.2
def preparar_promos_base(df_prep: pd.DataFrame) -> pd.DataFrame:
    """ P3.F2: Limpia datos de promociones y los expande a granularidad
    diaria.

    Inputs:
    df_prep [pd.DataFrame]: DataFrame resultado de la query

    Outputs:
    (pd.DataFrame) DataFrame limpio y tratado.

    Nota: el df recibido tiene a la fecha 23 columnas. Agrega 1 más y
    retorna 13 columnas.
    """

    logging.info('[3.2] Preparando base de promociones...')

    df_prep = df_prep.copy()

    # Cambio de tipos
    df_prep['precio_promocional'] = df_prep['precio_promocional'].astype(int)
    df_prep['precio_modal'] = df_prep['precio_modal'].astype(int)


    df_prep['fecha_inicio_de_promocion'] = pd.to_datetime(df_prep['fecha_inicio_de_promocion'])
    df_prep['fecha_fin_de_promocion'] = pd.to_datetime(df_prep['fecha_fin_de_promocion'])

    # Clave producto
    df_prep['clave_material'] = (
        df_prep['material'].astype(str) + '_' + df_prep['un_medida_venta']
    )

    # Expansión diaria (dataset todavía contenido)
    df_prep['p_date'] = df_prep.apply(
        lambda r: pd.date_range(
            start=r['fecha_inicio_de_promocion'],
            end=r['fecha_fin_de_promocion']
        ),
        axis=1
    )

    df_prep = df_prep.explode('p_date')

    return df_prep[[
        'n_promocion', 'nombre_promocion', 'desc_categoria',
        'material', 'desc_material', 'un_medida_venta',
        'clave_material', 'desc_promocion',
        'descripcion_evento_promocional',
        'p_date', 'precio_promocional', 'precio_modal', 'ean'
    ]].copy()

#3.3
def calcular_precios_minimos(df_base: pd.DataFrame) -> pd.DataFrame:
    """P3.F3: Calcula el precio promocional mínimo por clave_material y día

    Inputs:
    df_base [pd.DataFrame]: DataFrame de promociones limpio y tratado

    Outputs:
    (pd.DataFrame) Inclusión del precio min. promocional por clave_material
    y día para el df de promociones.

    Nota: no agrega filas, solo 2 columnas.
    """

    logging.info('[3.3] Tratado de precios precios mínimos...')
    def _minimo(df_aux: pd.DataFrame) -> pd.Series:
        """P3.F3 Aux 1: Función auxiliar pensada para aplicar por

        Inputs:
        df_aux (pd.DataFrame):Contiene la información de todas las
        promociones activas para un mismo producto en un mismo día.

        Outputs:
        (pd.Series) Retorna 'precio_promocional_minimo' y 'desc_promocion_
        minimo'

        Nota: para 'COMBINACION NX$'. Si hay más de una promo, se promedia
        el precio de las más baratas. Si no, se promedia precio modal y
        promocional. Si no es combinación, toma precio directo.
        """
        df_aux = df_aux.sort_values('precio_promocional')
        primera = df_aux.iloc[0]

        if primera['desc_promocion'] == 'COMBINACION NX$':
            if len(df_aux) > 1:
                segunda = df_aux.iloc[1]
                precio_min = (
                    primera['precio_promocional'] +
                    segunda['precio_promocional']
                ) / 2
            else:
                precio_min = (
                    primera['precio_promocional'] +
                    primera['precio_modal']
                ) / 2
        else:
            precio_min = primera['precio_promocional']

        return pd.Series({
            'precio_promocional_minimo': precio_min,
            'desc_promocion_minimo': primera['desc_promocion']
        })


    df_minimos = (
        df_base
        .groupby(['clave_material', 'p_date'], as_index=False)
        .apply(_minimo)
    )

    return df_base.merge(
        df_minimos,
        on=['clave_material', 'p_date'],
        how='left'
    )

#3.4
def filtrar_promos_proyectables(
    df_promos: pd.DataFrame,
    df_promos_importado: pd.DataFrame
) -> pd.DataFrame:
    """P3.F4: Función filtra solo promociones marcadas para proyección.

    Inputs:
    df_promos [pd.DataFrame] DataFrame con las promociones y minimos
    df_promos_importado [pd.DataFrame] Dataframe con la data del .xlsx
    importado

    Outpus:
    (pd.DataFrame) df_promos filtrado cuyas promociones tienen como valor
    'generar_proyección' = 'sí' en df_promos_importado.
    (list) listado de promos válidas para proyección.

    """
    logging.info('[3.4] Filtrado de promos proyectables...')
    promos_ok = df_promos_importado.loc[
        df_promos_importado['generar_proyeccion'].str.lower() == 'si',
        'n_promocion'
    ].unique()

    return df_promos[df_promos['n_promocion'].isin(promos_ok)],promos_ok

#3.5
def construir_universo_filtrado(
    df_final: pd.DataFrame,
    df_promos: pd.DataFrame
) -> pd.DataFrame:
    """P3.F5: Filtra df_final a solo las categorías presentes en
    promociones.

    Inputs:
    df_final [pd.DataFrame]: Dataframe resultante de  generarDataFrame().
    Contiene la metadata a nivel de producto.
    df_promos [pd.DataFrame] Dataframe de promociones ya filtrado.

    Outputs:
    (pd.Dataframe) df_final filtrado por categorías que se encuentran en
    df_promos.

    """
    logging.info('[3.5] Filtrado de categorías mediante desc_categoria')

    categorias = df_promos['desc_categoria'].unique()

    return df_final.loc[
        df_final['category_description'].isin(categorias)
    ].copy()

# Main Parte 3
def generar_dataset_promos_proyectables(
    string_promos: str,
    df_promos_importado: pd.DataFrame,
    df_final: pd.DataFrame,
    gbq_client: Client
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Parte 3:Genera el dataset diario de promociones proyectables y el
    universo de productos histórico filtrado por dichas promociones.

    Outputs:
    df_base [pd.DataFrame] Contiene el dataframe asociado a las promociones
    tratado y filtrado.
    df_universo [pd.DataFrame] Dataframe de productos filtrado por
    categorías coincidentes con df_base.
    promos_validas [list] Listado de promociones válidas para proyección.
    """

    df_promos = cargar_promos_forecasting(string_promos, gbq_client)
    df_base = preparar_promos_base(df_promos)
    df_base = calcular_precios_minimos(df_base)
    df_base, promos_validas = filtrar_promos_proyectables(df_base, df_promos_importado)

    df_universo = construir_universo_filtrado(df_final, df_base)

    return df_base, df_universo, promos_validas


#4.1
def extraer_contexto_material(df_ean: pd.DataFrame) -> dict:

    """P4.F1: Función encargada de extraer la 'metadata' del
    dataset asociado al material.

    Inputs:
    df_ean: DataFrame df_resultado con información de la promo-material

    Outputs:
    (dict) Diccionario con el contexto del material a trabajar.

    Nota:
    - material: número del material (único por promo - material)
    - sales_uom: unidad de medida de la venta
    - desc_material: descripción del material
    - categoría: categoría del material
    - fecha_inicio: fecha inicio de la promoción
    - fecha_fin: fecha término de promoción
    - fecha_inicio_mes: inicio de mes de fecha_inicio
    - ean_promocion: ean de la promo con gestión de nans

    """

    return {
        'material': int(df_ean['material'].iloc[0]),
        'sales_uom': str(df_ean['un_medida_venta'].iloc[0]),
        'desc_material': str(df_ean['desc_material'].iloc[0]),
        'categoria': str(df_ean['desc_categoria'].iloc[0]),
        'fecha_inicio': df_ean['p_date'].min().strftime('%Y-%m-%d'),
        'fecha_fin': df_ean['p_date'].max().strftime('%Y-%m-%d'),
        'fecha_inicio_mes': (
            df_ean['p_date'].min().replace(day=1).strftime('%Y-%m-%d')
        ),
        'fecha_limite': (pd.to_datetime(df_ean['p_date'].max()) -
                         pd.Timedelta(days=1)).strftime('%Y-%m-%d'),
        'ean_promocion': (
            df_ean['ean'].dropna().astype(str).iloc[0]
            if 'ean' in df_ean.columns and not df_ean['ean'].dropna().empty
            else ''
        )
    }

#4.2
def validar_material(df_hist_prod: pd.DataFrame,
                     fecha_inicio_mes: str) -> tuple[bool, str]:

    """P4.F2 Aux 1: Función auxiliar encargada de implementar las
    verificaciones antes de entrenar el modelo. Las verificaciones son:
    - ¿tengo historial?
    - De ser así, ¿es suficiente?
    - ¿Su comportamiento es estable?

    Inputs:
    df_hist_prod (pd.DataFrame): df_universo filtrado por material y
    sales_uom.
    fecha_inicio_mes (str): Fecha inicio de mes de la promoción

    Outputs:
    (bool,str): True si pasó todas las pruebas, y '' para representar
    este estado dentro del loggeo de las métricas.
    """
    #¿Existe historial para este material?
    if df_hist_prod.empty:
        return False, 'Sin ventas'

    # ¿tengo suficientes observaciones útiles para entrenar un modelo?
    if not verificacionFactibilidadModelo(df_hist_prod):
        return False, 'Sin historial suficiente'

    #¿este producto tiene un patrón mensual estable antes de la promo?
    if verificacionVentasVolatiles(df_hist_prod, fecha_inicio_mes):
        return False, 'Ventas volátiles'

    return True, 'Válido'

#4.2.1
def verificacionVentasVolatiles(
    df_ean: pd.DataFrame,
    fecha_inicio_mes: str
) -> bool:
    """P4.F2 Aux 1: Evalúa si las ventas históricas de un material
    presentan alta volatilidad mensual antes del inicio de una promoción.

    La volatilidad se mide usando el coeficiente de variación (CV)
    de las ventas mensuales agregadas, considerando los últimos
    8 meses previos a la promoción.

    Si el CV es mayor a 60%, el material se considera volátil
    y no apto para proyección con modelo econométrico.

    Parameters
    ----------
    df_ean : pd.DataFrame
        DataFrame histórico del material/EAN, con columnas
        `p_date` y `ventas_totales_producto`.

    fecha_inicio_mes : str
        Fecha (YYYY-MM-DD) correspondiente al primer día del
        mes de inicio de la promoción.

    Returns
    -------
    bool
        True si las ventas son volátiles (CV > 60%),
        False si el patrón de ventas es estable.
    """
    fecha_inicio_mes = pd.to_datetime(fecha_inicio_mes)

    df_aux = df_ean.copy()
    df_aux['p_date'] = pd.to_datetime(df_aux['p_date'])

    # Usar solo historial previo a la promo
    df_aux = df_aux[df_aux['p_date'] < fecha_inicio_mes]

    # Agregación mensual
    df_mensual = (
        df_aux
        .set_index('p_date')
        .resample('MS')['ventas_totales_producto']
        .sum()
        .fillna(0)
    )

    # Últimos 8 meses disponibles
    df_cv = df_mensual.tail(8)

    media = df_cv.mean()
    desviacion = df_cv.std()

    cv = (desviacion / media) * 100 if media > 0 else 0

    return cv > 60

#4.2.2
def verificacionFactibilidadModelo(df_ean: pd.DataFrame) -> bool:
    """P4.F2 Aux 2: Verifica si un material cuenta con suficiente
    historial útil para entrenar un modelo de demanda diario.

    El criterio de factibilidad es:
    - Se consideran solo días con ventas no extremadamente bajas
      (ventas >= 10% del promedio histórico).
    - Deben existir al menos 150 días que cumplan esta condición.

    Este filtro busca evitar entrenar modelos con:
    - series demasiado cortas
    - días con quiebres o ventas casi nulas
    - señal estadística insuficiente

    Parameters
    ----------
    df_ean : pd.DataFrame
        DataFrame histórico del material/EAN con columna `cantidad_total`.

    Returns
    -------
    bool
        True si el material tiene historial suficiente y útil
        para entrenar un modelo; False en caso contrario.
    """
    cantidad_media = df_ean['cantidad_total'].mean()

    # Umbral mínimo de venta diaria considerada "útil"
    cantidad_min = cantidad_media / 10

    df_filtrado = df_ean[
        df_ean['cantidad_total'] >= cantidad_min
    ]

    return df_filtrado.shape[0] >= 150

#4.3
def construir_pesos_wls(
    fechas: pd.Series,
    pesos_sqrt: bool = False,
    pesos_log: bool = False,
    pesos_percent: bool = False
) -> pd.Series:
    """P4.F3: Construye pesos WLS priorizando observaciones más recientes.

    Inputs:
    fechas : pd.Series
        Serie de fechas asociadas a cada observación.
    pesos_sqrt : bool
        Si True, aplica raíz cuadrada a los pesos base.
    pesos_log : bool
        Si True, aplica transformación logarítmica.
    pesos_percent : bool
        Si True, capea los pesos al percentil 90.

    Outputs:
    pd.Series
        Pesos WLS normalizados, alineados con `fechas`.
    """
    fechas_ordenadas = np.sort(fechas.unique())
    pesos_base = np.arange(1, len(fechas_ordenadas) + 1, dtype=float)

    if pesos_sqrt:
        pesos_base = np.sqrt(pesos_base)
    elif pesos_log:
        pesos_base = np.log1p(pesos_base)
    elif pesos_percent:
        p90 = np.percentile(pesos_base, 90)
        pesos_base = np.minimum(pesos_base, p90)

    pesos_base = pesos_base / pesos_base.mean()
    mapa = dict(zip(fechas_ordenadas, pesos_base))

    return fechas.map(mapa)

#4.4
def registro_iteracion(promo: str,
                       nombre_promo: str,
                       contexto: dict,
                       msn_elasticidad: str = '-',
                       motivo_historial: str = '-',
                       disp_modelo : str = '-',
                       estado_fecha_proy : str = '-',
                       estable : str = '-'
                         ):

    return {
            'N° promoción': str(promo),
            'Nombre promoción': nombre_promo,
            'Categoría': contexto['categoria'],
            'Descripcion': contexto['desc_material'],
            'Material': contexto['material'],
            'UMV': contexto['sales_uom'],
            'EAN': contexto['ean_promocion'],
            'R²': '-',
            'Elasticidad': '-',
            'Estable': estable,
            'Inicio Proy': contexto['fecha_inicio'],
            'Fin Proy': contexto['fecha_fin'],
            'Baseline_UV': '-',
            'UV Incremental Real': '-',
            'UV Incremental Proy': '-',
            'UV Real': '-',
            'UV Proy': '-',
            'Baseline Venta': '-',
            'Venta Incremental Real': '-',
            'Venta Incremental Proy': '-',
            'Venta Real': '-',
            'Venta Proy': '-',
            'Estado_Historial': motivo_historial,
            'Estado_Modelo': disp_modelo,
            'Estado_Elasticidad': msn_elasticidad,
            'Estado_fecha_proy': estado_fecha_proy,
            'Estado_proyección':'-',
            'Comentario':'-'
            }

#4.5
def calcular_gap_dias_y_rango(
    fecha_fin_hist: str | pd.Timestamp,
    fecha_inicio_promo: str | pd.Timestamp
) -> tuple[int, str]:
    """Calcula la cantidad de días entre dos fechas y clasifica
    el rango temporal según criterios de negocio.

    Reglas de clasificación:
    - <= 30 días          → 'rango corto'
    - 31 a 90 días        → 'rango medio'
    - 91 a 180 días       → 'rango alto'
    - > 180 días          → 'rango extremo'

    Parameters
    ----------
    fecha_inicio_promo : str | pd.Timestamp
        Fecha inicial (histórica).
    fecha_fin_hist : str | pd.Timestamp
        Fecha final (por ejemplo inicio de promoción).

    Returns
    -------
    tuple
        (dias, clasificacion)
        dias : int
            Cantidad de días entre las fechas.
        clasificacion : str
            Categoría del rango temporal.
    """

    fecha_inicio = pd.to_datetime(fecha_inicio_promo)
    fecha_fin = pd.to_datetime(fecha_fin_hist)

    dias = (fecha_inicio - fecha_fin).days

    if dias <= 0:
        rango = 'en rango'
    elif dias <= 30:
        rango = 'rango corto'
    elif dias <= 90:
        rango = 'rango medio'
    elif dias <= 180:
        rango = 'rango alto'
    else:
        rango = 'rango extremo'

    return dias, rango

#4.6
def entrenar_modelo_material(df_prod: pd.DataFrame,
                             ean: str,
                             fecha_limite: str,
                             fecha_inicial_entrenamiento: str):
    """Entrena un modelo econométrico (WLS log-log) para un material
    específico

    Esta función actúa como wrapper sobre `obtenerModeloOLS` y
    se encarga de:
    - Ejecutar el entrenamiento
    - Extraer métricas resumidas del modelo (R² ajustado, elasticidad)
    - Manejar el caso en que el modelo no sea entrenable

    Parameters
    ----------
    df_prod : pd.DataFrame
        Histórico completo del material.
    ean : str
        Código EAN del producto.
    fecha_limite : str
        Fecha máxima a considerar en entrenamiento (YYYY-MM-DD).
    fecha_inicial_entrenamiento : str
        Fecha mínima de entrenamiento (YYYY-MM-DD).

    Returns
    -------
    tuple
        (modelo, r2, elasticidad) si el modelo es entrenable.
        (None, None, None) si no lo es.
    """

    modelo, datos_train = obtenerModeloOLS(
        df_prod,
        ean,
        fecha_limite,
        fecha_inicial_entrenamiento,
        considerar_feriados=True,
    )

    if modelo is None:
        return None, None, None, None

    r2 = round(float(modelo.rsquared_adj), 2) if modelo is not None else None

    elasticidad = (
        float(modelo.params['log_precio'])
        if hasattr(modelo, 'params') and 'log_precio' in modelo.params.index
        else None
    )
    return modelo, r2, elasticidad, datos_train

#4.6.1
def obtenerModeloOLS(
    df_ool: pd.DataFrame,
    ean: str,
    fecha_limite: str,
    fecha_inicial_entrenamiento: str,
    considerar_feriados: bool = True,
    pesos_sqrt: bool = False,
    pesos_log: bool = False,
    pesos_percent: bool = False
) -> tuple[pd.DataFrame, sm.regression.linear_model.RegressionResultsWrapper] | None:
    """Ajusta un modelo econométrico WLS (log-log) de demanda diaria para
     un EAN.

    El modelo se entrena únicamente con datos previos al inicio
    de una promoción,
    utilizando pesos temporales para priorizar observaciones más recientes.

    La especificación incluye:
    - log_precio
    - dummies de calendario (día, año, feriados)
    - dummies mensuales (si hay cobertura suficiente)
    - variables de sustitución (si existen y no contienen NA)
    - flag de promo (apo)
    - variación porcentual de subcategoría

    Si el dataset no cumple condiciones mínimas, retorna None.

    Parameters
    ----------
    df_ool : pd.DataFrame
        Histórico completo del material.
    ean : str
        Código EAN a modelar.
    fecha_limite : str
        Fecha máxima a considerar en entrenamiento (YYYY-MM-DD).
    fecha_inicial_entrenamiento : str
        Fecha mínima a considerar en entrenamiento (YYYY-MM-DD).
    considerar_feriados : bool
        Si False, excluye feriados y pre-feriados.
    pesos_sqrt : bool
        Usa pesos con raíz cuadrada.
    pesos_log : bool
        Usa pesos logarítmicos (default recomendado).
    pesos_percent : bool
        Capea pesos al percentil 90.

    Returns
    -------
    tuple[pd.DataFrame, RegressionResultsWrapper] | None
        (X utilizado en el ajuste, modelo entrenado) o None si
        no entrenable.
    """

    # -------------------------------------------------------------
    # 1) Filtrado inicial del histórico
    # -------------------------------------------------------------
    df_ean = df_ool[df_ool['ean'] == ean].copy()

    df_ean = df_ean[
        (df_ean['p_date'] >= fecha_inicial_entrenamiento) &
        (df_ean['p_date'] <= fecha_limite)
    ]

    if not considerar_feriados and {'feriado', 'pre_feriado'}.issubset(df_ean.columns):
        df_ean = df_ean[
            (df_ean['feriado'] == 0) &
            (df_ean['pre_feriado'] == 0)
        ]

    if df_ean.empty:
        return None, None

    # -------------------------------------------------------------
    # 2) Transformaciones básicas
    # -------------------------------------------------------------
    columnas_requeridas = {'cantidad_total', 'precio_promedio', 'p_date'}
    if not columnas_requeridas.issubset(df_ean.columns):
        return None, None

    df_ean['log_cantidad'] = np.log(df_ean['cantidad_total'])
    df_ean['log_precio'] = np.log(df_ean['precio_promedio'])

    # Filtro de ventas extremadamente bajas (regla 10% del promedio)
    cantidad_media = df_ean['cantidad_total'].mean()
    if not np.isfinite(cantidad_media) or cantidad_media <= 0:
        return None, None

    df_ean = df_ean[df_ean['cantidad_total'] >= cantidad_media / 10]
    if df_ean.empty:
        return None, None

    # -------------------------------------------------------------
    # 3) Definición de variables explicativas
    # -------------------------------------------------------------
    fixed_vars = [
        'p_date',
        'log_precio', #antes log_precio
        'martes', 'miercoles', 'jueves', 'viernes', 'sabado', 'domingo',
        '2024', '2025', '2026',
        'multiplicador_x05',
        'jueves_santo', 'viernes_santo',
        '30_abril', '20_mayo', '21_mayo',
        '19_junio', '20_junio',
        '15_julio', '16_julio',
        '14_agosto', '15_agosto',
        '14_septiembre', '15_septiembre',
        '16_septiembre', '17_septiembre',
        'pre_halloween', 'halloween',
        'pre_navidad', 'navidad',
        'pre_ano_nuevo', 'ano_nuevo',
        'apo',
        'variacion_porcentual_subcategoria'
    ]

    fixed_vars = [
        v for v in fixed_vars
        if (v == 'log_precio') or (v in df_ean.columns and
                                  df_ean[v].nunique() > 1)  # noqa: PD101
    ]

    if 'log_precio' not in fixed_vars:
        return None, None

    # Meses (sin enero para evitar trampa de dummies)
    meses_vars = [
        'febrero', 'marzo', 'abril', 'mayo', 'junio',
        'julio', 'agosto', 'septiembre',
        'octubre', 'noviembre', 'diciembre'
    ]
    meses_vars = [
        m for m in meses_vars
        if m in df_ean.columns and df_ean[m].nunique() > 1  # noqa: PD101
    ]

    # Verificar cobertura completa por mes (>= 5 días)
    dias_por_mes = (
        df_ean
        .assign(mes=df_ean['p_date'].dt.month,
                dia=df_ean['p_date'].dt.date)
        .groupby('mes')['dia']
        .nunique()
        .reindex(range(1, 13), fill_value=0)
    )
    if not (dias_por_mes >= 5).all():
        meses_vars = []

    # Sustitutos
    sust_vars = []
    for v in ['variacion_top1_sustituto', 'variacion_top3_sustitutos']:
        if v in df_ean.columns and not df_ean[v].isna().any():
            sust_vars.append(v)  # noqa: PERF401

    current_vars = fixed_vars + meses_vars + sust_vars

    # -------------------------------------------------------------
    # 4) Ajuste del modelo WLS
    # -------------------------------------------------------------
    X = sm.add_constant(df_ean[current_vars], has_constant='add')  # noqa: N806
    y = df_ean['log_cantidad']

    pesos = construir_pesos_wls(
        df_ean['p_date'],
        pesos_sqrt=pesos_sqrt,
        pesos_log=pesos_log,
        pesos_percent=pesos_percent
    )

    x_sin_pdate = X.drop(columns=['p_date'])
    return sm.WLS(y, x_sin_pdate, weights=pesos).fit(), (X,y,pesos)


#4.7
def clasificar_elasticidad(elasticidad: float | None) -> str:
    """Clasifica una elasticidad precio según criterios de negocio.

    Reglas:
    - None           → 'Elasticidad inválida'
    - elasticidad > 0 → 'Elasticidad positiva'
    - -8 <= elasticidad <= -0.1 → 'Elasticidad en rango'
    - elasticidad < -8 → 'Elasticidad muy negativa'

    Parameters
    ----------
    elasticidad : float | None
        Elasticidad estimada del modelo.

    Returns
    -------
    str
        Clasificación textual de la elasticidad.
    """
    if elasticidad is None:
        return 'Elasticidad inválida'

    try:
        elasticidad = float(elasticidad)
    except (TypeError, ValueError):
        return 'Elasticidad inválida'

    if elasticidad >= 0:
        return 'positiva'

    #ojímetro
    if -8 <= elasticidad < 0:
        return 'En rango'

    if elasticidad < -8:
        return 'muy negativa'

    return None

#4.8
def obtenerDfDefaultProyeccion(
    df_hist_material: pd.DataFrame,
    modelo,
    ean: str,
    fecha_inicial_proyeccion: str,
    fecha_final_proyeccion: str,
    considerar_feriados: bool = True
) -> pd.DataFrame:
    """Construye el DataFrame de proyección compatible con un modelo
    econométrico ya entrenado.

    La función:
    - Usa el histórico del material
    - Construye filas conocidas y futuras
    - Asegura que las variables coincidan exactamente con las usadas
    en el modelo
    - Marca filas conocidas vs proyectadas

    Parameters
    ----------
    df_hist_material : pd.DataFrame
        Histórico completo del material.
    modelo : RegressionResultsWrapper
        Modelo econométrico ya entrenado.
    ean : str
        Código EAN a proyectar.
    fecha_inicial_proyeccion : str
        Fecha inicial del período de proyección (YYYY-MM-DD).
    fecha_final_proyeccion : str
        Fecha final del período de proyección (YYYY-MM-DD).
    considerar_feriados : bool
        Indica si se consideran feriados en la proyección.

    Returns
    -------
    pd.DataFrame
        DataFrame listo para ser usado en proyección.
    """

    # -------------------------------
    # 1) Variables esperadas por el modelo
    # -------------------------------
    x_vars = [v for v in modelo.model.exog_names if v != 'const']

    # Seguridad: asegurar apo
    if 'apo' not in x_vars:
        x_vars.append('apo')

    # -------------------------------
    # 2) Histórico del EAN
    # -------------------------------
    df_ean = df_hist_material[df_hist_material['ean'] == ean].copy()
    df_ean = df_ean.sort_values('p_date')

    fecha_inicio_dt = pd.to_datetime(fecha_inicial_proyeccion)
    fecha_fin_dt = pd.to_datetime(fecha_final_proyeccion)

    df_conocido = df_ean[
        (df_ean['p_date'] >= fecha_inicio_dt) &
        (df_ean['p_date'] <= fecha_fin_dt)
    ].copy()

    x_vars_base = [v for v in x_vars if v != 'log_precio']
    columnas_base = [
        'p_date',
        'precio_promedio',
        'cantidad_total',
        'ventas_totales_producto',
        'feriado',
        'pre_feriado',
        *x_vars_base
    ]
    columnas_base = list(dict.fromkeys(columnas_base))

    df_conocido = df_conocido[columnas_base]
    df_conocido['conocido'] = 'Si'

    # -------------------------------
    # 3) Fechas futuras
    # -------------------------------
    ultima_fecha = df_hist_material['p_date'].max()


    if fecha_fin_dt > ultima_fecha:

        fechas_faltantes = pd.date_range(
            start=max(ultima_fecha + pd.Timedelta(days=1), fecha_inicio_dt),
            end=fecha_fin_dt,
            freq='D'
        )

        df_futuro = pd.DataFrame({'p_date': fechas_faltantes})

        # Valores base
        df_futuro['precio_promedio'] = int(df_ean['precio_promedio'].mean())
        df_futuro['cantidad_total'] = -1
        df_futuro['ventas_totales_producto'] = -1

        # Dummies calendario

        df_futuro['martes'] = (df_futuro['p_date'].dt.dayofweek == 1).astype(int) #
        df_futuro['miércoles'] = (df_futuro['p_date'].dt.dayofweek == 2).astype(int) #
        df_futuro['jueves'] = (df_futuro['p_date'].dt.dayofweek == 3).astype(int)
        df_futuro['viernes'] = (df_futuro['p_date'].dt.dayofweek == 4).astype(int)
        df_futuro['sabado'] = (df_futuro['p_date'].dt.dayofweek == 5).astype(int)
        df_futuro['domingo'] = (df_futuro['p_date'].dt.dayofweek == 6).astype(int)

        df_futuro['mes'] = df_futuro['p_date'].dt.month
        for i, nombre_mes in enumerate(
            ['enero','febrero','marzo','abril','mayo','junio',
             'julio','agosto','septiembre','octubre','noviembre','diciembre'],
            start=1
        ):
            df_futuro[nombre_mes] = (df_futuro['mes'] == i).astype(int)

        df_futuro['p_year'] = df_futuro['p_date'].dt.year.astype(str)
        df_futuro = pd.concat([
            df_futuro,
            pd.get_dummies(df_futuro['p_year'], prefix='', prefix_sep='')
        ], axis=1)

        if considerar_feriados:
            df_futuro = agregarFeriados(df_futuro)
            df_futuro = df_futuro[df_futuro['feriado_irrenunciable'] != 1]

        df_futuro['conocido'] = 'No'

        # Asegurar columnas
        for col in df_conocido.columns:
            if col not in df_futuro.columns:
                df_futuro[col] = 0

        df_futuro = df_futuro[df_conocido.columns]
        df_final = pd.concat([df_conocido, df_futuro], ignore_index=True)

    else:
        df_final = df_conocido.copy()

    # -------------------------------
    # 4) Formato final
    # -------------------------------
    df_final['p_date'] = pd.to_datetime(df_final['p_date']).dt.strftime('%Y-%m-%d')

    orden_preferido = [
        'p_date', 'conocido', 'precio_promedio',
        'cantidad_total', 'ventas_totales_producto', 'apo'
    ]
    otras = [c for c in df_final.columns if c not in orden_preferido]

    return df_final[orden_preferido + otras]

#4.9
def aplicar_precios_promocionales(
    df_pre: pd.DataFrame,
    df_ean: pd.DataFrame
) -> pd.DataFrame:
    """Aplica precios promocionales al DataFrame de proyección
    usando la información de la promoción.
    """
    df_prices = df_ean[
        ['p_date', 'precio_promocional_minimo', 'precio_modal']
    ].copy()

    df_prices['p_date'] = df_prices['p_date'].dt.strftime('%Y-%m-%d')

    df_out = df_pre.merge(df_prices, on='p_date', how='left')

    df_out['precio_promedio'] = df_out['precio_promocional_minimo'].where(
        df_out['precio_promocional_minimo'] > 0,
        df_out['precio_promedio']
    )

    return df_out

#4.10
def proyectar_escenarios(
    df_pre: pd.DataFrame,
    modelo,
    df_hist_prod: pd.DataFrame,
    considerar_venta_incremental: bool
):
    """Proyecta escenarios promocional y base (sin descuento).

    Returns
    -------
    tuple
        (res_promo, res_base)
    """

    res_promo = calcular_resultados_escenario(
        df_pre,
        modelo,
        df_hist_prod
    )

    res_base = None
    if considerar_venta_incremental:
        df_base = df_pre.copy()
        df_base['precio_promedio'] = df_base['precio_modal']

        res_base = calcular_resultados_escenario(
            df_base,
            modelo,
            df_hist_prod
        )

    return res_promo, res_base

#4.10.1
def calcular_resultados_escenario(
    df_proj: pd.DataFrame,
    modelo,
    df_hist: pd.DataFrame
) -> dict:
    """Calcula resultados reales y proyectados para un escenario dado.

    Parameters
    ----------
    df_proj : pd.DataFrame
        DataFrame de proyección.
    modelo : RegressionResultsWrapper
        Modelo entrenado.
    df_hist : pd.DataFrame
        Histórico real del material.

    Returns
    -------
    dict
        Métricas agregadas del escenario.
    """
    df_pred = obtenerProyeccionV2(df_proj, modelo)

    dias = df_pred['p_date'].unique()
    hist = df_hist[df_hist['p_date'].isin(dias)]

    return {
        'uv_real': hist['cantidad_total'].sum(),
        'uv_proy': df_pred['cantidad_total_predicha'].sum(),
        'venta_real': hist['ventas_totales_producto'].sum(),
        'venta_proy': df_pred['ventas_totales_producto_predicha'].sum(),
        'contiene_art': (df_pred['cantidad_total'] == -1).any(),
        'df_pred': df_pred
    }

#4.10.1.1
def obtenerProyeccionV2(
    df_proyeccion: pd.DataFrame,
    modelo
) -> pd.DataFrame:
    """P4.F4: Genera proyecciones diarias de cantidad y ventas usando
    un modelo econométrico previamente entrenado.

    Parameters
    ----------
    df_proyeccion : pd.DataFrame
        DataFrame con las variables necesarias para predecir.
    modelo : RegressionResultsWrapper
        Modelo entrenado.

    Returns
    -------
    pd.DataFrame
        DataFrame con predicciones diarias.
    """
    df_pred = df_proyeccion.sort_values('p_date').copy()

    # Variable requerida por el modelo actual
    df_pred['log_precio'] = np.log(df_pred['precio_promedio'])

    x_vars = [v for v in modelo.model.exog_names if v != 'const']
    x = df_pred[x_vars].fillna(0)
    x = sm.add_constant(x, has_constant='add')

    if not np.isfinite(x.values).all():
        logging.warning('X contiene valores no finitos antes de la predicción')
        print(df_pred[['log_precio','precio_promedio']])
        print('x_vars:', x_vars)

    df_pred['log_cantidad_predicha'] = modelo.predict(x)
    df_pred['cantidad_total_predicha'] = np.exp(df_pred['log_cantidad_predicha'])
    df_pred['ventas_totales_producto_predicha'] = (
        df_pred['precio_promedio'] * df_pred['cantidad_total_predicha']
    )

    df_pred[['cantidad_total_predicha',
             'ventas_totales_producto_predicha']] = (
        df_pred[['cantidad_total_predicha',
                 'ventas_totales_producto_predicha']].astype(int)
    )

    df_pred['p_date'] = pd.to_datetime(df_pred['p_date'])

    return df_pred

#4.11
def adicion_registro_proyeccion(
    logs_iteracion: dict,
    res_promo: dict,
    res_base: dict | None
) -> dict:
    """Construye la fila resumen final para una combinación promoción
    material.

    Centraliza métricas de contexto, resultados proyectados, resultados
    reales (si existen) y diagnósticos de estabilidad del modelo.

    Parameters
    ----------
    logs_iteracion: dict
        Diccionario con el registro de iteraciones
    res_promo : dict
        Resultados del escenario promocional.
    res_base : dict | None
        Resultados del escenario base (sin promo).

    Returns
    -------
    dict
        Fila resumen lista para consolidar en el output final.
    """

    contiene_art = res_promo.get('contiene_art', False)

    # -----------------------------
    # Resultados reales
    # -----------------------------
    if contiene_art:
        uv_real = venta_real = uv_inc_real = venta_inc_real = '-'
    else:
        uv_real = res_promo['uv_real']
        venta_real = res_promo['venta_real']

        if res_base:
            uv_inc_real = uv_real - res_base['uv_proy']
            venta_inc_real = venta_real - res_base['venta_proy']
        else:
            uv_inc_real = venta_inc_real = '-'

    # -----------------------------
    # Resultados proyectados
    # -----------------------------
    if res_base:
        baseline_uv = res_base['uv_proy']
        baseline_venta = res_base['venta_proy']

        uv_inc_proy = res_promo['uv_proy'] - res_base['uv_proy']
        venta_inc_proy = res_promo['venta_proy'] - res_base['venta_proy']
    else:
        baseline_uv = baseline_venta = '-'
        uv_inc_proy = venta_inc_proy = '-'


    logs_iteracion['Baseline_UV'] = baseline_uv
    logs_iteracion['UV Real'] = uv_real
    logs_iteracion['UV Proy'] = res_promo['uv_proy']
    logs_iteracion['UV Incremental Real'] = uv_inc_real
    logs_iteracion['UV Incremental Proy'] = uv_inc_proy

    logs_iteracion['Baseline Venta'] = baseline_venta
    logs_iteracion['Venta Real'] = venta_real
    logs_iteracion['Venta Proy'] = res_promo['venta_proy']
    logs_iteracion['Venta Incremental Real'] = venta_inc_real
    logs_iteracion['Venta Incremental Proy'] = venta_inc_proy

    return logs_iteracion

def comparacion_ventas_historial(
    df_train: pd.DataFrame,
    df_pred: pd.DataFrame,
    porc_diff_up: float = 1.75,
    porc_diff_down: float = 0.5,
    porc_dias_comparacion: float = 0.75,
) -> str | None:
    """Compara ventas predichas contra historial reciente o anual."""


    #Información de entrenamiento
    x_train = df_train[0]

    #Ventas durante entrenamiento: precio * unidades_vendidas
    y_train = np.exp(df_train[0]['log_precio']) * np.exp(df_train[1])

    #Se ordenan las fechas de predicción
    dias_pred = sorted(df_pred['p_date'].unique())

    #Se crea una nueva lista con las fechas de pred pero un año antes
    dias_last_year = [
        fecha - pd.DateOffset(years=1)
        for fecha in dias_pred
    ]

    #Se extrae el periodo de predicción equivalente del año pasado
    x_last_year = x_train[
        x_train['p_date'].isin(dias_last_year)
    ]

    #Cantidad de días post extracción
    dias_encontrados = x_last_year['p_date'].nunique()

    #Obtención las ventas durante predicción.
    ventas_pred = df_pred[
        'ventas_totales_producto_predicha'
    ].sum()

    # Si los días encontrados superan una cantidad equivalente a:
    # <porc_dias_comparacion> * días de promo
    # Entonces se compara con el periodo anual anterior. Caso contrario,
    # se mira los últimos días equivalente al periodo evaluado.

    if dias_encontrados >= (
        porc_dias_comparacion * len(dias_pred)
    ):
        # print('Entramos a comparar periodo anual pasado')  # noqa: ERA001

        #Se extraen los índices de los días correspondientes al periodo
        #anual equivalente anterior y se indexa con las ventas observadas
        y_train_equivalente = y_train[
            y_train.index.isin(x_last_year.index)
        ]

        #la suma de la venta diaria corresponde a unas ventas pasadas
        ventas_past = y_train_equivalente.sum()

    #Caso contrario, sumamos las ventas de los últimos días equivalentes
    #a la duración de la promo
    else:
        # print('Entramos a comparar con últimos días')  # noqa: ERA001
        fechas_recientes = (
            pd.Series(
                x_train['p_date'].unique()
            )
            .sort_values()
            .tail(len(dias_pred))
        )

        #extracción de fechas.
        x_last_past = x_train[
            x_train['p_date'].isin(fechas_recientes)
        ]

        #Mismo proceso de extracción de índices
        y_train_equivalente = y_train[
            y_train.index.isin(x_last_past.index)
        ]

        #la suma de la venta diaria corresponde a unas ventas pasadas
        ventas_past = y_train_equivalente.sum()

    # print('Ventas predichas: ',ventas_pred)  # noqa: ERA001
    # print('Ventas observadas: ', ventas_past)  # noqa: ERA001
    if ventas_pred >= porc_diff_up * ventas_past:
        comentario = 'Proyección sobrestimada'
    elif ventas_pred <= porc_diff_down * ventas_past:
        comentario = 'Proyección subestimada'

    else:
        comentario = '-'

    return comentario, round(ventas_past), round(ventas_pred)

# Main Parte 4
def loop_promociones(
    promos_existentes,
    df_resultado: pd.DataFrame,
    df_universo: pd.DataFrame,
    fecha_inicial_entrenamiento: str,
    considerar_venta_incremental: bool = True
) -> pd.DataFrame:

    filas_resumen = []

    for idx, promo in enumerate(promos_existentes, start=1):

        #Parche temporal: vis promos vacías.
        logging.info('-----------------------------------------')
        logging.info(f'[{idx}/{len(promos_existentes)}] PROMO {promo}')
        logging.info('-----------------------------------------')

        df_promo = df_resultado[df_resultado['n_promocion'] == promo]
        nombre_promo = str(df_promo['nombre_promocion'].iloc[0])

        logging.info('-----------------------------------------')
        logging.info(f'[{idx}/{len(promos_existentes)}] PROMO {promo} - {nombre_promo}')
        logging.info('-----------------------------------------')

        claves = df_promo['clave_material'].unique()
        #claves = ['610485_ST', '610486_ST']  # noqa: ERA001
        total_claves = len(claves)
        porcentaje_anterior = -10

        for j, clave in enumerate(claves, start=1):
            porcentaje = int(j / total_claves * 100)
            if porcentaje % 10 == 0 and porcentaje != porcentaje_anterior:
                porcentaje_anterior = porcentaje
                logging.info(f'Avance materiales: {porcentaje}%')

            # -----------------------------
            # 1) Subset promo material
            # -----------------------------
            df_ean = df_promo[df_promo['clave_material'] == clave]
            logging.info(f'Procesando material {clave} ({j}/{total_claves})')
            contexto = extraer_contexto_material(df_ean)

            df_hist_prod = df_universo[
                (df_universo['material'] == contexto['material']) &
                (df_universo['sales_uom'] == contexto['sales_uom'])
            ]

            # -----------------------------
            # 1.a) Registro iteración
            # -----------------------------
            logs_iteracion = registro_iteracion(promo, nombre_promo,
                                                contexto)

            # -----------------------------
            # 1.b) Validación Datos Historial. False -> next iter
            # -----------------------------
            valido, motivo = validar_material(
                df_hist_prod,
                contexto['fecha_inicio_mes'])
            # -----------------------------
            # 1.c) Validación Datos Historial. False -> next iter
            # -----------------------------
            logs_iteracion['Estado_Historial'] = motivo

            if not valido:
                logs_iteracion['Estado_proyección'] = 'No se pudo proyectar'
                logs_iteracion['Comentario'] = motivo
                filas_resumen.append(logs_iteracion)
                continue

            # -----------------------------
            # 1.d) Conteo de días
            # -----------------------------
            _, rango = calcular_gap_dias_y_rango(
                fecha_fin_hist = df_hist_prod['p_date'].max(),
                fecha_inicio_promo=contexto['fecha_inicio'])

            logs_iteracion['Estado_fecha_proy'] = rango

            # -----------------------------
            # 2) Entrenamiento del modelo
            # -----------------------------
            modelo, r2, elasticidad, data_train = entrenar_modelo_material(
                df_hist_prod,
                contexto['ean_promocion'],
                contexto['fecha_limite'],
                fecha_inicial_entrenamiento)

            # -----------------------------
            # 2.a) Validación modelo
            # -----------------------------
            if modelo is None:

                #Para evaluación
                logs_iteracion['Estado_Modelo'] = 'Inválido'
                logs_iteracion['Estado_proyección'] = 'No se pudo proyectar'

                logs_iteracion['Comentario'] = 'Modelo no entrenable'
                filas_resumen.append(logs_iteracion)
                continue

            logs_iteracion['Estado_Modelo'] = 'Válido'

            # -----------------------------
            # 2.b) Validación elasticidad
            # -----------------------------
            logs_iteracion['Elasticidad'] = round(elasticidad,3)
            logs_iteracion['R²'] = r2

            msn_elasticidad= clasificar_elasticidad(elasticidad)
            logs_iteracion['Estado_Elasticidad'] = msn_elasticidad

            if msn_elasticidad != 'En rango':
                logs_iteracion['Estado_proyección'] = 'No se pudo proyectar'
                logs_iteracion['Comentario'] = f'Elasticidad {msn_elasticidad}'
                filas_resumen.append(logs_iteracion)
                continue

            # -----------------------------
            # 3) Preparar DF proyección
            # -----------------------------
            df_pre = obtenerDfDefaultProyeccion(
                df_hist_material=df_hist_prod,
                ean=contexto['ean_promocion'],
                modelo=modelo,
                fecha_inicial_proyeccion=contexto['fecha_inicio'],
                fecha_final_proyeccion=contexto['fecha_fin'],
                considerar_feriados=True)

            descripcion_promo = str(
                df_ean['descripcion_evento_promocional'].iloc[0]
            ).lower()

            df_pre['apo'] = int('apo' in descripcion_promo)
            df_pre = aplicar_precios_promocionales(df_pre, df_ean)

            # -----------------------------
            # 4) Proyecciones
            # -----------------------------
            res_promo, res_base = proyectar_escenarios(
                df_pre,
                modelo,
                df_hist_prod,
                considerar_venta_incremental
            )

            # Implementación Trigger Venta Sobre/subestimada vs historial
            comentario, _, _ = comparacion_ventas_historial(data_train, res_promo['df_pred'])  # noqa: E501

            if comentario != '-':
                logs_iteracion['Estado_proyección'] = 'No se pudo proyectar'
                logs_iteracion['Comentario'] = comentario
                # filas_resumen.append(logs_iteracion)  # noqa: ERA001
                # continue  # noqa: ERA001

            # -----------------------------
            # 5) Registro iteración válida
            # -----------------------------
            logs_iteracion = adicion_registro_proyeccion(
                logs_iteracion=logs_iteracion,
                res_promo=res_promo,
                res_base=res_base)

            logs_iteracion['Estado_proyección'] = 'Viable'
            #logs_iteracion['Valores Comparativa']=[ventas_past, ventas_obs]  # noqa: ERA001, W505

            filas_resumen.append(logs_iteracion)

    return pd.DataFrame(filas_resumen)


def generar_excel_buffer(
    df: pd.DataFrame,
    sheet_name: str = 'desglose_proyeccion'
) -> io.BytesIO:
    buffer = io.BytesIO()

    with pd.ExcelWriter(buffer, engine='xlsxwriter') as writer:
        df.to_excel(writer, index=False, sheet_name=sheet_name)
        writer.sheets[sheet_name].freeze_panes(1, 0)

    buffer.seek(0)
    return buffer

# Main Parte 5
def subir_archivo_sharepoint(
    contenido: io.BytesIO,
    nombre_archivo: str,
    outputs_dir: str,
    sp_cred: dict
) -> None:
    """Sube un archivo a SharePoint usando un buffer en memoria.
    """

    # MUY IMPORTANTE: asegurar puntero al inicio
    contenido.seek(0)

    output_remote_path = posixpath.join(outputs_dir, nombre_archivo)

    logging.info(f'Subiendo archivo a SharePoint: {output_remote_path}')

    sp_output = sp.SharePointFile(
        **sp_cred,
        server_relative_path=output_remote_path
    )

    # PASAR EL BUFFER, NO LOS BYTES
    sp_output.upload(content=contenido)

    logging.info('✅ Archivo subido correctamente a SharePoint')



def main():


    #------- Inputs ---------#
    args = vars(parser.parse_args())
    execution_date: str = args['execution_date']
    proyecto: str = args['project_id']  # noqa: F841
    store_banner:str = args['store_banner']

    file_site = '/sites/BigDatayAdvancedAnalytics/Documentos compartidos/'
    file_site += 'Pricing/Forecast Promociones'
    secret_name = 'bdaa_sharepoint_credentials'  # noqa: S105#HC
    sp_cred = secretmanager.getSecret(secret_name, project=proyecto)

    esquema = 'TMP'
    tabla = 'TMP_REGRESSION_PROCESSED_DATA_FORECAST'
    path_table = f'{proyecto}.{esquema}.{tabla}'

    # Nota: local queda definido, en producción se inyecta desde Airflow
    gbq_client = Client()
    logging.info(f'execution_date: {execution_date}')
    logging.info(f'proyecto: {proyecto}')
    logging.info(f'store_banner: {store_banner}')


    # ---- Parte 1: Extracción de datos desde el Sharepoint ---- #
    logging.info('Iniciando forecast de promociones')
    logging.info('[1/5]: Extracción de datos: ')

    df_input, lista_promos, outputs_dir, nombre_output = preparar_input_promociones(
        sp_cred=sp_cred,
        file_site=file_site,
        patron_input=PATRON_INPUT,
        patron_output=PATRON_OUTPUT
    )
    string_promos =  ','.join(lista_promos)

    logging.info(f'[patch] Visualización STRING PROMOS [n {len(lista_promos)}] \n', string_promos)

    # ---- Parte 2: Procesamiento y construcción de DataFrames ---- #
    logging.info('[2/5]: Construcción de DataFrame Historial venta...')
    df_final = generarDataFrame(query='query_data_procesada',
                                categorias=string_promos,
                                gbq_client=gbq_client,
                                path_table= path_table,
                                usuario='pricing',
                                store_banner = store_banner)

    # ---- Parte 3: Construcción de df_resultado y df_universo ---- #
    logging.info('[3/5]: Construcción de df_resultado y df_universo...')

    df_resultado, df_universo, promos_validas  = generar_dataset_promos_proyectables(string_promos,
                                    df_input,
                                    df_final,
                                    gbq_client)

    fecha_inicial_entrenamiento = df_universo['p_date'].min().strftime('%Y-%m-%d')

    # ---- Parte 4: Loop Promoción-Material ---- #
    logging.info('[4/5]: Loop Promociones...')
    df_proyecciones = loop_promociones(
        promos_validas,
        df_resultado,
        df_universo,
        fecha_inicial_entrenamiento)

    df_proyecciones = df_proyecciones.drop(columns=
                                           ['Estado_Historial',
                                            'Estado_Modelo',
                                            'Estado_Elasticidad',
                                            'Estado_fecha_proy',
                                            'Estado_proyección'])

    logging.info('[Anexo]: Estado de las proyecciones:\n%s', df_proyecciones['Comentario'].value_counts())  # noqa: E501

    print('Antes de subir a GCP: ', df_proyecciones.info())
    print('Execution date: ', execution_date, 'type ', type(execution_date))

    # ---- Parte 5: Subida resultados a Sharepoint ---- #
    logging.info('[5/5]: Guardado de resultados...')

    output_buffer = generar_excel_buffer(df_proyecciones)  # noqa: F841


    #subir_archivo_sharepoint(
    #    contenido=output_buffer,  # noqa: ERA001
    #    nombre_archivo=nombre_output,  # noqa: ERA001
    #    outputs_dir=outputs_dir,  # noqa: ERA001
    #    sp_cred=sp_cred  # noqa: ERA001
    #)  # noqa: ERA001


    df_proyecciones.insert(
        loc=0,
        column='execution_date',
        value=execution_date
    )

    # Definir el WHERE
    print('Justo antes de subir a GCP: ', df_proyecciones.info())
    where_clause = f"execution_date = '{execution_date}'"

    # Parametros
    # Parche 3: Tabla ajustada a sector oriente
    esquema = 'PRECIO_PROMOCIONES'
    tabla = 'FORECAST_PROMOCIONES'

    # Se elimina los datos para cierto store_banner y rango (si existen)
    deleteFromTable(table_ref=f'{proyecto}.{esquema}.{tabla}',
                    where_clause=where_clause,
                    gbq_client=gbq_client)



    # Se carga en BQ con los datos recalculados
    # Parche 4: json ajustado a sector oriente
    # Parche 5: append -> replace.

    uploadFrame(
        df_proyecciones,
        table_ddl_json_path=os.path.join('gbq_objects',
                                         'ingest_product_forecast.json'),
        project=proyecto,
        gbq_client=gbq_client,
        if_exists='append'
    )

    logging.info('Se sube la tabla a GCP')
if __name__ == '__main__':

    main()
