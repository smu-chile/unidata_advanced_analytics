from __future__ import annotations

import io  # noqa: F401
import os
import sys
import logging
import argparse  # noqa: F401
import posixpath
from logging import config  # noqa: F401

import numpy as np  # type: ignore  # noqa: F401, PGH003
import pandas as pd  # type: ignore  # noqa: PGH003, TC002

# Pip
from google.cloud.bigquery import Client  # type: ignore  # noqa: F401, PGH003


directorio_actual = os.path.abspath(os.curdir)

while directorio_actual != os.path.sep:
    try:
        sys.path.append(directorio_actual)
        from credentials import credentials  # noqa: F401
        break  # Si la importación es exitosa, sale del bucle
    except ModuleNotFoundError:
        sys.path.pop()  # Remueve el directorio que no contenía el módulo
        directorio_actual = os.path.dirname(directorio_actual)  # Retrocede


import re  # noqa: E402, TC003
import logging  # noqa: E402, F811
import posixpath  # noqa: E402, F811

import common.gcp_extended.secretsmanager as secretmanager  # noqa: E402, F401
import common.office365_extended.sharepoint as sp  # noqa: E402
from common.constants import LOGGING_CONFIG  # noqa: E402, F401
from common.databases.queries import QueryDict  # noqa: E402, F401
from common.gcp_extended.bigquery import (  # noqa: E402
    uploadFrame,  # noqa: F401
    readBigQuery,  # noqa: F401
    deleteFromTable,  # noqa: F401
    createTableAsSelect,  # noqa: F401
)


SUFIJO_INPUT = '_input_proyeccion.xlsx'
SUFIJO_OUTPUT = '_resultado_proyeccion.xlsx'


def obtener_rutas_sharepoint(
    ruta_base: str,
) -> tuple[str, str]:
    """Construye las rutas de las carpetas Inputs y Outputs en SharePoint.

    Parameters
    ----------
    ruta_base : str
        Ruta base del sitio en SharePoint.

    Returns
    -------
    tuple[str, str]
        Rutas de Inputs y Outputs, respectivamente.
    """
    return (
        posixpath.join(ruta_base, 'Inputs'),
        posixpath.join(ruta_base, 'Outputs'),
    )


def obtener_carpetas_sharepoint(
    credenciales_sharepoint: dict,
    ruta_base: str,
) -> tuple[sp.SharePointFolder, sp.SharePointFolder, str, str]:
    """Inicializa las carpetas Inputs y Outputs de SharePoint.

    Parameters
    ----------
    credenciales_sharepoint : dict
        Credenciales de acceso a SharePoint.
    ruta_base : str
        Ruta base del sitio en SharePoint.

    Returns
    -------
    tuple
        Objeto de carpeta Inputs, objeto de carpeta Outputs, ruta de Inputs
        y ruta de Outputs.
    """
    ruta_inputs, ruta_outputs = obtener_rutas_sharepoint(ruta_base)

    carpeta_inputs = sp.SharePointFolder(
        **credenciales_sharepoint,
        server_relative_folder=ruta_inputs,
    )

    carpeta_outputs = sp.SharePointFolder(
        **credenciales_sharepoint,
        server_relative_folder=ruta_outputs,
    )

    return (
        carpeta_inputs,
        carpeta_outputs,
        ruta_inputs,
        ruta_outputs,
    )


def filtrar_archivos_excel(
    archivos: list[str],
    patron: re.Pattern,
) -> list[str]:
    """Filtra archivos Excel válidos que cumplen un patrón de nombre.

    Excluye archivos temporales de Excel que comienzan con '~$'.

    Parameters
    ----------
    archivos : list[str]
        Nombres de archivos disponibles.
    patron : re.Pattern
        Patrón regex que debe cumplir el nombre completo del archivo.

    Returns
    -------
    list[str]
        Archivos Excel válidos ordenados ascendentemente.
    """
    return sorted(
        archivo
        for archivo in archivos
        if (
            archivo.lower().endswith('.xlsx')
            and not archivo.startswith('~$')
            and patron.fullmatch(archivo)
        )
    )


def obtener_clave_input(
    nombre_input: str,
) -> str:
    """Extrae la clave base desde un nombre de input válido.

    Example
    -------
    '2025_11_05_v5_input_proyeccion.xlsx' -> '2025_11_05_v5'
    """
    if not nombre_input.endswith(SUFIJO_INPUT):
        msg = (
            f'El archivo no cumple el sufijo esperado: {SUFIJO_INPUT}. '
            f'Archivo recibido: {nombre_input}.'
        )
        raise ValueError(
            msg
        )

    return nombre_input.removesuffix(SUFIJO_INPUT)


def obtener_nombre_output(
    nombre_input: str,
) -> str:
    """Construye el nombre esperado del output asociado a un input.

    Example
    -------
    '2025_11_05_v5_input_proyeccion.xlsx'
    -> '2025_11_05_v5_resultado_proyeccion.xlsx'
    """
    clave_input = obtener_clave_input(nombre_input)

    return f'{clave_input}{SUFIJO_OUTPUT}'


def obtener_inputs_pendientes(
    inputs_validos: list[str],
    outputs_validos: set[str],
) -> list[str]:
    """Identifica inputs que aún no tienen su output correspondiente.

    Parameters
    ----------
    inputs_validos : list[str]
        Inputs válidos disponibles en la carpeta Inputs.
    outputs_validos : set[str]
        Outputs válidos disponibles en la carpeta Outputs.

    Returns
    -------
    list[str]
        Inputs pendientes, ordenados ascendentemente.
    """
    outputs_normalizados = {
        nombre_output.lower()
        for nombre_output in outputs_validos
    }

    return [
        nombre_input
        for nombre_input in inputs_validos
        if obtener_nombre_output(nombre_input).lower()
        not in outputs_normalizados
    ]


def cargar_input_desde_sharepoint(
    credenciales_sharepoint: dict,
    ruta_inputs: str,
    nombre_input: str,
) -> pd.DataFrame:
    """Carga un archivo Excel desde la carpeta Inputs de SharePoint.

    Parameters
    ----------
    credenciales_sharepoint : dict
        Credenciales de acceso a SharePoint.
    ruta_inputs : str
        Ruta de la carpeta Inputs en SharePoint.
    nombre_input : str
        Nombre del archivo que se cargará.

    Returns
    -------
    pd.DataFrame
        Contenido del archivo de input.
    """
    ruta_archivo = posixpath.join(ruta_inputs, nombre_input)

    archivo_sharepoint = sp.SharePointFile(
        **credenciales_sharepoint,
        server_relative_path=ruta_archivo,
    )

    return archivo_sharepoint.toFrame()


def validar_input_promociones(
    tabla_input: pd.DataFrame,
) -> list[str]:
    """Valida la estructura del input y extrae promociones a proyectar.

    Parameters
    ----------
    tabla_input : pd.DataFrame
        Input de promociones.

    Returns
    -------
    list[str]
        Promociones únicas con 'generar_proyeccion' igual a 'si'.

    Raises
    ------
    ValueError
        Si faltan columnas requeridas o no existen promociones seleccio-
        nadas.
    """
    columnas_requeridas = {
        'n_promocion',
        'generar_proyeccion',
    }

    columnas_faltantes = columnas_requeridas.difference(
        tabla_input.columns
    )

    if columnas_faltantes:
        msg = (
            'Faltan columnas en el Excel: '
            f'{sorted(columnas_faltantes)}.'
        )
        raise ValueError(
            msg
        )

    promociones = (
        tabla_input.loc[
            tabla_input['generar_proyeccion']
            .astype('string')
            .str.strip()
            .str.lower()
            .eq('si'),
            'n_promocion',
        ]
        .dropna()
        .astype('string')
        .unique()
        .tolist()
    )

    if not promociones:
        msg = (
            'No se encontraron promociones con '
            '`generar_proyeccion = si`.'
        )
        raise ValueError(
            msg
        )

    return promociones


def preparar_input_promociones(
    credenciales_sharepoint: dict,
    ruta_base_sharepoint: str,
    patron_input: re.Pattern,
    patron_output: re.Pattern,
    input_manual: pd.DataFrame | None = None,
    nombre_input_manual: str | None = None,
    abortar_si_no_pendientes: bool = True,
) -> tuple[pd.DataFrame | None, list[str], str | None, str | None]:
    """Prepara y valida el input de promociones para generar proyecciones.

    El flujo admite dos modos mutuamente excluyentes:

    1. Automático:
       Busca en SharePoint el último archivo pendiente de procesamiento.
       Un input se considera pendiente cuando no existe su output equiva-
       lente.

    2. Manual:
       Recibe directamente un DataFrame y el nombre lógico del input.
       Este modo permite reprocesar o proyectar un archivo específico,
       independientemente de si ya existe un output previo.

    Parameters
    ----------
    credenciales_sharepoint : dict
        Credenciales de acceso a SharePoint.
    ruta_base_sharepoint : str
        Ruta base que contiene las carpetas Inputs y Outputs.
    patron_input : re.Pattern
        Patrón regex para nombres de inputs válidos.
    patron_output : re.Pattern
        Patrón regex para nombres de outputs válidos.
    input_manual : pd.DataFrame, optional
        DataFrame cargado externamente para ejecución manual.
    nombre_input_manual : str, optional
        Nombre lógico asociado a input_manual. Debe respetar la convención
        de nombres de input para poder derivar el nombre del output.
    abortar_si_no_pendientes : bool, default=True
        Si es True, lanza una excepción cuando no hay inputs pendientes
        en el modo automático. Si es False, retorna valores nulos.

    Returns
    -------
    tuple
        tabla_input : pd.DataFrame | None
            Input validado.
        promociones : list[str]
            Promociones seleccionadas para proyectar.
        ruta_outputs : str | None
            Ruta de Outputs en SharePoint.
        nombre_output : str | None
            Nombre esperado para el archivo resultado.

    Raises
    ------
    ValueError
        Si se combinan incorrectamente los parámetros manuales, el nombre
        manual no cumple el patrón, no hay pendientes o
        el input es inválido.
    """
    usa_input_manual = input_manual is not None
    tiene_nombre_manual = nombre_input_manual is not None

    if usa_input_manual != tiene_nombre_manual:
        msg = (
            '`input_manual` y `nombre_input_manual` deben entregarse '
            'juntos.'
        )
        raise ValueError(
            msg
        )

    ruta_inputs, ruta_outputs = obtener_rutas_sharepoint(
        ruta_base_sharepoint
    )

    if usa_input_manual:
        if not patron_input.fullmatch(nombre_input_manual):
            msg_0 = (
                '`nombre_input_manual` no cumple el patrón de input válido: '
                f'{nombre_input_manual}.'
            )
            raise ValueError(
                msg_0
            )

        tabla_input = input_manual.copy()
        nombre_input = nombre_input_manual

        logging.info(
            'Procesando input manual: %s',
            nombre_input,
        )
    else:
        (
            carpeta_inputs,
            carpeta_outputs,
            ruta_inputs,
            ruta_outputs,
        ) = obtener_carpetas_sharepoint(
            credenciales_sharepoint,
            ruta_base_sharepoint,
        )

        inputs_validos = filtrar_archivos_excel(
            carpeta_inputs.fileList(),
            patron_input,
        )

        outputs_validos = set(
            filtrar_archivos_excel(
                carpeta_outputs.fileList(),
                patron_output,
            )
        )

        inputs_pendientes = obtener_inputs_pendientes(
            inputs_validos,
            outputs_validos,
        )

        if not inputs_pendientes:
            mensaje = 'No hay inputs pendientes por procesar.'
            logging.info(mensaje)

            if abortar_si_no_pendientes:
                raise ValueError(mensaje)

            return None, [], None, None

        nombre_input = inputs_pendientes[-1]

        logging.info(
            'Procesando último input pendiente: %s',
            nombre_input,
        )

        tabla_input = cargar_input_desde_sharepoint(
            credenciales_sharepoint,
            ruta_inputs,
            nombre_input,
        )

    nombre_output = obtener_nombre_output(nombre_input)

    logging.info(
        'Nombre de output esperado: %s',
        nombre_output,
    )

    promociones = validar_input_promociones(tabla_input)

    return (
        tabla_input,
        promociones,
        ruta_outputs,
        nombre_output,
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
    help='GCP project in which the script will be executed')
parser.add_argument('--execution_date', type=str, help='DAG execution date')
parser.add_argument('--store_banner', type=str, help='Store banner')

PATRON_INPUT = re.compile(
    r'^\d{4}_\d{2}_\d{2}_v\d+_input_proyeccion\.xlsx$',
    re.IGNORECASE)

PATRON_OUTPUT = re.compile(
    r'^\d{4}_\d{2}_\d{2}_v\d+_resultado_proyeccion\.xlsx$',
    re.IGNORECASE)


QUERY_HISTORIAL = QueryDict({
    'query_historial':
    """


WITH productos_objetivo as (
  SELECT DISTINCT(EAN) FROM `cl-bigdata-analytics-preprod.CDA_VISTAS.VW_FACT_WORKFLOW`
  WHERE organizacion_ventas = '1000'
    AND canal_distribucion = '10'
    AND registro_valido = 'X'
    AND n_promocion in (${promos})

)

SELECT * EXCEPT(STORE_BANNER,
SALES_UOM, SALES_UNIT,
MULTIPLICADOR_X05, APO,PROPORCION_CATEGORIA,VARIACION_PORCENTUAL_SUBCATEGORIA,
EAN_SUSTITUTO_1, EAN_SUSTITUTO_2, EAN_SUSTITUTO_3,EAN_SUSTITUTO_4, EAN_SUSTITUTO_5)
FROM `cl-bigdata-analytics-preprod.PRECIO_PROMOCIONES.FORECAST_HISTORIALES_PROCESSED_DATA`
WHERE EAN IN (SELECT EAN FROM productos_objetivo)
""" })


QUERY_CARACTERIZACION = QueryDict({
    'query_caracterizacion':
    """
    SELECT * FROM `cl-bigdata-analytics-preprod.PRECIO_PROMOCIONES.FORECAST_CARACTERIZACION_PRODUCTOS`
    WHERE EAN IN (${lista_eans})
""" })  # noqa: E501


def main():

    #------- Inputs ---------#
    print('Hola mundillo')

    args = vars(parser.parse_args())
    proyecto: str = args['project_id']  # noqa: F841
    store_banner:str = args['store_banner']  # noqa: F841

    file_site = '/sites/BigDatayAdvancedAnalytics/Documentos compartidos/'
    file_site += 'Pricing/Forecast Promociones'
    secret_name = 'bdaa_sharepoint_credentials'  # noqa: S105#HC
    sp_cred = secretmanager.getSecret(secret_name, project=proyecto)

    gbq_client = Client()


    print('\n' + '#' * 70)
    print('PARTE 1: LECTURA DE DATASETS')
    print('\n' + '#' * 70)


    ### 1.1 PROMOCIONES
    print('\n' + '=' * 70)
    print('PARTE 1.1: PROMOCIONES A PROYECTAR')
    print('=' * 70)

    tabla_input, promociones, ruta_outputs, nombre_output = preparar_input_promociones(  # noqa: RUF059
        credenciales_sharepoint=sp_cred,
        ruta_base_sharepoint=file_site,
        patron_input=PATRON_INPUT,
        patron_output=PATRON_OUTPUT)

    print(f'Archivo proyecciones: {nombre_output}')
    print(f'Promociones a proyectar: {len(promociones)}')
    print('=' * 70 + '\n')


    ### 1.2 HISTORIAL
    print('\n' + '=' * 70)
    print('PARTE 1.2: DF HISTORIAL')
    print('=' * 70)

    query_historial = QUERY_HISTORIAL['query_historial'].substitute(promos =  ','.join(promociones))  # noqa: E501
    df_historial = readBigQuery(
                    query=query_historial, user='pricing', gbq_client=gbq_client)

    print('Dimensiones df historial: ', df_historial.shape)
    print(f'Peso Historial: {df_historial.memory_usage(deep=True).sum() / 1024**2:.2f} MB')
    print('Cantidad de eans únicos: ', df_historial['EAN'].nunique())
    print(f'[HISTORIAL] Fecha MIN: {df_historial['P_DATE'].min()} - Fecha MAX {df_historial['P_DATE'].max()}')  # noqa: E501
    print('Cantidad de columnas: ', len(df_historial.columns))
    print('=' * 70 + '\n')
    print(df_historial.info())


    ### 1.3 CARACTERIZACIÓN
    print('\n' + '=' * 70)
    print('PARTE 1.3: DF CARACTERIZACION')
    print('=' * 70)

    lista_eans = df_historial['EAN'].unique().to_list()
    query_caracterizacion = QUERY_CARACTERIZACION['query_caracterizacion'].substitute(promos =  ','.join(lista_eans))  # noqa: E501
    df_caracterizacion = readBigQuery(
                    query=query_caracterizacion, user='pricing', gbq_client=gbq_client)

    print('Dimensiones df caracterizacion: ', df_caracterizacion.shape)
    print(f'Peso Caracterizacion: {df_caracterizacion.memory_usage(deep=True).sum() / 1024**2:.2f} MB')  # noqa: E501
    print('Cantidad de eans únicos: ', df_caracterizacion['EAN'].nunique())
    print('Cantidad de columnas: ', len(df_caracterizacion.columns))
    print('=' * 70 + '\n')
    print(df_caracterizacion.info())


if __name__ == '__main__':
    main()
