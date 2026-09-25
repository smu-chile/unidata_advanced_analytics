from __future__ import annotations

import io  # noqa: F401
import os
import sys
import logging
import argparse  # noqa: F401
import posixpath
from logging import config  # noqa: F401
from dataclasses import field, dataclass
from collections.abc import Mapping

import numpy as np  # type: ignore  # noqa: F401, PGH003
import pandas as pd  # type: ignore  # noqa: PGH003, TC002
from sklearn.ensemble import HistGradientBoostingRegressor

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



logger = logging.getLogger('modelo_promocional')


def configurar_logging(nivel: int = logging.INFO) -> None:
    """Configura el logger del pipeline con salida por consola.

    Parameters
    ----------
    nivel : int
        Nivel de logging (por ejemplo logging.INFO o logging.DEBUG).
        Default: logging.INFO.

    Returns
    -------
    None
    """
    logging.basicConfig(
        level=nivel,
        format='%(asctime)s | %(levelname)s | %(message)s',
        datefmt='%H:%M:%S',
    )
    logger.setLevel(nivel)


# =========================================================================
# 1. CONFIGURACIÓN CENTRAL
# =========================================================================
@dataclass(frozen=True)
class ConfiguracionModeloPromo:
    '''Configuración central del modelo de forecast promocional.

    Decisiones metodológicas
    -------------------------
    - Se entrena un modelo independiente por combinación promoción-EAN.
    - La validación usa el último tramo temporal como test.
    - La proyección principal corresponde al total del período promocional.

    Atributos (parámetro: tipo — valores válidos — descripción)
    --------------------------------------------------------------
    loss : str — {'poisson', 'squared_error', 'absolute_error', ...}
        Función de pérdida de HistGradientBoostingRegressor.
    reentrenar_con_historial_completo : bool
        Si True, se reentrena un modelo productivo con 100% del historial.
    usar_pesos_temporales : bool
        Activa ponderación cronológica en el ajuste del modelo.
    metodo_pesos_temporales : str
        {'sin_pesos', 'lineal', 'raiz', 'logaritmico', 'exponencial'}.
    normalizar_pesos_temporales : bool
        Si True, los pesos se normalizan para promediar 1.
    tasa_decaimiento_exponencial : float
        Tasa de decaimiento (>0) usada solo si metodo es 'exponencial'.
    tolerancia_cobertura_relativa : float
        Proporción (0-1) del valor real usada como tolerancia de coverage.
    tolerancia_cobertura_absoluta : float
        Tolerancia absoluta mínima (unidades) usada en el cálculo coverage.
    tratar_pasado_sin_registro_como_cero : bool
        Si True, días pasados sin historial se imputan en 0 unidades.
    columna_adi, columna_cv2, columna_clasificacion_adi_cv2 : str
        Nombres de columnas de tipología de demanda en caracterización.
    familia_promocional : str — {'B', 'T'}
        Familia de columnas promocionales a utilizar.
    minimo_dias_con_venta : int
        Días mínimos con venta histórica para que un EAN sea elegible.
    umbral_intensidad_baja, umbral_intensidad_alta : float — [0, 100]
        Cortes de intensidad promocional que definen el régimen.
    proporcion_test : float — (0, 1)
        Proporción final del historial reservada para test temporal.
    guardar_detalle_diario : bool
        Si True, se conserva el detalle diario de cada proyección.
    usar_peso_recencia : bool
        Activa ponderación adicional por antigüedad (independiente de
        usar_pesos_temporales).
    tasa_decaimiento_recencia : float
        Tasa de decaimiento (>0) para el peso por recencia.
    incluir_precio, incluir_promocion, incluir_descuento,
    incluir_mecanica, incluir_dia_semana : bool
        Interruptores globales de familias de variables del modelo.
    minimo_mecanicas_distintas : int
        Cantidad mínima de mecánicas distintas para habilitar mecánica.
    minimo_observaciones_por_mecanica : int
        Observaciones históricas mínimas para que una mecánica sea válida.
    learning_rate, max_iter, max_leaf_nodes, max_depth, min_samples_leaf,
    l2_regularization, max_bins, early_stopping, validation_fraction,
    n_iter_no_change, tol, random_state
        Hiperparámetros estándar de HistGradientBoostingRegressor.
    l2_regularization_regularizado, min_samples_leaf_regularizado,
    max_leaf_nodes_regularizado
        Hiperparámetros conservadores para intensidad media-alta.
    columna_ean, columna_fecha, columna_target, columna_precio,
    columna_intensidad, columna_dias_venta, columna_producto_nuevo : str
        Nombres estructurales de columnas en el historial.
    valores_afirmativos : tuple[str, ...]
        Valores de texto interpretados como "Sí" en flags binarios.
    '''

    loss: str = 'poisson'
    reentrenar_con_historial_completo: bool = True

    usar_pesos_temporales: bool = False
    metodo_pesos_temporales: str = 'sin_pesos'
    normalizar_pesos_temporales: bool = True
    tasa_decaimiento_exponencial: float = 0.005

    tolerancia_cobertura_relativa: float = 0.30
    tolerancia_cobertura_absoluta: float = 1.00

    tratar_pasado_sin_registro_como_cero: bool = True

    columna_adi: str = 'ADI'
    columna_cv2: str = 'CV2'
    columna_clasificacion_adi_cv2: str = 'TIPOLOGIA_DEMANDA'

    familia_promocional: str = 'B'
    minimo_dias_con_venta: int = 150

    umbral_intensidad_baja: float = 40.0
    umbral_intensidad_alta: float = 80.0

    proporcion_test: float = 0.20
    guardar_detalle_diario: bool = True
    usar_peso_recencia: bool = False
    tasa_decaimiento_recencia: float = 0.002

    incluir_precio: bool = True
    incluir_promocion: bool = True
    incluir_descuento: bool = True
    incluir_mecanica: bool = True
    incluir_dia_semana: bool = True

    minimo_mecanicas_distintas: int = 2
    minimo_observaciones_por_mecanica: int = 5

    learning_rate: float = 0.05
    max_iter: int = 300
    max_leaf_nodes: int = 15
    max_depth: int | None = None
    min_samples_leaf: int = 20
    l2_regularization: float = 1.0
    max_bins: int = 255

    early_stopping: bool = True
    validation_fraction: float = 0.10
    n_iter_no_change: int = 20
    tol: float = 1e-7
    random_state: int = 123

    l2_regularization_regularizado: float = 5.0
    min_samples_leaf_regularizado: int = 30
    max_leaf_nodes_regularizado: int = 10

    columna_ean: str = 'EAN'
    columna_fecha: str = 'P_DATE'
    columna_target: str = 'CANTIDAD_TOTAL'
    columna_precio: str = 'PRECIO_PROMEDIO'
    columna_intensidad: str = 'INTENSIDAD_PROMOCIONAL'
    columna_dias_venta: str = 'DIAS_CON_VENTA'
    columna_producto_nuevo: str = 'ES_PRODUCTO_NUEVO'

    valores_afirmativos: tuple[str, ...] = field(
        default=('SI', 'SÍ', 'TRUE', '1')
    )

    def __post_init__(self) -> None:
        familia = self.familia_promocional.upper()

        if familia not in {'B', 'T'}:
            msg = 'familia_promocional debe ser B o T.'
            raise ValueError(msg)

        if not 0 < self.proporcion_test < 1:
            msg_0 = 'proporcion_test debe estar entre 0 y 1.'
            raise ValueError(msg_0)

        if not (
            0
            <= self.umbral_intensidad_baja
            < self.umbral_intensidad_alta
            <= 100
        ):
            msg_1 = (
                'Los umbrales de intensidad deben cumplir: '
                '0 <= umbral bajo < umbral alto <= 100.'
            )
            raise ValueError(
                msg_1
            )


# 2. COLUMNAS DE LA FAMILIA PROMOCIONAL
# =========================================================================
@dataclass(frozen=True)
class ColumnasFamiliaPromocional:
    """Nombres de columnas históricas asociadas a la familia B o T.

    Atributos
    ---------
    familia : str — {'B', 'T'}
    precio_modal, precio_promocional, precio_promocional_minimo : str
    numero_promocion, nombre_promocion : str
    descripcion_evento : str
        Columna con la mecánica/evento promocional.
    porcentaje_descuento : str
    flag_promocion : str
    """

    familia: str
    precio_modal: str
    precio_promocional: str
    precio_promocional_minimo: str
    numero_promocion: str
    nombre_promocion: str
    descripcion_evento: str
    porcentaje_descuento: str
    flag_promocion: str


def resolver_columnas_familia(
    familia_promocional: str,
) -> ColumnasFamiliaPromocional:
    """Construye los nombres de columnas correspondientes a la familia.

    Parameters
    ----------
    familia_promocional : str
        'B' o 'T' (no sensible a mayúsculas).

    Returns
    -------
    ColumnasFamiliaPromocional
        Nombres a utilizar en todo el pipeline.
    """
    familia = familia_promocional.strip().upper()

    if familia not in {'B', 'T'}:
        msg = 'familia_promocional debe ser B o T.'
        raise ValueError(msg)

    return ColumnasFamiliaPromocional(
        familia=familia,
        precio_modal=f'PRECIO_MODAL_{familia}',
        precio_promocional=f'PRECIO_PROMOCIONAL_{familia}',
        precio_promocional_minimo=f'PRECIO_PROMOCIONAL_MINIMO_{familia}',
        numero_promocion=f'N_PROMOCION_{familia}',
        nombre_promocion=f'NOMBRE_PROMOCION_{familia}',
        descripcion_evento=f'DESCRIPCION_EVENTO_PROMOCIONAL_{familia}',
        porcentaje_descuento=f'PORCENTAJE_DESCUENTO_{familia}',
        flag_promocion=f'FLAG_PROMO_{familia}',
    )


# =========================================================================
# 3. ESTRUCTURAS CONTENEDORAS
# =========================================================================
@dataclass
class FuentesModeloPreparadas:
    """Contenedor de las fuentes preparadas para entrenar y proyectar.

    Atributos
    ---------
    historial, caracterizacion, promociones_futuras : pd.DataFrame
    columnas_familia : ColumnasFamiliaPromocional
    """

    historial: pd.DataFrame
    caracterizacion: pd.DataFrame
    promociones_futuras: pd.DataFrame
    columnas_familia: ColumnasFamiliaPromocional


@dataclass(frozen=True)
class ResultadoElegibilidad:
    """Resultado de las reglas mínimas para entrenar/proyectar un EAN.

    Atributos
    ---------
    elegible : bool
    es_producto_nuevo : bool
    dias_con_venta : int
    estado_historial : str — {'Válido', 'Inválido', 'Sin caracterización'}
    comentario : str
    """

    elegible: bool
    es_producto_nuevo: bool
    dias_con_venta: int
    estado_historial: str
    comentario: str


@dataclass(frozen=True)
class RegimenPromocional:
    """Tratamiento metodológico asignado a un producto.

    Atributos
    ---------
    nombre : str — {'BASELINE', 'BASE_UPLIFT', 'BASE_UPLIFT_REGULARIZADO',
        'BASELINE_PROMOCIONAL'}
    intensidad_promocional : float — [0, 100]
    usar_flag_promocion, usar_porcentaje_descuento, usar_mecanica : bool
    usar_configuracion_regularizada : bool
    baseline_claro : bool
    tipo_baseline : str
    confianza_baseline : str — {'ALTA', 'MEDIA', 'BAJA'}
    comentario_baseline : str
    """

    nombre: str
    intensidad_promocional: float
    usar_flag_promocion: bool
    usar_porcentaje_descuento: bool
    usar_mecanica: bool
    usar_configuracion_regularizada: bool
    baseline_claro: bool
    tipo_baseline: str
    confianza_baseline: str
    comentario_baseline: str


@dataclass
class DatosEntrenamiento:
    """Matrices y metadatos para entrenar, evaluar y reproducir variables.

    Atributos
    ---------
    X_train, X_test : pd.DataFrame
    y_train, y_test : pd.Series
    pesos_train : np.ndarray | None
    fechas_train, fechas_test : pd.Series
    columnas_modelo, columnas_mecanica, mecanicas_validas : list[str]
    fecha_inicio_train, fecha_fin_train : pd.Timestamp
    fecha_inicio_test, fecha_fin_test : pd.Timestamp | None
    numero_observaciones, numero_observaciones_train,
    numero_observaciones_test : int
    usar_mecanica : bool
    comentario_mecanica : str
    """

    X_train: pd.DataFrame
    y_train: pd.Series
    X_test: pd.DataFrame
    y_test: pd.Series
    pesos_train: np.ndarray | None
    fechas_train: pd.Series
    fechas_test: pd.Series
    columnas_modelo: list[str]
    columnas_mecanica: list[str]
    mecanicas_validas: list[str]
    fecha_inicio_train: pd.Timestamp
    fecha_fin_train: pd.Timestamp
    fecha_inicio_test: pd.Timestamp | None
    fecha_fin_test: pd.Timestamp | None
    numero_observaciones: int
    numero_observaciones_train: int
    numero_observaciones_test: int
    usar_mecanica: bool
    comentario_mecanica: str


@dataclass(frozen=True)
class MetricasModelo:
    """Métricas de error calculadas en unidades originales.

    Atributos
    ---------
    wmape, bias, mae, coverage : float
    win_rate : float | None
    suma_real, suma_predicha, error_absoluto_total : float
    numero_observaciones, numero_predicciones_validas : int
    """

    wmape: float
    bias: float
    mae: float
    coverage: float
    win_rate: float | None
    suma_real: float
    suma_predicha: float
    error_absoluto_total: float
    numero_observaciones: int
    numero_predicciones_validas: int


@dataclass
class ResultadoEntrenamiento:
    """Resultado del doble entrenamiento (validación + productivo).

    Atributos
    ---------
    modelo_validacion : HistGradientBoostingRegressor
        Entrenado solo con el primer tramo temporal (train).
    modelo_productivo : HistGradientBoostingRegressor
        Entrenado con el 100% del historial; único usado en proyección.
    metricas_train, metricas_test : MetricasModelo
    predicciones_train, predicciones_test, benchmark_test : np.ndarray
    columnas_modelo : list[str]
    parametros_modelo : dict[str, object]
    iteraciones_validacion, iteraciones_productivo : int
    uso_pesos_temporales : bool
    metodo_pesos_temporales : str
    peso_minimo_train, peso_maximo_train,
    peso_minimo_productivo, peso_maximo_productivo : float | None
    """

    modelo_validacion: HistGradientBoostingRegressor
    modelo_productivo: HistGradientBoostingRegressor
    metricas_train: MetricasModelo
    metricas_test: MetricasModelo
    predicciones_train: np.ndarray
    predicciones_test: np.ndarray
    benchmark_test: np.ndarray
    columnas_modelo: list[str]
    parametros_modelo: dict[str, object]
    iteraciones_validacion: int
    iteraciones_productivo: int
    uso_pesos_temporales: bool
    metodo_pesos_temporales: str
    peso_minimo_train: float | None
    peso_maximo_train: float | None
    peso_minimo_productivo: float | None
    peso_maximo_productivo: float | None


@dataclass
class ResultadoEscenarios:
    """Resultado diario y agregado de una combinación promoción-EAN.

    Atributos
    ---------
    detalle_diario : pd.DataFrame
    resumen : dict[str, object]
    """

    detalle_diario: pd.DataFrame
    resumen: dict[str, object]


# 4. FUNCIONES AUXILIARES DE NORMALIZACIÓN
# =========================================================================
def normalizar_ean(serie_ean: pd.Series) -> pd.Series:
    """Normaliza EAN como texto, eliminando el sufijo .0 de Excel.

    Parameters
    ----------
    serie_ean : pd.Series
        Serie de identificadores EAN en cualquier tipo.

    Returns
    -------
    pd.Series
        Serie de tipo 'string', sin espacios ni sufijo '.0'.
    """
    return (
        serie_ean.astype('string')
        .str.strip()
        .str.replace(r'\.0$', '', regex=True)
    )


def normalizar_flag(serie_flag: pd.Series) -> pd.Series:  # noqa: D417
    """Normaliza flags de texto (mayúsculas, sin espacios).

    Parameters
    ----------
    serie_flag : pd.Series

    Returns
    -------
    pd.Series
        Serie 'string' en mayúsculas, sin convertir a booleano.
    """
    return serie_flag.astype('string').str.strip().str.upper()


def limpiar_porcentaje_descuento(serie_descuento: pd.Series) -> pd.Series:
    """Convierte descuento a float32, con NaN/negativos reemplazados por 0.

    Parameters
    ----------
    serie_descuento : pd.Series

    Returns
    -------
    pd.Series
        Serie float32, valores en [0, +inf).
    """
    descuento = pd.to_numeric(serie_descuento, errors='coerce').fillna(0.0)

    return descuento.clip(lower=0.0).astype('float32')


def es_valor_afirmativo(
    valor: object,
    configuracion: ConfiguracionModeloPromo,
) -> bool:
    """Determina si un flag representa una respuesta afirmativa.

    Parameters
    ----------
    valor : object
        Valor crudo del flag (texto, número, NaN, etc.).
    configuracion : ConfiguracionModeloPromo
        Se usa configuracion.valores_afirmativos.

    Returns
    -------
    bool
    """
    if pd.isna(valor):
        return False

    return str(valor).strip().upper() in configuracion.valores_afirmativos


# =========================================================================
# 5. PREPARACIÓN DE FUENTES
# =========================================================================
def preparar_promociones_futuras(
    promociones_futuras: pd.DataFrame,
) -> pd.DataFrame:
    """Prepara las combinaciones promoción-EAN a proyectar.

    Una fila representa una combinación promoción-EAN; el período no se
    expande a nivel diario aquí (se hace por combinación dentro del loop).

    Parameters
    ----------
    promociones_futuras : pd.DataFrame
        Debe incluir: N_PROMOCION, NOMBRE_PROMOCION, EAN,
        FECHA_INICIO_DE_PROMOCION, FECHA_FIN_DE_PROMOCION,
        PRECIO_PROMOCIONAL, PRECIO_MODAL, PORCENTAJE_DESCUENTO,
        DESCRIPCION_EVENTO_PROMOCIONAL.

    Returns
    -------
    pd.DataFrame
        Copia tipada y validada, con DURACION_PROMOCION_DIAS agregada.
    """
    columnas_requeridas = [
        'N_PROMOCION', 'NOMBRE_PROMOCION', 'EAN',
        'FECHA_INICIO_DE_PROMOCION', 'FECHA_FIN_DE_PROMOCION',
        'PRECIO_PROMOCIONAL', 'PRECIO_MODAL', 'PORCENTAJE_DESCUENTO',
        'DESCRIPCION_EVENTO_PROMOCIONAL',
    ]

    columnas_faltantes = set(columnas_requeridas).difference(
        promociones_futuras.columns
    )

    if columnas_faltantes:
        msg = (
            f'Faltan columnas en promociones futuras: '
            f'{sorted(columnas_faltantes)}'
        )
        raise ValueError(
            msg
        )

    promociones_preparadas = promociones_futuras.loc[
        :, columnas_requeridas
    ].copy()

    promociones_preparadas['EAN'] = normalizar_ean(
        promociones_preparadas['EAN']
    )
    promociones_preparadas['N_PROMOCION'] = (
        promociones_preparadas['N_PROMOCION'].astype('string').str.strip()
    )

    for columna in ('FECHA_INICIO_DE_PROMOCION', 'FECHA_FIN_DE_PROMOCION'):
        promociones_preparadas[columna] = pd.to_datetime(
            promociones_preparadas[columna], errors='coerce'
        )

    for columna in ('PRECIO_PROMOCIONAL', 'PRECIO_MODAL'):
        promociones_preparadas[columna] = pd.to_numeric(
            promociones_preparadas[columna], errors='coerce'
        ).astype('float32')

    promociones_preparadas['PORCENTAJE_DESCUENTO'] = (
        limpiar_porcentaje_descuento(
            promociones_preparadas['PORCENTAJE_DESCUENTO']
        )
    )

    promociones_preparadas['DESCRIPCION_EVENTO_PROMOCIONAL'] = (
        promociones_preparadas['DESCRIPCION_EVENTO_PROMOCIONAL']
        .astype('string')
        .fillna('SIN_MECANICA')
        .str.strip()
    )

    fechas_invalidas = (
        promociones_preparadas['FECHA_INICIO_DE_PROMOCION'].isna()
        | promociones_preparadas['FECHA_FIN_DE_PROMOCION'].isna()
        | (
            promociones_preparadas['FECHA_FIN_DE_PROMOCION']
            < promociones_preparadas['FECHA_INICIO_DE_PROMOCION']
        )
    )

    if fechas_invalidas.any():
        msg_0 = 'Existen promociones con fechas inválidas o incompletas.'
        raise ValueError(
            msg_0
        )

    precios_invalidos = (
        promociones_preparadas['PRECIO_PROMOCIONAL'].isna()
        | promociones_preparadas['PRECIO_MODAL'].isna()
        | promociones_preparadas['PRECIO_PROMOCIONAL'].le(0)
        | promociones_preparadas['PRECIO_MODAL'].le(0)
    )

    if precios_invalidos.any():
        msg_1 = 'Existen promociones con precios nulos o no positivos.'
        raise ValueError(
            msg_1
        )

    promociones_preparadas['DURACION_PROMOCION_DIAS'] = (
        (
            promociones_preparadas['FECHA_FIN_DE_PROMOCION']
            - promociones_preparadas['FECHA_INICIO_DE_PROMOCION']
        ).dt.days
        + 1
    ).astype('int16')

    return promociones_preparadas.reset_index(drop=True)


def preparar_caracterizacion(
    caracterizacion: pd.DataFrame,
    ean_requeridos: pd.Index,
) -> pd.DataFrame:
    """Prepara metadata y reglas de negocio solo para los EAN requeridos.

    Parameters
    ----------
    caracterizacion : pd.DataFrame
        Tabla completa de caracterización de productos.
    ean_requeridos : pd.Index
        EAN presentes en las promociones futuras.

    Returns
    -------
    pd.DataFrame
        Indexado por EAN (columna EAN conservada), un registro por EAN.
    """
    columnas_seleccionadas = [
        'EAN', 'PRODUCT_DESCRIPTION', 'CATEGORY_DESCRIPTION',
        'SUB_CATEGORY_DESCRIPTION', 'MATERIAL', 'SALES_UOM', 'SALES_UNIT',
        'DIAS_CON_VENTA', 'ES_PRODUCTO_NUEVO', 'INTENSIDAD_PROMOCIONAL',
        'TIPOLOGIA_DEMANDA', 'MECANICAS_VALIDAS', 'CASO_MODELO',
        'INCLUIR_MECANICA', 'MECANICA_REFERENCIA', 'SEGMENTO_ABCD',
        'ADI', 'CV2',
    ]

    columnas_disponibles = [
        columna
        for columna in columnas_seleccionadas
        if columna in caracterizacion.columns
    ]

    columnas_minimas = {
        'EAN', 'DIAS_CON_VENTA', 'ES_PRODUCTO_NUEVO',
        'INTENSIDAD_PROMOCIONAL',
    }

    columnas_faltantes = columnas_minimas.difference(columnas_disponibles)

    if columnas_faltantes:
        msg = f'Faltan columnas en caracterización: {sorted(columnas_faltantes)}'
        raise ValueError(
            msg
        )

    mascara_ean = normalizar_ean(caracterizacion['EAN']).isin(
        ean_requeridos
    )

    caracterizacion_preparada = caracterizacion.loc[
        mascara_ean, columnas_disponibles
    ].copy()

    caracterizacion_preparada['EAN'] = normalizar_ean(
        caracterizacion_preparada['EAN']
    )

    caracterizacion_preparada['DIAS_CON_VENTA'] = (
        pd.to_numeric(
            caracterizacion_preparada['DIAS_CON_VENTA'], errors='coerce'
        )
        .fillna(0)
        .astype('int32')
    )

    caracterizacion_preparada['INTENSIDAD_PROMOCIONAL'] = (
        pd.to_numeric(
            caracterizacion_preparada['INTENSIDAD_PROMOCIONAL'],
            errors='coerce',
        )
        .fillna(0.0)
        .clip(0.0, 100.0)
        .astype('float32')
    )

    caracterizacion_preparada['ES_PRODUCTO_NUEVO'] = normalizar_flag(
        caracterizacion_preparada['ES_PRODUCTO_NUEVO']
    )

    if 'INCLUIR_MECANICA' in caracterizacion_preparada.columns:
        caracterizacion_preparada['INCLUIR_MECANICA'] = normalizar_flag(
            caracterizacion_preparada['INCLUIR_MECANICA']
        )

    return (
        caracterizacion_preparada.drop_duplicates(subset='EAN', keep='first')
        .set_index('EAN', drop=False)
    )


def preparar_historial(  # noqa: D417
    historial: pd.DataFrame,
    ean_requeridos: pd.Index,
    columnas_familia: ColumnasFamiliaPromocional,
) -> pd.DataFrame:
    """Prepara únicamente el historial necesario para los EAN requeridos.

    Parameters
    ----------
    historial : pd.DataFrame
        Historial diario completo.
    ean_requeridos : pd.Index
    columnas_familia : ColumnasFamiliaPromocional

    Returns
    -------
    pd.DataFrame
        Ordenado por EAN y fecha, tipado en float32/int8 donde aplica.
    """
    columnas_modelo = [
        'EAN', 'P_DATE', 'CANTIDAD_TOTAL', 'PRECIO_PROMEDIO',
        'TENDENCIA_LINEAL', 'SIN_ANUAL', 'COS_ANUAL', 'FLAG_FERIADO',
        'FLAG_PRE_FERIADO',
        columnas_familia.precio_modal,
        columnas_familia.precio_promocional,
        columnas_familia.precio_promocional_minimo,
        columnas_familia.numero_promocion,
        columnas_familia.descripcion_evento,
        columnas_familia.porcentaje_descuento,
        columnas_familia.flag_promocion,
    ]

    columnas_faltantes = set(columnas_modelo).difference(historial.columns)

    if columnas_faltantes:
        msg = f'Faltan columnas en el historial: {sorted(columnas_faltantes)}'
        raise ValueError(
            msg
        )

    ean_normalizado = normalizar_ean(historial['EAN'])
    mascara_ean = ean_normalizado.isin(ean_requeridos)

    historial_preparado = historial.loc[mascara_ean, columnas_modelo].copy()
    historial_preparado['EAN'] = ean_normalizado.loc[mascara_ean]

    historial_preparado['P_DATE'] = pd.to_datetime(
        historial_preparado['P_DATE'], errors='coerce'
    )

    historial_preparado[columnas_familia.porcentaje_descuento] = (
        limpiar_porcentaje_descuento(
            historial_preparado[columnas_familia.porcentaje_descuento]
        )
    )

    columnas_binarias = [
        columnas_familia.flag_promocion, 'FLAG_FERIADO', 'FLAG_PRE_FERIADO',
    ]

    for columna in columnas_binarias:
        historial_preparado[columna] = (
            pd.to_numeric(historial_preparado[columna], errors='coerce')
            .fillna(0)
            .clip(0, 1)
            .astype('int8')
        )

    columnas_float32 = [
        'CANTIDAD_TOTAL', 'PRECIO_PROMEDIO', 'TENDENCIA_LINEAL',
        'SIN_ANUAL', 'COS_ANUAL',
        columnas_familia.precio_modal,
        columnas_familia.precio_promocional,
        columnas_familia.precio_promocional_minimo,
    ]

    for columna in columnas_float32:
        historial_preparado[columna] = pd.to_numeric(
            historial_preparado[columna], errors='coerce'
        ).astype('float32')

    historial_preparado = historial_preparado.dropna(
        subset=['EAN', 'P_DATE', 'CANTIDAD_TOTAL', 'PRECIO_PROMEDIO']
    )

    return (
        historial_preparado.sort_values(['EAN', 'P_DATE'], kind='mergesort')
        .reset_index(drop=True)
    )


def preparar_fuentes_modelo(  # noqa: D417
    historial: pd.DataFrame,
    caracterizacion: pd.DataFrame,
    promociones_futuras: pd.DataFrame,
    configuracion: ConfiguracionModeloPromo,
) -> FuentesModeloPreparadas:
    """Prepara las tres fuentes antes de entrar al loop promoción-EAN.

    Parameters
    ----------
    historial : pd.DataFrame
    caracterizacion : pd.DataFrame
    promociones_futuras : pd.DataFrame
    configuracion : ConfiguracionModeloPromo

    Returns
    -------
    FuentesModeloPreparadas
    """
    promociones_preparadas = preparar_promociones_futuras(
        promociones_futuras
    )

    ean_requeridos = pd.Index(
        promociones_preparadas['EAN'].dropna().unique()
    )

    columnas_familia = resolver_columnas_familia(
        configuracion.familia_promocional
    )

    caracterizacion_preparada = preparar_caracterizacion(
        caracterizacion=caracterizacion, ean_requeridos=ean_requeridos
    )

    historial_preparado = preparar_historial(
        historial=historial,
        ean_requeridos=ean_requeridos,
        columnas_familia=columnas_familia,
    )

    return FuentesModeloPreparadas(
        historial=historial_preparado,
        caracterizacion=caracterizacion_preparada,
        promociones_futuras=promociones_preparadas,
        columnas_familia=columnas_familia,
    )











COLUMNAS_TRANSVERSALES = [
    'TENDENCIA_LINEAL',
    'SIN_ANUAL',
    'COS_ANUAL',
    'FLAG_FERIADO',
    'FLAG_PRE_FERIADO',
]

COLUMNAS_DIA_SEMANA = [
    'dow_martes',
    'dow_miercoles',
    'dow_jueves',
    'dow_viernes',
    'dow_sabado',
    'dow_domingo',
]

COLUMNAS_EXCEL_FINAL = [
    'N° promoción', 'Nombre promoción', 'Categoría', 'Subcategoría',
    'Descripcion', 'Material', 'UMV', 'EAN', 'R²', 'Elasticidad',
    'Estable', 'TIPOLOGIA_DEMANDA', 'Inicio Proy', 'Fin Proy',
    'Precio Modal', 'Precio Promocional', 'Baseline_UV',
    'UV Incremental Real', 'UV Incremental Proy', 'UV Real', 'UV Proy',
    'Baseline Venta', 'Venta Incremental Real', 'Venta Incremental Proy',
    'Venta Real', 'Venta Proy', 'Estado_Historial', 'Estado_Modelo',
    'Estado_Elasticidad', 'Estado_fecha_proy', 'Estado_proyección',
    'Comentario', 'WMAPE Train', 'Bias Train', 'MAE Train',
    'Coverage Train', 'WMAPE Test', 'Bias Test', 'MAE Test',
    'Coverage Test', 'Win Rate Test', 'Intensidad Promocional',
    'SEGMENTO_ABCD', 'ADI', 'CV2', 'DIAS_CON_VENTA',
]





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


QUERY_INFO_PROMOS = QueryDict({
    'query_info_promos':
    """
    SELECT
        N_PROMOCION,
        NOMBRE_PROMOCION,
        DESCRIPCION_EVENTO_PROMOCIONAL,
        EAN,
        MATERIAL,
        FECHA_INICIO_DE_PROMOCION,
        FECHA_FIN_DE_PROMOCION,
        PORCENTAJE_DESCUENTO,
        PRECIO_MODAL,
        PRECIO_PROMOCIONAL
    FROM `cl-bigdata-analytics-preprod.CDA_VISTAS.VW_FACT_WORKFLOW`
    WHERE ORGANIZACION_VENTAS = '1000'
    AND CANAL_DISTRIBUCION = '10'
    AND REGISTRO_VALIDO = 'X'
    AND N_PROMOCION in (${promos})
"""
})


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

    lista_eans = list(df_historial['EAN'].unique())
    query_caracterizacion = QUERY_CARACTERIZACION['query_caracterizacion'].substitute(lista_eans =  ','.join(f"'{ean}'" for ean in lista_eans))  # noqa: E501
    df_caracterizacion = readBigQuery(
                    query=query_caracterizacion, user='pricing', gbq_client=gbq_client)

    print('Dimensiones df caracterizacion: ', df_caracterizacion.shape)
    print(f'Peso Caracterizacion: {df_caracterizacion.memory_usage(deep=True).sum() / 1024**2:.2f} MB')  # noqa: E501
    print('Cantidad de eans únicos: ', df_caracterizacion['EAN'].nunique())
    print('Cantidad de columnas: ', len(df_caracterizacion.columns))
    print('=' * 70 + '\n')
    print(df_caracterizacion.info())


    ### 1.4 PROMOS A PROYECTAR
    print('\n' + '=' * 70)
    print('PARTE 1.4: DF PROMOS A PROYECTAR')
    print('=' * 70)

    query_promos_proy = QUERY_INFO_PROMOS['query_info_promos'].substitute(promos =  ','.join(promociones))  # noqa: E501
    df_promos_proy = readBigQuery(
                    query=query_promos_proy, user='pricing', gbq_client=gbq_client)

    print('Dimensiones df promos proy: ', df_promos_proy.shape)
    print(f'Peso Caracterizacion: {df_promos_proy.memory_usage(deep=True).sum() / 1024**2:.2f} MB')  # noqa: E501
    print('Cantidad de eans únicos: ', df_promos_proy['N_PROMOCION'].nunique())
    print('Cantidad de columnas: ', len(df_promos_proy.columns))
    print('=' * 70 + '\n')
    print(df_promos_proy.info())


if __name__ == '__main__':
    main()
