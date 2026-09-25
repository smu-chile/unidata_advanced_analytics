from __future__ import annotations

import io  # noqa: F401
import os
import sys
import logging
import argparse  # noqa: F401
import posixpath
from logging import config  # noqa: F401
from dataclasses import field, dataclass
from collections.abc import Mapping  # noqa: F401

import numpy as np  # type: ignore  # noqa: F401, PGH003
import pandas as pd  # type: ignore  # noqa: PGH003, TC002
from sklearn.ensemble import (
    HistGradientBoostingRegressor,  # type: ignore  # noqa: PGH003, TC002
)

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


def limpiar_porcentaje_descuento(serie_descuento: pd.Series) -> pd.Series:  # noqa: D417
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


# 6. ELEGIBILIDAD
# =========================================================================
def evaluar_elegibilidad(  # noqa: D417
    caracterizacion_ean: pd.Series | None,
    configuracion: ConfiguracionModeloPromo,
) -> ResultadoElegibilidad:
    """Evalúa las reglas mínimas para proyectar un EAN.

    Reglas: (1) debe existir en caracterización, (2) no ser producto
    nuevo, (3) tener al menos configuracion.minimo_dias_con_venta.

    Parameters
    ----------
    caracterizacion_ean : pd.Series | None
    configuracion : ConfiguracionModeloPromo

    Returns
    -------
    ResultadoElegibilidad
    """
    if caracterizacion_ean is None:
        return ResultadoElegibilidad(
            elegible=False,
            es_producto_nuevo=False,
            dias_con_venta=0,
            estado_historial='Sin caracterización',
            comentario='EAN no encontrado en caracterización',
        )

    dias_con_venta_crudo = pd.to_numeric(
        caracterizacion_ean.get('DIAS_CON_VENTA', 0), errors='coerce'
    )
    dias_con_venta = (
        int(dias_con_venta_crudo) if pd.notna(dias_con_venta_crudo) else 0
    )

    es_producto_nuevo = es_valor_afirmativo(
        caracterizacion_ean.get('ES_PRODUCTO_NUEVO'), configuracion
    )

    causas_exclusion = []

    if es_producto_nuevo:
        causas_exclusion.append('Producto nuevo')

    if dias_con_venta < configuracion.minimo_dias_con_venta:
        causas_exclusion.append(
            'Historial insuficiente: menos de '
            f'{configuracion.minimo_dias_con_venta} días con venta'
        )

    if causas_exclusion:
        return ResultadoElegibilidad(
            elegible=False,
            es_producto_nuevo=es_producto_nuevo,
            dias_con_venta=dias_con_venta,
            estado_historial='Inválido',
            comentario='; '.join(causas_exclusion),
        )

    return ResultadoElegibilidad(
        elegible=True,
        es_producto_nuevo=False,
        dias_con_venta=dias_con_venta,
        estado_historial='Válido',
        comentario='-',
    )


# =========================================================================
# 7. RÉGIMEN PROMOCIONAL
# =========================================================================
def determinar_uso_mecanica(  # noqa: D417
    caracterizacion_ean: pd.Series,
    configuracion: ConfiguracionModeloPromo,
) -> bool:
    """Determina si la mecánica promocional puede utilizarse.

    Se incluye solo si está habilitada globalmente, marcada en
    caracterización y existen al menos minimo_mecanicas_distintas.

    Parameters
    ----------
    caracterizacion_ean : pd.Series
    configuracion : ConfiguracionModeloPromo

    Returns
    -------
    bool
    """
    if not configuracion.incluir_mecanica:
        return False

    if not es_valor_afirmativo(
        caracterizacion_ean.get('INCLUIR_MECANICA'), configuracion
    ):
        return False

    numero_mecanicas = pd.to_numeric(
        caracterizacion_ean.get('N_MECANICAS_VALIDAS', 0), errors='coerce'
    )
    numero_mecanicas = 0 if pd.isna(numero_mecanicas) else int(
        numero_mecanicas
    )

    return numero_mecanicas >= configuracion.minimo_mecanicas_distintas


def clasificar_regimen_promocional(  # noqa: D417
    caracterizacion_ean: pd.Series,
    configuracion: ConfiguracionModeloPromo,
) -> RegimenPromocional:
    """Clasifica el EAN según su intensidad promocional.

    Intervalos: 0 → BASELINE; (0, bajo] → BASE_UPLIFT;
    (bajo, alto] → BASE_UPLIFT_REGULARIZADO; (alto, 100] →
    BASELINE_PROMOCIONAL (excluye flag, descuento y mecánica).

    Parameters
    ----------
    caracterizacion_ean : pd.Series
    configuracion : ConfiguracionModeloPromo

    Returns
    -------
    RegimenPromocional
    """
    intensidad = pd.to_numeric(
        caracterizacion_ean.get('INTENSIDAD_PROMOCIONAL', 0.0),
        errors='coerce',
    )
    intensidad = 0.0 if pd.isna(intensidad) else float(
        np.clip(intensidad, 0.0, 100.0)
    )

    usar_mecanica = determinar_uso_mecanica(
        caracterizacion_ean=caracterizacion_ean, configuracion=configuracion
    )

    if intensidad == 0.0:
        return RegimenPromocional(
            nombre='BASELINE',
            intensidad_promocional=intensidad,
            usar_flag_promocion=False,
            usar_porcentaje_descuento=False,
            usar_mecanica=False,
            usar_configuracion_regularizada=False,
            baseline_claro=True,
            tipo_baseline='BASELINE_OBSERVADO',
            confianza_baseline='ALTA',
            comentario_baseline='-',
        )

    if intensidad <= configuracion.umbral_intensidad_baja:
        return RegimenPromocional(
            nombre='BASE_UPLIFT',
            intensidad_promocional=intensidad,
            usar_flag_promocion=configuracion.incluir_promocion,
            usar_porcentaje_descuento=configuracion.incluir_descuento,
            usar_mecanica=usar_mecanica,
            usar_configuracion_regularizada=False,
            baseline_claro=True,
            tipo_baseline='BASELINE_CONVENCIONAL',
            confianza_baseline='ALTA',
            comentario_baseline='-',
        )

    if intensidad <= configuracion.umbral_intensidad_alta:
        return RegimenPromocional(
            nombre='BASE_UPLIFT_REGULARIZADO',
            intensidad_promocional=intensidad,
            usar_flag_promocion=configuracion.incluir_promocion,
            usar_porcentaje_descuento=configuracion.incluir_descuento,
            usar_mecanica=usar_mecanica,
            usar_configuracion_regularizada=True,
            baseline_claro=True,
            tipo_baseline='BASELINE_CONVENCIONAL',
            confianza_baseline='MEDIA',
            comentario_baseline=(
                'Baseline estimado con menor disponibilidad relativa '
                'de observaciones sin promoción'
            ),
        )

    return RegimenPromocional(
        nombre='BASELINE_PROMOCIONAL',
        intensidad_promocional=intensidad,
        usar_flag_promocion=False,
        usar_porcentaje_descuento=False,
        usar_mecanica=False,
        usar_configuracion_regularizada=False,
        baseline_claro=False,
        tipo_baseline='CONTRAFACTUAL_PRECIO_MODAL',
        confianza_baseline='BAJA',
        comentario_baseline=(
            'Baseline referencial por alta intensidad promocional; '
            'existe evidencia limitada de ventas sin promoción'
        ),
    )


def evaluar_ean(  # noqa: D417
    ean: str,
    caracterizacion: pd.DataFrame,
    configuracion: ConfiguracionModeloPromo,
) -> tuple[ResultadoElegibilidad, RegimenPromocional | None]:
    """Evalúa elegibilidad y régimen promocional de un EAN.

    Parameters
    ----------
    ean : str
    caracterizacion : pd.DataFrame
        Indexado por EAN (ver preparar_caracterizacion).
    configuracion : ConfiguracionModeloPromo

    Returns
    -------
    tuple[ResultadoElegibilidad, RegimenPromocional | None]
        El régimen es None si el EAN no es elegible.
    """
    if ean not in caracterizacion.index:
        return evaluar_elegibilidad(None, configuracion), None

    caracterizacion_ean = caracterizacion.loc[ean]
    elegibilidad = evaluar_elegibilidad(caracterizacion_ean, configuracion)

    if not elegibilidad.elegible:
        return elegibilidad, None

    regimen = clasificar_regimen_promocional(
        caracterizacion_ean=caracterizacion_ean, configuracion=configuracion
    )

    return elegibilidad, regimen


# =========================================================================
# 8. VARIABLES: DÍA DE SEMANA, MECÁNICA, PESOS
# =========================================================================
def agregar_dummies_dia_semana(  # noqa: D417
    historial: pd.DataFrame,
    columna_ean: str = 'EAN',
    columna_fecha: str = 'P_DATE',
) -> pd.DataFrame:
    """Agrega dummies de día de semana (lunes = categoría de referencia).

    Parameters
    ----------
    historial : pd.DataFrame
    columna_ean : str
    columna_fecha : str

    Returns
    -------
    pd.DataFrame
        Copia ordenada por EAN y fecha, con las 6 dummies dow_*.
    """
    columnas_faltantes = {columna_ean, columna_fecha}.difference(
        historial.columns
    )

    if columnas_faltantes:
        msg = f'Faltan las siguientes columnas: {sorted(columnas_faltantes)}'
        raise KeyError(
            msg
        )

    historial_resultado = historial.copy()
    historial_resultado[columna_fecha] = pd.to_datetime(
        historial_resultado[columna_fecha], errors='coerce'
    )

    fechas_invalidas = historial_resultado[columna_fecha].isna()

    if fechas_invalidas.any():
        msg = (
            f'La columna {columna_fecha!r} contiene '
            f'{int(fechas_invalidas.sum())} fecha(s) inválida(s).'
        )
        raise ValueError(
            msg
        )

    historial_resultado = historial_resultado.sort_values(
        [columna_ean, columna_fecha], kind='stable'
    ).reset_index(drop=True)

    dia_semana = historial_resultado[columna_fecha].dt.dayofweek

    mapa_dias = {
        'dow_martes': 1, 'dow_miercoles': 2, 'dow_jueves': 3,
        'dow_viernes': 4, 'dow_sabado': 5, 'dow_domingo': 6,
    }

    for nombre_columna, codigo_dia in mapa_dias.items():
        historial_resultado[nombre_columna] = dia_semana.eq(
            codigo_dia
        ).astype('int8')

    return historial_resultado


def obtener_mecanicas_validas(  # noqa: D417
    historial_ean: pd.DataFrame,
    columna_mecanica: str,
    columna_flag_promocion: str,
    configuracion: ConfiguracionModeloPromo,
) -> list[str]:
    """Identifica mecánicas con soporte histórico suficiente.

    Parameters
    ----------
    historial_ean : pd.DataFrame
    columna_mecanica : str
    columna_flag_promocion : str
    configuracion : ConfiguracionModeloPromo

    Returns
    -------
    list[str]
        Vacía si no se alcanza minimo_mecanicas_distintas.
    """
    mascara_promocion = pd.to_numeric(
        historial_ean[columna_flag_promocion], errors='coerce'
    ).fillna(0).eq(1)

    mecanica = (
        historial_ean.loc[mascara_promocion, columna_mecanica]
        .astype('string')
        .fillna('SIN_MECANICA')
        .str.strip()
        .replace('', 'SIN_MECANICA')
    )

    conteo_mecanicas = mecanica.value_counts()

    mecanicas_validas = conteo_mecanicas[
        conteo_mecanicas >= configuracion.minimo_observaciones_por_mecanica
    ].index.tolist()

    mecanicas_validas = [
        valor for valor in mecanicas_validas if valor != 'SIN_MECANICA'
    ]

    if len(mecanicas_validas) < configuracion.minimo_mecanicas_distintas:
        return []

    return sorted(mecanicas_validas)


def agregar_variables_mecanica(  # noqa: D417
    datos_ean: pd.DataFrame,
    columna_mecanica: str,
    columna_flag_promocion: str,
    mecanicas_validas: list[str],
) -> tuple[pd.DataFrame, list[str]]:
    """Codifica la mecánica promocional como variables binarias.

    Días sin promoción → SIN_MECANICA (referencia, sin dummy). Mecánicas
    poco frecuentes → MECANICA_OTRA.

    Parameters
    ----------
    datos_ean : pd.DataFrame
    columna_mecanica : str
    columna_flag_promocion : str
    mecanicas_validas : list[str]

    Returns
    -------
    tuple[pd.DataFrame, list[str]]
        Datos con dummies agregadas y la lista de columnas creadas.
    """
    resultado = datos_ean.copy()

    mecanica_original = (
        resultado[columna_mecanica]
        .astype('string')
        .fillna('SIN_MECANICA')
        .str.strip()
        .replace('', 'SIN_MECANICA')
    )

    es_promocion = pd.to_numeric(
        resultado[columna_flag_promocion], errors='coerce'
    ).fillna(0).eq(1)

    mecanica_agrupada = mecanica_original.where(es_promocion, 'SIN_MECANICA')
    es_mecanica_valida = mecanica_agrupada.isin(mecanicas_validas)

    mecanica_agrupada = mecanica_agrupada.where(
        es_mecanica_valida | mecanica_agrupada.eq('SIN_MECANICA'),
        'MECANICA_OTRA',
    )

    categorias = ['SIN_MECANICA', *mecanicas_validas, 'MECANICA_OTRA']
    mecanica_agrupada = pd.Categorical(mecanica_agrupada, categories=categorias)

    variables_mecanica = pd.get_dummies(
        mecanica_agrupada, prefix='MECANICA', dtype='int8'
    )

    columna_referencia = 'MECANICA_SIN_MECANICA'

    if columna_referencia in variables_mecanica.columns:
        variables_mecanica = variables_mecanica.drop(columns=columna_referencia)

    resultado = pd.concat(
        [resultado.reset_index(drop=True), variables_mecanica.reset_index(drop=True)],
        axis=1,
    )

    return resultado, variables_mecanica.columns.tolist()


def calcular_pesos_recencia(  # noqa: D417
    fechas_train: pd.Series,
    configuracion: ConfiguracionModeloPromo,
) -> np.ndarray | None:
    """Calcula pesos exponenciales por antigüedad
    (promedio normalizado a 1).

    Parameters
    ----------
    fechas_train : pd.Series
    configuracion : ConfiguracionModeloPromo
        Usa usar_peso_recencia y tasa_decaimiento_recencia.

    Returns
    -------
    np.ndarray | None
        None si la ponderación por recencia está desactivada.
    """
    if not configuracion.usar_peso_recencia or fechas_train.empty:
        return None

    fecha_maxima = fechas_train.max()
    antiguedad_dias = (fecha_maxima - fechas_train).dt.days.to_numpy(
        dtype='float32'
    )

    pesos = np.exp(
        -configuracion.tasa_decaimiento_recencia * antiguedad_dias
    ).astype('float32')

    promedio_pesos = pesos.mean()

    if promedio_pesos > 0:
        pesos = pesos / promedio_pesos

    return pesos


def construir_pesos_temporales(  # noqa: D417
    fechas: pd.Series,
    configuracion: ConfiguracionModeloPromo,
) -> np.ndarray | None:
    """Construye pesos cronológicos opcionales (crecen con la recencia).

    Parameters
    ----------
    fechas : pd.Series
    configuracion : ConfiguracionModeloPromo
        Usa usar_pesos_temporales, metodo_pesos_temporales
        {'sin_pesos', 'lineal', 'raiz', 'logaritmico', 'exponencial'}
        y normalizar_pesos_temporales.

    Returns
    -------
    np.ndarray | None
    """
    if not configuracion.usar_pesos_temporales:
        return None

    metodo = configuracion.metodo_pesos_temporales.strip().lower()

    if metodo == 'sin_pesos':
        return None

    metodos_validos = {'lineal', 'raiz', 'logaritmico', 'exponencial'}

    if metodo not in metodos_validos:
        msg = (
            f'Método de pesos temporales inválido: {metodo!r}. '
            f'Permitidos: {sorted(metodos_validos)}.'
        )
        raise ValueError(
            msg
        )

    orden_temporal = (
        pd.to_datetime(fechas, errors='raise')
        .rank(method='dense')
        .to_numpy(dtype='float64')
    )

    if metodo == 'lineal':
        pesos = orden_temporal
    elif metodo == 'raiz':
        pesos = np.sqrt(orden_temporal)
    elif metodo == 'logaritmico':
        pesos = np.log1p(orden_temporal)
    else:
        distancia = orden_temporal.max() - orden_temporal
        pesos = np.exp(-configuracion.tasa_decaimiento_exponencial * distancia)

    if configuracion.normalizar_pesos_temporales:
        promedio_pesos = pesos.mean()

        if promedio_pesos > 0:
            pesos = pesos / promedio_pesos

    return pesos.astype('float64', copy=False)


def seleccionar_columnas_modelo(  # noqa: D417
    regimen: RegimenPromocional,
    columnas_familia: ColumnasFamiliaPromocional,
    configuracion: ConfiguracionModeloPromo,
    columnas_mecanica: list[str],
) -> list[str]:
    """Construye la lista final de variables explicativas del modelo.

    Parameters
    ----------
    regimen : RegimenPromocional
    columnas_familia : ColumnasFamiliaPromocional
    configuracion : ConfiguracionModeloPromo
    columnas_mecanica : list[str]

    Returns
    -------
    list[str]
        Sin duplicados, preservando orden de inserción.
    """
    columnas_modelo: list[str] = []

    if configuracion.incluir_precio:
        columnas_modelo.append(configuracion.columna_precio)

    columnas_modelo.extend(COLUMNAS_TRANSVERSALES)

    if configuracion.incluir_dia_semana:
        columnas_modelo.extend(COLUMNAS_DIA_SEMANA)

    if regimen.usar_flag_promocion:
        columnas_modelo.append(columnas_familia.flag_promocion)

    if regimen.usar_porcentaje_descuento:
        columnas_modelo.append(columnas_familia.porcentaje_descuento)

    if regimen.usar_mecanica and columnas_mecanica:
        columnas_modelo.extend(columnas_mecanica)

    return list(dict.fromkeys(columnas_modelo))


def calcular_indice_corte_temporal(  # noqa: D417
    numero_observaciones: int,
    proporcion_test: float,
) -> int:
    """Calcula la posición del corte temporal train/test.

    Parameters
    ----------
    numero_observaciones : int
    proporcion_test : float — (0, 1)

    Returns
    -------
    int
        Índice de corte; se conserva al menos 1 observación por lado.
    """
    if numero_observaciones < 2:
        msg = (
            'Se requieren al menos dos observaciones para la división '
            'temporal.'
        )
        raise ValueError(
            msg
        )

    if not 0 < proporcion_test < 1:
        msg = 'proporcion_test debe estar entre 0 y 1.'
        raise ValueError(msg)

    numero_test = max(1, int(np.ceil(numero_observaciones * proporcion_test)))
    numero_test = min(numero_test, numero_observaciones - 1)

    return numero_observaciones - numero_test


# 9. PREPARACIÓN DE ENTRENAMIENTO POR EAN
# =========================================================================
def preparar_datos_entrenamiento(  # noqa: D417
    historial_ean: pd.DataFrame,
    regimen: RegimenPromocional,
    columnas_familia: ColumnasFamiliaPromocional,
    configuracion: ConfiguracionModeloPromo,
) -> DatosEntrenamiento:
    """Prepara matrices de entrenamiento y test para un EAN.

    Las variables TENDENCIA_LINEAL, SIN_ANUAL, COS_ANUAL, FLAG_FERIADO y
    FLAG_PRE_FERIADO deben existir previamente en historial_ean.

    Parameters
    ----------
    historial_ean : pd.DataFrame
    regimen : RegimenPromocional
    columnas_familia : ColumnasFamiliaPromocional
    configuracion : ConfiguracionModeloPromo

    Returns
    -------
    DatosEntrenamiento
    """
    columnas_requeridas = [
        configuracion.columna_ean,
        configuracion.columna_fecha,
        configuracion.columna_target,
        *COLUMNAS_TRANSVERSALES,
    ]

    if configuracion.incluir_precio:
        columnas_requeridas.append(configuracion.columna_precio)

    if regimen.usar_flag_promocion:
        columnas_requeridas.append(columnas_familia.flag_promocion)

    if regimen.usar_porcentaje_descuento:
        columnas_requeridas.append(columnas_familia.porcentaje_descuento)

    if regimen.usar_mecanica:
        columnas_requeridas.extend(
            [columnas_familia.flag_promocion, columnas_familia.descripcion_evento]
        )

    columnas_requeridas = list(dict.fromkeys(columnas_requeridas))
    columnas_faltantes = set(columnas_requeridas).difference(
        historial_ean.columns
    )

    if columnas_faltantes:
        msg = f'Faltan columnas para el entrenamiento: {sorted(columnas_faltantes)}'
        raise ValueError(
            msg
        )

    datos_modelo = (
        historial_ean.sort_values(configuracion.columna_fecha, kind='stable')
        .reset_index(drop=True)
        .copy()
    )

    datos_modelo[configuracion.columna_fecha] = pd.to_datetime(
        datos_modelo[configuracion.columna_fecha], errors='coerce'
    )

    fechas_invalidas = datos_modelo[configuracion.columna_fecha].isna()

    if fechas_invalidas.any():
        msg_0 = (
            f'La columna {configuracion.columna_fecha!r} contiene '
            f'{int(fechas_invalidas.sum())} fecha(s) inválida(s).'
        )
        raise ValueError(
            msg_0
        )

    if configuracion.incluir_dia_semana:
        datos_modelo = agregar_dummies_dia_semana(
            historial=datos_modelo,
            columna_ean=configuracion.columna_ean,
            columna_fecha=configuracion.columna_fecha,
        )

    mecanicas_validas: list[str] = []
    columnas_mecanica: list[str] = []
    usar_mecanica = regimen.usar_mecanica

    if usar_mecanica:
        mecanicas_validas = obtener_mecanicas_validas(
            historial_ean=datos_modelo,
            columna_mecanica=columnas_familia.descripcion_evento,
            columna_flag_promocion=columnas_familia.flag_promocion,
            configuracion=configuracion,
        )

        if mecanicas_validas:
            datos_modelo, columnas_mecanica = agregar_variables_mecanica(
                datos_ean=datos_modelo,
                columna_mecanica=columnas_familia.descripcion_evento,
                columna_flag_promocion=columnas_familia.flag_promocion,
                mecanicas_validas=mecanicas_validas,
            )
            comentario_mecanica = (
                'Mecánica incluida con soporte histórico suficiente'
            )
        else:
            usar_mecanica = False
            comentario_mecanica = (
                'Mecánica excluida por falta de categorías u '
                'observaciones suficientes'
            )
    else:
        comentario_mecanica = 'Mecánica no requerida para el régimen'

    columnas_modelo = seleccionar_columnas_modelo(
        regimen=regimen,
        columnas_familia=columnas_familia,
        configuracion=configuracion,
        columnas_mecanica=columnas_mecanica if usar_mecanica else [],
    )

    columnas_modelo_faltantes = set(columnas_modelo).difference(
        datos_modelo.columns
    )

    if columnas_modelo_faltantes:
        msg_1 = (
            'No fue posible construir las siguientes variables del '
            f'modelo: {sorted(columnas_modelo_faltantes)}'
        )
        raise ValueError(
            msg_1
        )

    matriz_variables = datos_modelo.loc[:, columnas_modelo].copy()

    for columna in columnas_modelo:
        matriz_variables[columna] = pd.to_numeric(
            matriz_variables[columna], errors='coerce'
        )

    matriz_variables = matriz_variables.astype('float32')

    objetivo = pd.to_numeric(
        datos_modelo[configuracion.columna_target], errors='coerce'
    ).astype('float32')

    fechas = datos_modelo[configuracion.columna_fecha].copy()
    mascara_objetivo_valido = objetivo.notna()

    matriz_variables = matriz_variables.loc[mascara_objetivo_valido].reset_index(
        drop=True
    )
    objetivo = objetivo.loc[mascara_objetivo_valido].reset_index(drop=True)
    fechas = fechas.loc[mascara_objetivo_valido].reset_index(drop=True)

    numero_observaciones = len(objetivo)

    if numero_observaciones < 2:
        msg_2 = 'No existen observaciones suficientes para la división temporal.'
        raise ValueError(
            msg_2
        )

    indice_corte = calcular_indice_corte_temporal(
        numero_observaciones=numero_observaciones,
        proporcion_test=configuracion.proporcion_test,
    )

    X_train = matriz_variables.iloc[:indice_corte].reset_index(drop=True)  # noqa: N806
    X_test = matriz_variables.iloc[indice_corte:].reset_index(drop=True)  # noqa: N806
    y_train = objetivo.iloc[:indice_corte].reset_index(drop=True)
    y_test = objetivo.iloc[indice_corte:].reset_index(drop=True)
    fechas_train = fechas.iloc[:indice_corte].reset_index(drop=True)
    fechas_test = fechas.iloc[indice_corte:].reset_index(drop=True)

    pesos_train = calcular_pesos_recencia(
        fechas_train=fechas_train, configuracion=configuracion
    )

    return DatosEntrenamiento(
        X_train=X_train,
        y_train=y_train,
        X_test=X_test,
        y_test=y_test,
        pesos_train=pesos_train,
        fechas_train=fechas_train,
        fechas_test=fechas_test,
        columnas_modelo=columnas_modelo,
        columnas_mecanica=columnas_mecanica if usar_mecanica else [],
        mecanicas_validas=mecanicas_validas if usar_mecanica else [],
        fecha_inicio_train=fechas_train.min(),
        fecha_fin_train=fechas_train.max(),
        fecha_inicio_test=fechas_test.min() if not fechas_test.empty else None,
        fecha_fin_test=fechas_test.max() if not fechas_test.empty else None,
        numero_observaciones=numero_observaciones,
        numero_observaciones_train=len(y_train),
        numero_observaciones_test=len(y_test),
        usar_mecanica=usar_mecanica,
        comentario_mecanica=comentario_mecanica,
    )


# =========================================================================
# 10. MÉTRICAS Y BENCHMARK
# =========================================================================
def calcular_metricas_modelo(  # noqa: D417
    valores_reales: pd.Series | np.ndarray,
    valores_predichos: np.ndarray,
    configuracion: ConfiguracionModeloPromo,
    benchmark: np.ndarray | None = None,
) -> MetricasModelo:
    """Calcula WMAPE, Bias, MAE, Coverage y Win Rate.

    Parameters
    ----------
    valores_reales : pd.Series | np.ndarray
    valores_predichos : np.ndarray
    configuracion : ConfiguracionModeloPromo
        Usa tolerancia_cobertura_absoluta/relativa.
    benchmark : np.ndarray | None
        Predicción alternativa para calcular win_rate (opcional).

    Returns
    -------
    MetricasModelo
    """
    reales = np.asarray(valores_reales, dtype='float64')
    predichos = np.asarray(valores_predichos, dtype='float64')

    mascara_valida = np.isfinite(reales) & np.isfinite(predichos)
    reales = reales[mascara_valida]
    predichos = predichos[mascara_valida]

    if reales.size == 0:
        return MetricasModelo(
            wmape=np.nan, bias=np.nan, mae=np.nan, coverage=np.nan,
            win_rate=None, suma_real=0.0, suma_predicha=0.0,
            error_absoluto_total=0.0, numero_observaciones=0,
            numero_predicciones_validas=0,
        )

    errores = predichos - reales
    errores_absolutos = np.abs(errores)

    suma_real = float(reales.sum())
    suma_predicha = float(predichos.sum())
    error_absoluto_total = float(errores_absolutos.sum())

    if suma_real > 0:
        wmape = error_absoluto_total / suma_real
        bias = float(errores.sum()) / suma_real
    else:
        wmape = np.nan
        bias = np.nan

    mae = float(errores_absolutos.mean())

    tolerancia = np.maximum(
        configuracion.tolerancia_cobertura_absoluta,
        configuracion.tolerancia_cobertura_relativa * np.abs(reales),
    )
    coverage = float(np.mean(errores_absolutos <= tolerancia))

    win_rate = None

    if benchmark is not None:
        benchmark_array = np.asarray(benchmark, dtype='float64')[mascara_valida]
        mascara_benchmark = np.isfinite(benchmark_array)

        if mascara_benchmark.any():
            error_modelo = errores_absolutos[mascara_benchmark]
            error_benchmark = np.abs(
                benchmark_array[mascara_benchmark] - reales[mascara_benchmark]
            )
            win_rate = float(np.mean(error_modelo < error_benchmark))

    return MetricasModelo(
        wmape=float(wmape), bias=float(bias), mae=mae, coverage=coverage,
        win_rate=win_rate, suma_real=suma_real, suma_predicha=suma_predicha,
        error_absoluto_total=error_absoluto_total,
        numero_observaciones=int(reales.size),
        numero_predicciones_validas=int(reales.size),
    )


def construir_benchmark_dia_semana(  # noqa: D417
    fechas_train: pd.Series,
    target_train: pd.Series,
    fechas_objetivo: pd.Series,
) -> np.ndarray:
    """Construye un benchmark ingenuo: promedio histórico por día de semana

    Parameters
    ----------
    fechas_train : pd.Series
    target_train : pd.Series
    fechas_objetivo : pd.Series

    Returns
    -------
    np.ndarray
        Un valor de benchmark por cada fecha en fechas_objetivo. Si un
        día de semana no tiene representación en train, usa el promedio
        global.
    """
    tabla_benchmark = pd.DataFrame({
        'P_DATE': pd.to_datetime(fechas_train, errors='raise').to_numpy(),
        'TARGET': np.asarray(target_train, dtype='float64'),
    })

    tabla_benchmark['DIA_SEMANA'] = tabla_benchmark['P_DATE'].dt.dayofweek
    promedio_global = float(tabla_benchmark['TARGET'].mean())

    promedio_por_dia = (
        tabla_benchmark.groupby('DIA_SEMANA', observed=True)['TARGET'].mean()
    )

    dias_objetivo = pd.to_datetime(fechas_objetivo, errors='raise').dt.dayofweek

    return (
        dias_objetivo.map(promedio_por_dia)
        .fillna(promedio_global)
        .to_numpy(dtype='float64')
    )


# =========================================================================
# 11. CONSTRUCCIÓN Y ENTRENAMIENTO DEL MODELO
# =========================================================================
def construir_modelo_hist_gradient_boosting(  # noqa: D417
    regimen: RegimenPromocional,
    configuracion: ConfiguracionModeloPromo,
) -> HistGradientBoostingRegressor:
    """Construye el estimador con configuración regular o conservadora.

    Parameters
    ----------
    regimen : RegimenPromocional
    configuracion : ConfiguracionModeloPromo

    Returns
    -------
    HistGradientBoostingRegressor
    """
    if regimen.usar_configuracion_regularizada:
        max_leaf_nodes = configuracion.max_leaf_nodes_regularizado
        min_samples_leaf = configuracion.min_samples_leaf_regularizado
        l2_regularization = configuracion.l2_regularization_regularizado
    else:
        max_leaf_nodes = configuracion.max_leaf_nodes
        min_samples_leaf = configuracion.min_samples_leaf
        l2_regularization = configuracion.l2_regularization

    return HistGradientBoostingRegressor(
        loss=configuracion.loss,
        learning_rate=configuracion.learning_rate,
        max_iter=configuracion.max_iter,
        max_leaf_nodes=max_leaf_nodes,
        max_depth=configuracion.max_depth,
        min_samples_leaf=min_samples_leaf,
        l2_regularization=l2_regularization,
        max_bins=configuracion.max_bins,
        early_stopping=configuracion.early_stopping,
        validation_fraction=configuracion.validation_fraction,
        n_iter_no_change=configuracion.n_iter_no_change,
        tol=configuracion.tol,
        random_state=configuracion.random_state,
    )


def entrenar_modelo_producto(  # noqa: D417
    datos_entrenamiento: DatosEntrenamiento,
    regimen: RegimenPromocional,
    configuracion: ConfiguracionModeloPromo,
) -> ResultadoEntrenamiento:
    """Ejecuta el entrenamiento de validación y el productivo.

    El modelo de validación se ajusta solo con train (para métricas); el
    productivo se reajusta desde cero con train + test y es el único
    usado en la proyección.

    Parameters
    ----------
    datos_entrenamiento : DatosEntrenamiento
    regimen : RegimenPromocional
    configuracion : ConfiguracionModeloPromo

    Returns
    -------
    ResultadoEntrenamiento
    """
    pesos_train = construir_pesos_temporales(
        datos_entrenamiento.fechas_train, configuracion
    )

    modelo_validacion = construir_modelo_hist_gradient_boosting(
        regimen, configuracion
    )
    modelo_validacion.fit(
        datos_entrenamiento.X_train,
        datos_entrenamiento.y_train,
        sample_weight=pesos_train,
    )

    predicciones_train = np.clip(
        modelo_validacion.predict(datos_entrenamiento.X_train), 0.0, None
    )
    predicciones_test = np.clip(
        modelo_validacion.predict(datos_entrenamiento.X_test), 0.0, None
    )

    benchmark_test = construir_benchmark_dia_semana(
        fechas_train=datos_entrenamiento.fechas_train,
        target_train=datos_entrenamiento.y_train,
        fechas_objetivo=datos_entrenamiento.fechas_test,
    )

    metricas_train = calcular_metricas_modelo(
        valores_reales=datos_entrenamiento.y_train,
        valores_predichos=predicciones_train,
        configuracion=configuracion,
    )
    metricas_test = calcular_metricas_modelo(
        valores_reales=datos_entrenamiento.y_test,
        valores_predichos=predicciones_test,
        configuracion=configuracion,
        benchmark=benchmark_test,
    )

    variables_completas = pd.concat(
        [datos_entrenamiento.X_train, datos_entrenamiento.X_test],
        axis=0, ignore_index=True,
    )
    target_completo = pd.concat(
        [datos_entrenamiento.y_train, datos_entrenamiento.y_test],
        axis=0, ignore_index=True,
    )
    fechas_completas = pd.concat(
        [datos_entrenamiento.fechas_train, datos_entrenamiento.fechas_test],
        axis=0, ignore_index=True,
    )

    pesos_productivo = construir_pesos_temporales(fechas_completas, configuracion)

    modelo_productivo = construir_modelo_hist_gradient_boosting(
        regimen, configuracion
    )
    modelo_productivo.fit(
        variables_completas, target_completo, sample_weight=pesos_productivo
    )

    parametros_modelo = {
        'loss': configuracion.loss,
        'learning_rate': configuracion.learning_rate,
        'max_iter': configuracion.max_iter,
        'early_stopping': configuracion.early_stopping,
        'validation_fraction': configuracion.validation_fraction,
        'n_iter_no_change': configuracion.n_iter_no_change,
        'tol': configuracion.tol,
        'random_state': configuracion.random_state,
        'usar_configuracion_regularizada': (
            regimen.usar_configuracion_regularizada
        ),
        'usar_pesos_temporales': pesos_productivo is not None,
        'metodo_pesos_temporales': configuracion.metodo_pesos_temporales,
    }

    return ResultadoEntrenamiento(
        modelo_validacion=modelo_validacion,
        modelo_productivo=modelo_productivo,
        metricas_train=metricas_train,
        metricas_test=metricas_test,
        predicciones_train=predicciones_train,
        predicciones_test=predicciones_test,
        benchmark_test=benchmark_test,
        columnas_modelo=datos_entrenamiento.columnas_modelo.copy(),
        parametros_modelo=parametros_modelo,
        iteraciones_validacion=int(modelo_validacion.n_iter_),
        iteraciones_productivo=int(modelo_productivo.n_iter_),
        uso_pesos_temporales=pesos_productivo is not None,
        metodo_pesos_temporales=(
            configuracion.metodo_pesos_temporales
            if pesos_productivo is not None else 'sin_pesos'
        ),
        peso_minimo_train=(
            float(pesos_train.min()) if pesos_train is not None else None
        ),
        peso_maximo_train=(
            float(pesos_train.max()) if pesos_train is not None else None
        ),
        peso_minimo_productivo=(
            float(pesos_productivo.min())
            if pesos_productivo is not None else None
        ),
        peso_maximo_productivo=(
            float(pesos_productivo.max())
            if pesos_productivo is not None else None
        ),
    )


# 12. CALENDARIO Y VARIABLES TEMPORALES FUTURAS
# =========================================================================
def construir_calendario_promocion(promocion_ean: pd.Series) -> pd.DataFrame:
    """Expande una combinación promoción-EAN a una fila por día.

    Parameters
    ----------
    promocion_ean : pd.Series
        Debe incluir FECHA_INICIO_DE_PROMOCION, FECHA_FIN_DE_PROMOCION,
        N_PROMOCION, EAN.

    Returns
    -------
    pd.DataFrame
        Columnas P_DATE, N_PROMOCION, EAN.
    """
    fecha_inicio = pd.to_datetime(
        promocion_ean['FECHA_INICIO_DE_PROMOCION'], errors='raise'
    )
    fecha_fin = pd.to_datetime(
        promocion_ean['FECHA_FIN_DE_PROMOCION'], errors='raise'
    )

    if fecha_fin < fecha_inicio:
        msg = (
            'FECHA_FIN_DE_PROMOCION no puede ser anterior a '
            'FECHA_INICIO_DE_PROMOCION.'
        )
        raise ValueError(
            msg
        )

    calendario = pd.DataFrame({
        'P_DATE': pd.date_range(start=fecha_inicio, end=fecha_fin, freq='D')
    })
    calendario['N_PROMOCION'] = promocion_ean['N_PROMOCION']
    calendario['EAN'] = str(promocion_ean['EAN'])

    return calendario


def agregar_variables_temporales_futuras(  # noqa: D417
    calendario: pd.DataFrame,
    historial_ean: pd.DataFrame,
    calendario_futuro: pd.DataFrame | None = None,
) -> tuple[pd.DataFrame, bool]:
    """Construye variables temporales reproducibles en fechas futuras.

    TENDENCIA_LINEAL continúa desde el origen del histórico; SIN_ANUAL y
    COS_ANUAL usan periodicidad anual. Los feriados se toman de
    calendario_futuro si está disponible; si no, se asignan en cero.

    Parameters
    ----------
    calendario : pd.DataFrame
    historial_ean : pd.DataFrame
    calendario_futuro : pd.DataFrame | None
        Debe incluir P_DATE, FLAG_FERIADO, FLAG_PRE_FERIADO.

    Returns
    -------
    tuple[pd.DataFrame, bool]
        Calendario enriquecido y flag de calendario incompleto.
    """
    resultado = calendario.copy()

    fecha_origen = pd.to_datetime(
        historial_ean['P_DATE'], errors='raise'
    ).min()
    dias_desde_origen = (resultado['P_DATE'] - fecha_origen).dt.days
    resultado['TENDENCIA_LINEAL'] = dias_desde_origen.astype('int32')

    dia_anio = resultado['P_DATE'].dt.dayofyear
    resultado['SIN_ANUAL'] = np.sin(2.0 * np.pi * dia_anio / 365.25)
    resultado['COS_ANUAL'] = np.cos(2.0 * np.pi * dia_anio / 365.25)

    calendario_incompleto = False

    if calendario_futuro is not None:
        columnas_calendario = {'P_DATE', 'FLAG_FERIADO', 'FLAG_PRE_FERIADO'}
        faltantes = columnas_calendario.difference(calendario_futuro.columns)

        if faltantes:
            msg = f'Faltan columnas en calendario_futuro: {sorted(faltantes)}'
            raise KeyError(
                msg
            )

        calendario_auxiliar = calendario_futuro[
            ['P_DATE', 'FLAG_FERIADO', 'FLAG_PRE_FERIADO']
        ].copy()
        calendario_auxiliar['P_DATE'] = pd.to_datetime(
            calendario_auxiliar['P_DATE'], errors='raise'
        )
        calendario_auxiliar = calendario_auxiliar.drop_duplicates(
            'P_DATE', keep='last'
        )

        resultado = resultado.merge(
            calendario_auxiliar, on='P_DATE', how='left', validate='one_to_one'
        )

        columnas_flag = ['FLAG_FERIADO', 'FLAG_PRE_FERIADO']
        resultado[columnas_flag] = (
            resultado[columnas_flag].fillna(0).astype('int8')
        )

        calendario_incompleto = bool(
            resultado['P_DATE'].isin(calendario_auxiliar['P_DATE']).eq(False).any()  # noqa: FBT003
        )
    else:
        resultado['FLAG_FERIADO'] = np.int8(0)
        resultado['FLAG_PRE_FERIADO'] = np.int8(0)
        calendario_incompleto = True

    dia_semana = resultado['P_DATE'].dt.dayofweek
    mapa_dias = {
        'dow_martes': 1, 'dow_miercoles': 2, 'dow_jueves': 3,
        'dow_viernes': 4, 'dow_sabado': 5, 'dow_domingo': 6,
    }

    for columna, numero_dia in mapa_dias.items():
        resultado[columna] = dia_semana.eq(numero_dia).astype('int8')

    return resultado, calendario_incompleto


def incorporar_datos_observados(  # noqa: D417
    calendario: pd.DataFrame,
    historial_ean: pd.DataFrame,
    configuracion: ConfiguracionModeloPromo,
) -> pd.DataFrame:
    """Incorpora el target real cuando la promoción ya comenzó.

    Clasifica cada día en OBSERVADO, PROYECTADO (futuro) o
    PASADO_SIN_REGISTRO.

    Parameters
    ----------
    calendario : pd.DataFrame
    historial_ean : pd.DataFrame
    configuracion : ConfiguracionModeloPromo
        Usa tratar_pasado_sin_registro_como_cero.

    Returns
    -------
    pd.DataFrame
    """
    resultado = calendario.copy()

    historial_target = historial_ean[['P_DATE', configuracion.columna_target]].copy()
    historial_target['P_DATE'] = pd.to_datetime(
        historial_target['P_DATE'], errors='raise'
    )
    historial_target = historial_target.drop_duplicates(
        'P_DATE', keep='last'
    ).rename(columns={configuracion.columna_target: 'CANTIDAD_OBSERVADA'})

    resultado = resultado.merge(
        historial_target, on='P_DATE', how='left', validate='one_to_one'
    )

    ultima_fecha_disponible = pd.to_datetime(
        historial_ean['P_DATE'], errors='raise'
    ).max()

    mascara_observada = resultado['CANTIDAD_OBSERVADA'].notna()
    mascara_futura = resultado['P_DATE'] > ultima_fecha_disponible
    mascara_pasado_sin_registro = ~mascara_observada & ~mascara_futura

    resultado['ORIGEN_CANTIDAD'] = np.select(
        [mascara_observada, mascara_futura, mascara_pasado_sin_registro],
        ['OBSERVADO', 'PROYECTADO', 'PASADO_SIN_REGISTRO'],
        default='PROYECTADO',
    )

    resultado['FLAG_DATO_REAL'] = mascara_observada.astype('int8')
    resultado['FLAG_DIA_FUTURO'] = mascara_futura.astype('int8')
    resultado['FLAG_DIA_PASADO_SIN_DATO'] = mascara_pasado_sin_registro.astype(
        'int8'
    )

    if configuracion.tratar_pasado_sin_registro_como_cero:
        resultado.loc[mascara_pasado_sin_registro, 'CANTIDAD_OBSERVADA'] = 0.0

    return resultado


def asignar_mecanicas_escenario(  # noqa: D417
    escenario: pd.DataFrame,
    columnas_modelo: list[str],
    nombre_mecanica: object,
    activar_mecanica: bool,
) -> pd.DataFrame:
    """Activa la dummy de mecánica correspondiente al escenario.

    Parameters
    ----------
    escenario : pd.DataFrame
    columnas_modelo : list[str]
    nombre_mecanica : object
    activar_mecanica : bool

    Returns
    -------
    pd.DataFrame
        El baseline mantiene todas las dummies de mecánica en cero.
    """
    resultado = escenario.copy()

    columnas_mecanica = [
        columna for columna in columnas_modelo
        if columna.startswith('MECANICA_')
    ]

    for columna in columnas_mecanica:
        resultado[columna] = np.int8(0)

    if not activar_mecanica or pd.isna(nombre_mecanica):
        return resultado

    mecanica_normalizada = str(nombre_mecanica).strip().upper().replace(' ', '_')
    columna_objetivo = f'MECANICA_{mecanica_normalizada}'

    if columna_objetivo in columnas_mecanica:
        resultado[columna_objetivo] = np.int8(1)
    elif 'MECANICA_OTRA' in columnas_mecanica:
        resultado['MECANICA_OTRA'] = np.int8(1)

    return resultado


# =========================================================================
# 13. CONSTRUCCIÓN DE ESCENARIOS Y PREDICCIÓN
# =========================================================================
def construir_escenarios_promocional_baseline(  # noqa: D417
    promocion_ean: pd.Series,
    historial_ean: pd.DataFrame,
    resultado_entrenamiento: ResultadoEntrenamiento,
    regimen: RegimenPromocional,
    columnas_familia: ColumnasFamiliaPromocional,
    configuracion: ConfiguracionModeloPromo,
    calendario_futuro: pd.DataFrame | None = None,
) -> tuple[pd.DataFrame, pd.DataFrame, bool]:
    """Construye los escenarios diarios promocional y baseline.

    El escenario promocional usa PRECIO_PROMOCIONAL y activa las
    variables autorizadas por el régimen; el baseline usa PRECIO_MODAL y
    desactiva promoción, descuento y mecánica.

    Parameters
    ----------
    promocion_ean : pd.Series
    historial_ean : pd.DataFrame
    resultado_entrenamiento : ResultadoEntrenamiento
    regimen : RegimenPromocional
    columnas_familia : ColumnasFamiliaPromocional
    configuracion : ConfiguracionModeloPromo
    calendario_futuro : pd.DataFrame | None

    Returns
    -------
    tuple[pd.DataFrame, pd.DataFrame, bool]
        (escenario_promocional, escenario_baseline, calendario_incompleto)
    """
    calendario = construir_calendario_promocion(promocion_ean)

    calendario, calendario_incompleto = agregar_variables_temporales_futuras(
        calendario=calendario,
        historial_ean=historial_ean,
        calendario_futuro=calendario_futuro,
    )

    calendario = incorporar_datos_observados(
        calendario=calendario, historial_ean=historial_ean,
        configuracion=configuracion,
    )

    precio_promocional = pd.to_numeric(
        promocion_ean['PRECIO_PROMOCIONAL'], errors='coerce'
    )
    precio_modal = pd.to_numeric(promocion_ean['PRECIO_MODAL'], errors='coerce')
    porcentaje_descuento = pd.to_numeric(
        promocion_ean['PORCENTAJE_DESCUENTO'], errors='coerce'
    )

    if pd.isna(precio_promocional) or precio_promocional <= 0:
        msg = 'PRECIO_PROMOCIONAL debe ser mayor que cero.'
        raise ValueError(msg)

    if pd.isna(precio_modal) or precio_modal <= 0:
        msg = 'PRECIO_MODAL debe ser mayor que cero.'
        raise ValueError(msg)

    porcentaje_descuento = max(
        float(porcentaje_descuento) if pd.notna(porcentaje_descuento) else 0.0,
        0.0,
    )

    columnas_modelo = resultado_entrenamiento.columnas_modelo

    escenario_promocional = calendario.copy()
    escenario_promocional[configuracion.columna_precio] = float(precio_promocional)

    if columnas_familia.flag_promocion in columnas_modelo:
        escenario_promocional[columnas_familia.flag_promocion] = np.int8(1)

    if columnas_familia.porcentaje_descuento in columnas_modelo:
        escenario_promocional[columnas_familia.porcentaje_descuento] = (
            porcentaje_descuento
        )

    nombre_mecanica = promocion_ean.get('DESCRIPCION_EVENTO_PROMOCIONAL', pd.NA)

    escenario_promocional = asignar_mecanicas_escenario(
        escenario=escenario_promocional,
        columnas_modelo=columnas_modelo,
        nombre_mecanica=nombre_mecanica,
        activar_mecanica=regimen.usar_mecanica,
    )
    escenario_promocional['ESCENARIO'] = 'PROMOCIONAL'
    escenario_promocional['PRECIO_ESCENARIO'] = float(precio_promocional)

    escenario_baseline = calendario.copy()
    escenario_baseline[configuracion.columna_precio] = float(precio_modal)

    if columnas_familia.flag_promocion in columnas_modelo:
        escenario_baseline[columnas_familia.flag_promocion] = np.int8(0)

    if columnas_familia.porcentaje_descuento in columnas_modelo:
        escenario_baseline[columnas_familia.porcentaje_descuento] = 0.0

    escenario_baseline = asignar_mecanicas_escenario(
        escenario=escenario_baseline,
        columnas_modelo=columnas_modelo,
        nombre_mecanica=pd.NA,
        activar_mecanica=False,
    )
    escenario_baseline['ESCENARIO'] = 'BASELINE'
    escenario_baseline['PRECIO_ESCENARIO'] = float(precio_modal)

    for columna in columnas_modelo:
        if columna not in escenario_promocional.columns:
            escenario_promocional[columna] = 0.0

        if columna not in escenario_baseline.columns:
            escenario_baseline[columna] = 0.0

    return escenario_promocional, escenario_baseline, calendario_incompleto


def predecir_escenario(  # noqa: D417
    escenario: pd.DataFrame,
    resultado_entrenamiento: ResultadoEntrenamiento,
    usar_observados_en_total: bool,
    configuracion: ConfiguracionModeloPromo,
) -> pd.DataFrame:
    """Predice un escenario con el modelo productivo.

    En el promocional, los días observados usan el target real en
    CANTIDAD_FINAL; el baseline nunca se reemplaza (es contrafactual).

    Parameters
    ----------
    escenario : pd.DataFrame
    resultado_entrenamiento : ResultadoEntrenamiento
    usar_observados_en_total : bool
    configuracion : ConfiguracionModeloPromo

    Returns
    -------
    pd.DataFrame
        Con columnas CANTIDAD_MODELO y CANTIDAD_FINAL agregadas.
    """
    resultado = escenario.copy()

    matriz_modelo = resultado[
        resultado_entrenamiento.columnas_modelo
    ].astype('float64')

    cantidad_modelo = np.clip(
        resultado_entrenamiento.modelo_productivo.predict(matriz_modelo),
        0.0, None,
    )
    resultado['CANTIDAD_MODELO'] = cantidad_modelo

    if usar_observados_en_total:
        cantidad_final = resultado['CANTIDAD_OBSERVADA'].where(
            resultado['FLAG_DATO_REAL'].eq(1), resultado['CANTIDAD_MODELO']
        )

        if configuracion.tratar_pasado_sin_registro_como_cero:
            cantidad_final = cantidad_final.where(
                resultado['FLAG_DIA_PASADO_SIN_DATO'].eq(0), 0.0
            )

        resultado['CANTIDAD_FINAL'] = cantidad_final
    else:
        resultado['CANTIDAD_FINAL'] = resultado['CANTIDAD_MODELO']

    return resultado


# 14. UTILIDADES DE ACCESO SEGURO
# =========================================================================
def obtener_valor_caracterizacion(  # noqa: D417
    caracterizacion_ean: pd.Series, columna: str
) -> object:
    """Obtiene un valor ya calculado desde caracterización (sin recalcular)

    Parameters
    ----------
    caracterizacion_ean : pd.Series
    columna : str

    Returns
    -------
    object
        pd.NA si la columna no existe.
    """
    if columna not in caracterizacion_ean.index:
        return pd.NA

    return caracterizacion_ean[columna]


def obtener_valor(  # noqa: D417
    registro: pd.Series | Mapping | None,
    columna: str,
    valor_default: object = '-',
) -> object:
    """Obtiene un valor desde una Series, dict o Mapping de forma segura.

    Parameters
    ----------
    registro : pd.Series | Mapping | None
    columna : str
    valor_default : object

    Returns
    -------
    object
        valor_default si el atributo no existe o es nulo.
    """
    if registro is None:
        return valor_default

    if isinstance(registro, pd.Series):
        valor = registro[columna] if columna in registro.index else valor_default
    elif isinstance(registro, Mapping):
        valor = registro.get(columna, valor_default)
    else:
        msg = (
            'registro debe ser pd.Series, Mapping o None. '
            f'Tipo recibido: {type(registro).__name__}.'
        )
        raise TypeError(
            msg
        )

    if valor is None:
        return valor_default

    if isinstance(valor, list | tuple | dict | set | np.ndarray):
        return valor

    if pd.isna(valor):
        return valor_default

    return valor


def redondear_total(valor: float | int | object, decimales: int = 2) -> float | str:  # noqa: D417, PYI041
    """Redondea un total numérico para la salida final.

    Parameters
    ----------
    valor : float | int | object
    decimales : int

    Returns
    -------
    float | str
        '-' si el valor no es numérico.
    """
    valor_numerico = pd.to_numeric(valor, errors='coerce')

    if pd.isna(valor_numerico):
        return '-'

    return round(float(valor_numerico), decimales)


def obtener_motivo_elegibilidad(elegibilidad: object) -> str:  # noqa: D417
    """Obtiene el primer motivo textual disponible en un objeto de
    elegibilidad, sin asumir un nombre fijo de atributo.

    Parameters
    ----------
    elegibilidad : object

    Returns
    -------
    str
    """
    atributos = vars(elegibilidad)

    for nombre_atributo in ('motivo', 'comentario', 'razon', 'mensaje'):
        valor = atributos.get(nombre_atributo)

        if valor is not None and str(valor).strip():
            return str(valor)

    return 'EAN no elegible o sin régimen de entrenamiento asignado.'


def obtener_metadata_ean(  # noqa: D417
    caracterizacion_ean: pd.Series | None,
) -> dict[str, object]:
    """Obtiene metadata descriptiva desde caracterización.

    Disponible aunque el EAN no tenga historial, no sea elegible o no
    pueda proyectarse.

    Parameters
    ----------
    caracterizacion_ean : pd.Series | None

    Returns
    -------
    dict[str, object]
    """
    return {
        'Categoría': obtener_valor(caracterizacion_ean, 'CATEGORY_DESCRIPTION'),
        'Subcategoría': obtener_valor(
            caracterizacion_ean, 'SUB_CATEGORY_DESCRIPTION'
        ),
        'Descripcion': obtener_valor(caracterizacion_ean, 'PRODUCT_DESCRIPTION'),
        'Material': obtener_valor(caracterizacion_ean, 'MATERIAL'),
        'UMV': obtener_valor(caracterizacion_ean, 'SALES_UOM'),
        'TIPOLOGIA_DEMANDA': obtener_valor(
            caracterizacion_ean, 'TIPOLOGIA_DEMANDA'
        ),
        'SEGMENTO_ABCD': obtener_valor(caracterizacion_ean, 'SEGMENTO_ABCD'),
        'ADI': obtener_valor(caracterizacion_ean, 'ADI'),
        'CV2': obtener_valor(caracterizacion_ean, 'CV2'),
        'Intensidad Promocional': obtener_valor(
            caracterizacion_ean, 'INTENSIDAD_PROMOCIONAL'
        ),
        'DIAS_CON_VENTA': obtener_valor(caracterizacion_ean, 'DIAS_CON_VENTA'),
    }


# =========================================================================
# 15. RESUMEN Y CONSOLIDACIÓN DE ESCENARIOS
# =========================================================================
def construir_resumen_escenarios(  # noqa: D417
    detalle_promocional: pd.DataFrame,
    detalle_baseline: pd.DataFrame,
    promocion_ean: pd.Series,
    caracterizacion_ean: pd.Series,
    resultado_entrenamiento: ResultadoEntrenamiento,
    regimen: RegimenPromocional,
    calendario_incompleto: bool,
    configuracion: ConfiguracionModeloPromo,
) -> dict[str, object]:
    """Construye el resultado agregado de una combinación promoción-EAN.

    Parameters
    ----------
    detalle_promocional : pd.DataFrame
    detalle_baseline : pd.DataFrame
    promocion_ean : pd.Series
    caracterizacion_ean : pd.Series
    resultado_entrenamiento : ResultadoEntrenamiento
    regimen : RegimenPromocional
    calendario_incompleto : bool
    configuracion : ConfiguracionModeloPromo

    Returns
    -------
    dict[str, object]
    """
    total_promocional = int(
        np.rint(float(detalle_promocional['CANTIDAD_FINAL'].sum()))
    )
    total_baseline = int(
        np.rint(float(detalle_baseline['CANTIDAD_FINAL'].sum()))
    )
    unidades_incrementales = total_promocional - total_baseline

    uplift_porcentual = (
        unidades_incrementales / total_baseline if total_baseline > 0 else np.nan
    )

    fecha_actual = pd.Timestamp.now().normalize()
    fecha_inicio = pd.to_datetime(promocion_ean['FECHA_INICIO_DE_PROMOCION'])
    fecha_fin = pd.to_datetime(promocion_ean['FECHA_FIN_DE_PROMOCION'])

    contiene_datos_reales = bool(detalle_promocional['FLAG_DATO_REAL'].eq(1).any())
    contiene_dias_futuros = bool(
        detalle_promocional['FLAG_DIA_FUTURO'].eq(1).any()
    )
    contiene_pasados_sin_dato = bool(
        detalle_promocional['FLAG_DIA_PASADO_SIN_DATO'].eq(1).any()
    )

    comentarios = []

    if not regimen.baseline_claro:
        comentarios.append(regimen.comentario_baseline)

    if contiene_pasados_sin_dato:
        comentarios.append('Existen días pasados sin registro histórico')

    if calendario_incompleto:
        comentarios.append(
            'Calendario futuro sin cobertura completa de feriados'
        )

    comentario = '; '.join(comentarios) if comentarios else 'Proyección generada'

    return {
        'N_PROMOCION': promocion_ean['N_PROMOCION'],
        'EAN': str(promocion_ean['EAN']),
        'FECHA_INICIO_DE_PROMOCION': fecha_inicio,
        'FECHA_FIN_DE_PROMOCION': fecha_fin,
        'CASO_MODELO': regimen.nombre,
        'TIPO_BASELINE': regimen.tipo_baseline,
        'CONFIANZA_BASELINE': regimen.confianza_baseline,
        'ADI': obtener_valor_caracterizacion(
            caracterizacion_ean, configuracion.columna_adi
        ),
        'CV2': obtener_valor_caracterizacion(
            caracterizacion_ean, configuracion.columna_cv2
        ),
        'CLASIFICACION_ADI_CV2': obtener_valor_caracterizacion(
            caracterizacion_ean, configuracion.columna_clasificacion_adi_cv2
        ),
        'CANTIDAD_PROMOCIONAL_TOTAL': total_promocional,
        'CANTIDAD_BASELINE_TOTAL': total_baseline,
        'UNIDADES_INCREMENTALES': unidades_incrementales,
        'UPLIFT_PORCENTUAL': uplift_porcentual,
        'WMAPE_TRAIN': resultado_entrenamiento.metricas_train.wmape,
        'BIAS_TRAIN': resultado_entrenamiento.metricas_train.bias,
        'MAE_TRAIN': resultado_entrenamiento.metricas_train.mae,
        'COVERAGE_TRAIN': resultado_entrenamiento.metricas_train.coverage,
        'WMAPE_TEST': resultado_entrenamiento.metricas_test.wmape,
        'BIAS_TEST': resultado_entrenamiento.metricas_test.bias,
        'MAE_TEST': resultado_entrenamiento.metricas_test.mae,
        'COVERAGE_TEST': resultado_entrenamiento.metricas_test.coverage,
        'WIN_RATE_TEST': resultado_entrenamiento.metricas_test.win_rate,
        'ITERACIONES_VALIDACION': resultado_entrenamiento.iteraciones_validacion,
        'ITERACIONES_PRODUCTIVO': resultado_entrenamiento.iteraciones_productivo,
        'FLAG_MODELO_VALIDADO': 1,
        'FLAG_MODELO_PRODUCTIVO': 1,
        'FLAG_USA_PESOS': int(resultado_entrenamiento.uso_pesos_temporales),
        'METODO_PESOS_TEMPORALES': resultado_entrenamiento.metodo_pesos_temporales,
        'FLAG_PROMOCION_INICIADA': int(fecha_inicio <= fecha_actual),
        'FLAG_PROMOCION_FINALIZADA': int(fecha_fin < fecha_actual),
        'FLAG_CONTIENE_DATOS_REALES': int(contiene_datos_reales),
        'FLAG_CONTIENE_DIAS_FUTUROS': int(contiene_dias_futuros),
        'FLAG_CONTIENE_PASADOS_SIN_DATO': int(contiene_pasados_sin_dato),
        'FLAG_BASELINE_REFERENCIAL': int(not regimen.baseline_claro),
        'FLAG_CALENDARIO_INCOMPLETO': int(calendario_incompleto),
        'COMENTARIO': comentario,
    }


def proyectar_promocion_ean(  # noqa: D417
    promocion_ean: pd.Series,
    historial_ean: pd.DataFrame,
    caracterizacion_ean: pd.Series,
    resultado_entrenamiento: ResultadoEntrenamiento,
    regimen: RegimenPromocional,
    columnas_familia: ColumnasFamiliaPromocional,
    configuracion: ConfiguracionModeloPromo,
    calendario_futuro: pd.DataFrame | None = None,
) -> ResultadoEscenarios:
    """Construye, predice y consolida los escenarios de una combinación
    promoción-EAN.

    Parameters
    ----------
    promocion_ean : pd.Series
    historial_ean : pd.DataFrame
    caracterizacion_ean : pd.Series
    resultado_entrenamiento : ResultadoEntrenamiento
    regimen : RegimenPromocional
    columnas_familia : ColumnasFamiliaPromocional
    configuracion : ConfiguracionModeloPromo
    calendario_futuro : pd.DataFrame | None

    Returns
    -------
    ResultadoEscenarios
    """
    escenario_promocional, escenario_baseline, calendario_incompleto = (
        construir_escenarios_promocional_baseline(
            promocion_ean=promocion_ean,
            historial_ean=historial_ean,
            resultado_entrenamiento=resultado_entrenamiento,
            regimen=regimen,
            columnas_familia=columnas_familia,
            configuracion=configuracion,
            calendario_futuro=calendario_futuro,
        )
    )

    detalle_promocional = predecir_escenario(
        escenario=escenario_promocional,
        resultado_entrenamiento=resultado_entrenamiento,
        usar_observados_en_total=True,
        configuracion=configuracion,
    )
    detalle_baseline = predecir_escenario(
        escenario=escenario_baseline,
        resultado_entrenamiento=resultado_entrenamiento,
        usar_observados_en_total=False,
        configuracion=configuracion,
    )

    resumen = construir_resumen_escenarios(
        detalle_promocional=detalle_promocional,
        detalle_baseline=detalle_baseline,
        promocion_ean=promocion_ean,
        caracterizacion_ean=caracterizacion_ean,
        resultado_entrenamiento=resultado_entrenamiento,
        regimen=regimen,
        calendario_incompleto=calendario_incompleto,
        configuracion=configuracion,
    )

    columnas_detalle = [
        'N_PROMOCION', 'EAN', 'P_DATE', 'ESCENARIO', 'PRECIO_ESCENARIO',
        'CANTIDAD_OBSERVADA', 'CANTIDAD_MODELO', 'CANTIDAD_FINAL',
        'ORIGEN_CANTIDAD', 'FLAG_DATO_REAL', 'FLAG_DIA_FUTURO',
        'FLAG_DIA_PASADO_SIN_DATO',
    ]

    detalle_diario = pd.concat(
        [detalle_promocional[columnas_detalle], detalle_baseline[columnas_detalle]],
        axis=0, ignore_index=True,
    ).sort_values(['P_DATE', 'ESCENARIO'], kind='stable').reset_index(drop=True)

    return ResultadoEscenarios(detalle_diario=detalle_diario, resumen=resumen)


# =========================================================================
# 16. FILAS DEL EXCEL FINAL
# =========================================================================
def construir_fila_no_proyectable(  # noqa: D417
    promocion_ean: pd.Series,
    caracterizacion_ean: pd.Series | None,
    motivo: str,
) -> dict[str, object]:
    """Construye una fila del Excel final para un EAN que no pudo
    entrenarse o proyectarse, preservando metadatos disponibles.

    Parameters
    ----------
    promocion_ean : pd.Series
    caracterizacion_ean : pd.Series | None
    motivo : str

    Returns
    -------
    dict[str, object]
    """
    metadata_ean = obtener_metadata_ean(caracterizacion_ean=caracterizacion_ean)

    return {
        'N° promoción': promocion_ean['N_PROMOCION'],
        'Nombre promoción': obtener_valor(promocion_ean, 'NOMBRE_PROMOCION'),
        'Intensidad Promocional': metadata_ean['Intensidad Promocional'],
        'WMAPE Train': '-', 'Bias Train': '-', 'MAE Train': '-',
        'Coverage Train': '-', 'WMAPE Test': '-', 'Bias Test': '-',
        'MAE Test': '-', 'Coverage Test': '-', 'Win Rate Test': '-',
        'Categoría': metadata_ean['Categoría'],
        'Subcategoría': metadata_ean['Subcategoría'],
        'Descripcion': metadata_ean['Descripcion'],
        'Material': metadata_ean['Material'],
        'UMV': obtener_valor(promocion_ean, 'UN_MEDIDA_VENTA'),
        'EAN': obtener_valor(promocion_ean, 'EAN'),
        'R²': '-', 'Elasticidad': '-',
        'Estable': obtener_valor(caracterizacion_ean, 'CLASIFICACION_ADI_CV2'),
        'TIPOLOGIA_DEMANDA': metadata_ean['TIPOLOGIA_DEMANDA'],
        'Inicio Proy': obtener_valor(promocion_ean, 'FECHA_INICIO_DE_PROMOCION'),
        'Fin Proy': obtener_valor(promocion_ean, 'FECHA_FIN_DE_PROMOCION'),
        'Precio Modal': obtener_valor(promocion_ean, 'PRECIO_MODAL'),
        'Precio Promocional': obtener_valor(promocion_ean, 'PRECIO_PROMOCIONAL'),
        'Baseline_UV': '-', 'UV Incremental Real': '-',
        'UV Incremental Proy': '-', 'UV Real': '-', 'UV Proy': '-',
        'Baseline Venta': '-', 'Venta Incremental Real': '-',
        'Venta Incremental Proy': '-', 'Venta Real': '-', 'Venta Proy': '-',
        'Estado_Historial': 'No elegible',
        'Estado_Modelo': 'No entrenado',
        'Estado_Elasticidad': '-', 'Estado_fecha_proy': '-',
        'Estado_proyección': 'No se pudo proyectar',
        'Comentario': motivo,
        'SEGMENTO_ABCD': metadata_ean['SEGMENTO_ABCD'],
        'ADI': redondear_total(metadata_ean['ADI'], decimales=4),
        'CV2': redondear_total(metadata_ean['CV2'], decimales=4),
        'DIAS_CON_VENTA': metadata_ean['DIAS_CON_VENTA'],
    }


def construir_fila_excel_final(  # noqa: D417
    resultado_escenarios: ResultadoEscenarios,
    promocion_ean: pd.Series,
    caracterizacion_ean: pd.Series,
) -> dict[str, object]:
    """Construye una fila del Excel final para una combinación
    promoción-EAN proyectada.

    Baseline Venta = Baseline_UV * PRECIO_MODAL; Venta Proy =
    UV Proy * PRECIO_PROMOCIONAL. Los precios provienen directamente de
    promocion_ean, sin recalcular precios efectivos ni ponderados.

    Parameters
    ----------
    resultado_escenarios : ResultadoEscenarios
    promocion_ean : pd.Series
    caracterizacion_ean : pd.Series

    Returns
    -------
    dict[str, object]
    """
    resumen = resultado_escenarios.resumen
    detalle = resultado_escenarios.detalle_diario.copy()

    columnas_detalle_requeridas = {
        'P_DATE', 'ESCENARIO', 'CANTIDAD_FINAL', 'FLAG_DATO_REAL',
    }
    faltantes_detalle = columnas_detalle_requeridas.difference(detalle.columns)

    if faltantes_detalle:
        msg = f'Faltan columnas en detalle_diario: {sorted(faltantes_detalle)}'
        raise KeyError(
            msg
        )

    columnas_promocion_requeridas = {'PRECIO_MODAL', 'PRECIO_PROMOCIONAL'}
    faltantes_promocion = columnas_promocion_requeridas.difference(
        promocion_ean.index
    )

    if faltantes_promocion:
        msg = f'Faltan columnas en promocion_ean: {sorted(faltantes_promocion)}'
        raise KeyError(
            msg
        )

    detalle['P_DATE'] = pd.to_datetime(detalle['P_DATE'], errors='coerce')
    detalle['CANTIDAD_FINAL'] = pd.to_numeric(
        detalle['CANTIDAD_FINAL'], errors='coerce'
    ).fillna(0.0)
    detalle['FLAG_DATO_REAL'] = (
        pd.to_numeric(detalle['FLAG_DATO_REAL'], errors='coerce')
        .fillna(0).astype(np.int8)
    )

    precio_modal = pd.to_numeric(promocion_ean['PRECIO_MODAL'], errors='coerce')
    precio_promocional = pd.to_numeric(
        promocion_ean['PRECIO_PROMOCIONAL'], errors='coerce'
    )

    identificador_promocion = obtener_valor(
        resumen, 'N_PROMOCION',
        valor_default=obtener_valor(promocion_ean, 'N_PROMOCION'),
    )
    identificador_ean = obtener_valor(
        resumen, 'EAN', valor_default=obtener_valor(promocion_ean, 'EAN')
    )

    if pd.isna(precio_modal):
        msg = (
            'PRECIO_MODAL es nulo o inválido para promoción-EAN '
            f'{identificador_promocion}-{identificador_ean}.'
        )
        raise ValueError(
            msg
        )

    if pd.isna(precio_promocional):
        msg = (
            'PRECIO_PROMOCIONAL es nulo o inválido para promoción-EAN '
            f'{identificador_promocion}-{identificador_ean}.'
        )
        raise ValueError(
            msg
        )

    precio_modal = float(precio_modal)
    precio_promocional = float(precio_promocional)

    detalle_promocional = detalle.loc[detalle['ESCENARIO'].eq('PROMOCIONAL')].copy()
    detalle_baseline = detalle.loc[detalle['ESCENARIO'].eq('BASELINE')].copy()

    if detalle_promocional.empty:
        msg = (
            'No existen filas PROMOCIONAL para promoción-EAN '
            f'{identificador_promocion}-{identificador_ean}.'
        )
        raise ValueError(
            msg
        )

    if detalle_baseline.empty:
        msg = (
            'No existen filas BASELINE para promoción-EAN '
            f'{identificador_promocion}-{identificador_ean}.'
        )
        raise ValueError(
            msg
        )

    detalle_promocional_real = detalle_promocional.loc[
        detalle_promocional['FLAG_DATO_REAL'].eq(1)
    ]
    detalle_baseline_real = detalle_baseline.loc[
        detalle_baseline['FLAG_DATO_REAL'].eq(1)
    ]

    uv_proy = detalle_promocional['CANTIDAD_FINAL'].sum()
    baseline_uv = detalle_baseline['CANTIDAD_FINAL'].sum()
    uv_incremental_proy = uv_proy - baseline_uv

    uv_real = detalle_promocional_real['CANTIDAD_FINAL'].sum()
    baseline_uv_real = detalle_baseline_real['CANTIDAD_FINAL'].sum()
    uv_incremental_real = uv_real - baseline_uv_real

    baseline_venta = baseline_uv * precio_modal
    venta_proy = uv_proy * precio_promocional
    venta_incremental_proy = venta_proy - baseline_venta

    venta_real = uv_real * precio_promocional
    baseline_venta_real = baseline_uv_real * precio_modal
    venta_incremental_real = venta_real - baseline_venta_real

    metadata_ean = obtener_metadata_ean(caracterizacion_ean=caracterizacion_ean)

    return {
        'N° promoción': identificador_promocion,
        'Nombre promoción': obtener_valor(promocion_ean, 'NOMBRE_PROMOCION'),
        'Categoría': metadata_ean['Categoría'],
        'Subcategoría': metadata_ean['Subcategoría'],
        'Descripcion': metadata_ean['Descripcion'],
        'Material': metadata_ean['Material'],
        'UMV': metadata_ean['UMV'],
        'EAN': identificador_ean,
        'R²': '-', 'Elasticidad': '-',
        'Estable': obtener_valor(caracterizacion_ean, 'CLASIFICACION_ADI_CV2'),
        'TIPOLOGIA_DEMANDA': metadata_ean['TIPOLOGIA_DEMANDA'],
        'Inicio Proy': obtener_valor(
            resumen, 'FECHA_INICIO_DE_PROMOCION',
            valor_default=obtener_valor(promocion_ean, 'FECHA_INICIO_DE_PROMOCION'),
        ),
        'Fin Proy': obtener_valor(
            resumen, 'FECHA_FIN_DE_PROMOCION',
            valor_default=obtener_valor(promocion_ean, 'FECHA_FIN_DE_PROMOCION'),
        ),
        'Precio Modal': precio_modal,
        'Precio Promocional': precio_promocional,
        'Baseline_UV': redondear_total(baseline_uv),
        'UV Incremental Real': uv_incremental_real,
        'UV Incremental Proy': redondear_total(uv_incremental_proy),
        'UV Real': uv_real,
        'UV Proy': redondear_total(uv_proy),
        'Baseline Venta': redondear_total(baseline_venta),
        'Venta Incremental Real': venta_incremental_real,
        'Venta Incremental Proy': redondear_total(venta_incremental_proy),
        'Venta Real': venta_real,
        'Venta Proy': redondear_total(venta_proy),
        'Estado_Historial': obtener_valor(resumen, 'FLAG_BASELINE_REFERENCIAL'),
        'Estado_Modelo': obtener_valor(resumen, 'FLAG_MODELO_PRODUCTIVO'),
        'Estado_Elasticidad': '-',
        'Estado_fecha_proy': obtener_valor(resumen, 'FLAG_CONTIENE_DIAS_FUTUROS'),
        'Estado_proyección': 'Proyectado',
        'Comentario': obtener_valor(resumen, 'COMENTARIO'),
        'WMAPE Train': redondear_total(
            obtener_valor(resumen, 'WMAPE_TRAIN'), decimales=4
        ),
        'Bias Train': redondear_total(
            obtener_valor(resumen, 'BIAS_TRAIN'), decimales=4
        ),
        'MAE Train': redondear_total(
            obtener_valor(resumen, 'MAE_TRAIN'), decimales=4
        ),
        'Coverage Train': redondear_total(
            obtener_valor(resumen, 'COVERAGE_TRAIN'), decimales=4
        ),
        'WMAPE Test': redondear_total(
            obtener_valor(resumen, 'WMAPE_TEST'), decimales=4
        ),
        'Bias Test': redondear_total(
            obtener_valor(resumen, 'BIAS_TEST'), decimales=4
        ),
        'MAE Test': redondear_total(
            obtener_valor(resumen, 'MAE_TEST'), decimales=4
        ),
        'Coverage Test': redondear_total(
            obtener_valor(resumen, 'COVERAGE_TEST'), decimales=4
        ),
        'Win Rate Test': redondear_total(
            obtener_valor(resumen, 'WIN_RATE_TEST'), decimales=4
        ),
        'Intensidad Promocional': metadata_ean['Intensidad Promocional'],
        'SEGMENTO_ABCD': metadata_ean['SEGMENTO_ABCD'],
        'ADI': redondear_total(metadata_ean['ADI'], decimales=4),
        'CV2': redondear_total(metadata_ean['CV2'], decimales=4),
        'DIAS_CON_VENTA': metadata_ean['DIAS_CON_VENTA'],
    }


# =========================================================================
# 17. FERIADOS DE CHILE Y CALENDARIO FUTURO
# =========================================================================
def procesar_feriados_chile(historial_diario: pd.DataFrame) -> pd.DataFrame:
    """Reconstruye de forma determinista las dummies FLAG_FERIADO y
    FLAG_PRE_FERIADO para Chile, incluyendo feriados irrenunciables.

    Los nombres de columna coinciden exactamente con los usados en
    df_historial (FLAG_FERIADO, FLAG_PRE_FERIADO) para evitar
    desalineaciones entre el histórico y el calendario futuro.

    Parameters
    ----------
    historial_diario : pd.DataFrame
        Debe incluir la columna P_DATE.

    Returns
    -------
    pd.DataFrame
        Copia con columnas FLAG_FERIADO y FLAG_PRE_FERIADO (int8).
    """
    resultado = historial_diario.copy()

    if not pd.api.types.is_datetime64_any_dtype(resultado['P_DATE']):
        resultado['P_DATE'] = pd.to_datetime(resultado['P_DATE'])

    feriados_fijos = {
        (1, 1), (5, 1), (5, 21), (6, 20), (6, 29), (7, 16), (8, 15),
        (9, 18), (9, 19), (10, 12), (10, 31), (11, 1), (12, 8), (12, 25),
    }

    feriados_moviles = {
        2024: [pd.Timestamp('2024-03-29'), pd.Timestamp('2024-03-30')],
        2025: [pd.Timestamp('2025-04-18'), pd.Timestamp('2025-04-19')],
        2026: [pd.Timestamp('2026-04-03'), pd.Timestamp('2026-04-04')],
    }

    fechas_unicas = resultado['P_DATE'].drop_duplicates()

    def es_fecha_feriado(fecha: pd.Timestamp) -> bool:
        anio = fecha.year

        if anio in feriados_moviles and fecha in feriados_moviles[anio]:
            return True

        return (fecha.month, fecha.day) in feriados_fijos

    es_feriado = fechas_unicas.apply(es_fecha_feriado)
    es_pre_feriado = (
        (fechas_unicas + pd.Timedelta(days=1)).apply(es_fecha_feriado)
    )

    mapa_feriados = pd.DataFrame({
        'P_DATE': fechas_unicas,
        'FLAG_FERIADO': es_feriado.astype(np.int8),
        'FLAG_PRE_FERIADO': es_pre_feriado.astype(np.int8),
    })

    columnas_a_borrar = [
        columna
        for columna in (
            'FLAG_FERIADO', 'FLAG_PRE_FERIADO', 'FERIADO_IRRENUNCIABLE'
        )
        if columna in resultado.columns
    ]

    if columnas_a_borrar:
        resultado = resultado.drop(columns=columnas_a_borrar)

    return resultado.merge(mapa_feriados, on='P_DATE', how='left')


def construir_calendario_futuro(  # noqa: D417
    promociones: pd.DataFrame,
    columna_fecha_inicio: str = 'FECHA_INICIO_DE_PROMOCION',
    columna_fecha_fin: str = 'FECHA_FIN_DE_PROMOCION',
) -> pd.DataFrame:
    """Construye el calendario diario requerido para proyectar promociones.

    Cubre desde la fecha de inicio más temprana hasta la fecha de
    término más tardía presente en promociones.

    Parameters
    ----------
    promociones : pd.DataFrame
    columna_fecha_inicio : str
    columna_fecha_fin : str

    Returns
    -------
    pd.DataFrame
        Columnas P_DATE, FLAG_FERIADO, FLAG_PRE_FERIADO.
    """
    columnas_faltantes = {columna_fecha_inicio, columna_fecha_fin}.difference(
        promociones.columns
    )

    if columnas_faltantes:
        msg = f'Faltan las siguientes columnas: {sorted(columnas_faltantes)}'
        raise KeyError(
            msg
        )

    fechas_inicio = pd.to_datetime(
        promociones[columna_fecha_inicio], errors='coerce'
    )
    fechas_fin = pd.to_datetime(promociones[columna_fecha_fin], errors='coerce')

    promociones_invalidas = (
        fechas_inicio.isna() | fechas_fin.isna() | fechas_fin.lt(fechas_inicio)
    )

    if promociones_invalidas.any():
        msg = (
            'Existen promociones con fechas nulas, inválidas o con '
            'término anterior al inicio.'
        )
        raise ValueError(
            msg
        )

    calendario_futuro = pd.DataFrame({
        'P_DATE': pd.date_range(
            start=fechas_inicio.min().normalize(),
            end=fechas_fin.max().normalize(),
            freq='D',
        )
    })

    calendario_futuro = procesar_feriados_chile(calendario_futuro)

    calendario_futuro['FLAG_FERIADO'] = (
        calendario_futuro['FLAG_FERIADO'].fillna(0).astype(np.int8)
    )
    calendario_futuro['FLAG_PRE_FERIADO'] = (
        calendario_futuro['FLAG_PRE_FERIADO'].fillna(0).astype(np.int8)
    )

    return calendario_futuro


# 18. ORQUESTACIÓN: LOOP PROMOCIÓN-EAN
# =========================================================================
def loop_promociones(  # noqa: D417
    promociones_modelo: pd.DataFrame,
    historial_modelo: pd.DataFrame,
    caracterizacion_modelo: pd.DataFrame,
    columnas_familia: ColumnasFamiliaPromocional,
    configuracion: ConfiguracionModeloPromo,
    calendario_futuro: pd.DataFrame | None = None,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Procesa cada combinación promoción-EAN y retorna las dos salidas
    finales del pipeline.

    Los EAN sin caracterización, historial, elegibilidad, régimen o con
    errores puntuales quedan registrados como no proyectables, sin
    detener el procesamiento del resto.

    Para eficiencia, el historial se agrupa por EAN una sola vez (en
    lugar de filtrar el historial completo en cada iteración) y la
    caracterización se indexa por EAN.

    Parameters
    ----------
    promociones_modelo : pd.DataFrame
        Debe incluir N_PROMOCION y EAN (una fila por combinación).
    historial_modelo : pd.DataFrame
        Debe incluir EAN.
    caracterizacion_modelo : pd.DataFrame
        Debe incluir EAN.
    columnas_familia : ColumnasFamiliaPromocional
    configuracion : ConfiguracionModeloPromo
    calendario_futuro : pd.DataFrame | None

    Returns
    -------
    tuple[pd.DataFrame, pd.DataFrame]
        (excel_final, detalle_diario_completo). excel_final tiene una
        fila por combinación promoción-EAN; detalle_diario_completo
        concatena el detalle diario de todas las combinaciones
        proyectadas.
    """
    columnas_requeridas_por_tabla = {
        'promociones_modelo': ('N_PROMOCION', 'EAN'),
        'historial_modelo': ('EAN',),
        'caracterizacion_modelo': ('EAN',),
    }

    tablas = {
        'promociones_modelo': promociones_modelo,
        'historial_modelo': historial_modelo,
        'caracterizacion_modelo': caracterizacion_modelo,
    }

    for nombre_tabla, columnas_requeridas in columnas_requeridas_por_tabla.items():
        faltantes = set(columnas_requeridas).difference(
            tablas[nombre_tabla].columns
        )

        if faltantes:
            msg = f'Faltan columnas en {nombre_tabla}: {sorted(faltantes)}'
            raise KeyError(msg)

    promociones_trabajo = promociones_modelo.copy()
    historial_trabajo = historial_modelo.copy()
    caracterizacion_trabajo = caracterizacion_modelo.copy()

    promociones_trabajo['EAN'] = (
        promociones_trabajo['EAN'].astype('string').str.strip()
    )
    promociones_trabajo['N_PROMOCION'] = (
        promociones_trabajo['N_PROMOCION'].astype('string').str.strip()
    )
    historial_trabajo['EAN'] = historial_trabajo['EAN'].astype('string').str.strip()
    caracterizacion_trabajo['EAN'] = (
        caracterizacion_trabajo['EAN'].astype('string').str.strip()
    )

    # Pre-agrupación: evita re-filtrar el historial completo por cada fila.
    historial_por_ean = {  # noqa: C416
        ean: subhistorial
        for ean, subhistorial in historial_trabajo.groupby('EAN', sort=False)
    }
    caracterizacion_indexada = caracterizacion_trabajo.set_index(
        'EAN', drop=False
    )

    filas_excel_final: list[dict[str, object]] = []
    detalles_diarios: list[pd.DataFrame] = []

    promociones_agrupadas = promociones_trabajo.groupby('N_PROMOCION', sort=False)
    total_promociones = promociones_agrupadas.ngroups

    for indice_promocion, (numero_promocion, grupo_promocion) in enumerate(
        promociones_agrupadas, start=1
    ):
        total_ean_promocion = len(grupo_promocion)

        logging.info(
            'Promoción %s/%s (N_PROMOCION=%s) — %s EAN a procesar',
            indice_promocion, total_promociones, numero_promocion,
            total_ean_promocion,
        )

        for indice_ean, (_, promocion_ean) in enumerate(
            grupo_promocion.iterrows(), start=1
        ):
            ean = str(promocion_ean['EAN'])

            logging.info(
                '  EAN %s/%s — %s', indice_ean, total_ean_promocion, ean
            )

            caracterizacion_ean = (
                caracterizacion_indexada.loc[ean]
                if ean in caracterizacion_indexada.index
                else None
            )

            if caracterizacion_ean is None:
                filas_excel_final.append(
                    construir_fila_no_proyectable(
                        promocion_ean=promocion_ean,
                        caracterizacion_ean=None,
                        motivo='EAN sin registro en caracterización.',
                    )
                )
                continue

            historial_ean = historial_por_ean.get(ean)

            if historial_ean is None or historial_ean.empty:
                filas_excel_final.append(
                    construir_fila_no_proyectable(
                        promocion_ean=promocion_ean,
                        caracterizacion_ean=caracterizacion_ean,
                        motivo='EAN sin historial disponible.',
                    )
                )
                continue

            try:
                elegibilidad, regimen = evaluar_ean(
                    ean=ean,
                    caracterizacion=caracterizacion_indexada,
                    configuracion=configuracion,
                )

                if not elegibilidad.elegible or regimen is None:
                    filas_excel_final.append(
                        construir_fila_no_proyectable(
                            promocion_ean=promocion_ean,
                            caracterizacion_ean=caracterizacion_ean,
                            motivo=obtener_motivo_elegibilidad(elegibilidad),
                        )
                    )
                    continue

                datos_entrenamiento = preparar_datos_entrenamiento(
                    historial_ean=historial_ean,
                    regimen=regimen,
                    columnas_familia=columnas_familia,
                    configuracion=configuracion,
                )

                resultado_entrenamiento = entrenar_modelo_producto(
                    datos_entrenamiento=datos_entrenamiento,
                    regimen=regimen,
                    configuracion=configuracion,
                )

                resultado_escenarios = proyectar_promocion_ean(
                    promocion_ean=promocion_ean,
                    historial_ean=historial_ean,
                    caracterizacion_ean=caracterizacion_ean,
                    resultado_entrenamiento=resultado_entrenamiento,
                    regimen=regimen,
                    columnas_familia=columnas_familia,
                    configuracion=configuracion,
                    calendario_futuro=calendario_futuro,
                )

                filas_excel_final.append(
                    construir_fila_excel_final(
                        resultado_escenarios=resultado_escenarios,
                        promocion_ean=promocion_ean,
                        caracterizacion_ean=caracterizacion_ean,
                    )
                )

                if configuracion.guardar_detalle_diario:
                    detalles_diarios.append(resultado_escenarios.detalle_diario)

            except Exception as error:  # noqa: BLE001
                logging.warning(
                    '  Error en Promoción %s / EAN %s: %s: %s',
                    numero_promocion, ean, type(error).__name__, error,
                )
                filas_excel_final.append(
                    construir_fila_no_proyectable(
                        promocion_ean=promocion_ean,
                        caracterizacion_ean=caracterizacion_ean,
                        motivo=(
                            'Error durante entrenamiento o proyección: '
                            f'{type(error).__name__}: {error}'
                        ),
                    )
                )

    excel_final = pd.DataFrame(filas_excel_final, columns=COLUMNAS_EXCEL_FINAL)

    detalle_diario_completo = (
        pd.concat(detalles_diarios, axis=0, ignore_index=True)
        if detalles_diarios
        else pd.DataFrame(
            columns=[
                'N_PROMOCION', 'EAN', 'P_DATE', 'ESCENARIO',
                'PRECIO_ESCENARIO', 'CANTIDAD_OBSERVADA', 'CANTIDAD_MODELO',
                'CANTIDAD_FINAL', 'ORIGEN_CANTIDAD', 'FLAG_DATO_REAL',
                'FLAG_DIA_FUTURO', 'FLAG_DIA_PASADO_SIN_DATO',
            ]
        )
    )

    logging.info(
        'Proceso finalizado: %s filas en Excel final, %s filas en '
        'detalle diario.',
        len(excel_final), len(detalle_diario_completo),
    )

    return excel_final, detalle_diario_completo


# =========================================================================
# 19. EJECUCIÓN FINAL
# =========================================================================
def ejecutar_pipeline_promocional(
    df_historial: pd.DataFrame,
    df_caracterizacion: pd.DataFrame,
    df_promos_proy: pd.DataFrame,
    configuracion: ConfiguracionModeloPromo | None = None,
    ruta_salida_excel: str | None = None,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Ejecuta el pipeline completo de forecast promocional.

    Parameters
    ----------
    df_historial : pd.DataFrame
        Historial diario por producto (nombre acordado con negocio).
    df_caracterizacion : pd.DataFrame
        Características y flags de productos.
    df_promos_proy : pd.DataFrame
        Información promocional de productos y promos a proyectar.
    configuracion : ConfiguracionModeloPromo | None
        Si es None, se usa la configuración por defecto.
    ruta_salida_excel : str | None
        Si se entrega, el Excel final se guarda en esa ruta.

    Returns
    -------
    tuple[pd.DataFrame, pd.DataFrame]
        (excel_final, detalle_diario_completo) — las únicas dos salidas
        requeridas por el proceso de implementación.
    """
    configurar_logging()

    configuracion = configuracion or ConfiguracionModeloPromo()

    logging.info('Preparando fuentes del modelo...')
    fuentes = preparar_fuentes_modelo(
        historial=df_historial,
        caracterizacion=df_caracterizacion,
        promociones_futuras=df_promos_proy,
        configuracion=configuracion,
    )

    logging.info('Construyendo calendario futuro (feriados Chile)...')
    calendario_futuro = construir_calendario_futuro(
        promociones=fuentes.promociones_futuras,
    )

    logging.info('Iniciando loop de promociones...')
    excel_final, detalle_diario_completo = loop_promociones(
        promociones_modelo=fuentes.promociones_futuras,
        historial_modelo=fuentes.historial,
        caracterizacion_modelo=fuentes.caracterizacion,
        columnas_familia=fuentes.columnas_familia,
        configuracion=configuracion,
        calendario_futuro=calendario_futuro,
    )

    if ruta_salida_excel is not None:
        logging.info('Guardando Excel final en %s', ruta_salida_excel)
        excel_final.to_excel(ruta_salida_excel, index=False)

    return excel_final, detalle_diario_completo


def generar_excel_buffer(
    df: pd.DataFrame,
    sheet_name: str = 'Resultados_proyeccion'
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
    outputs_dir = posixpath.join(file_site, 'Outputs')

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

    configuracion_pipeline = ConfiguracionModeloPromo(
        familia_promocional='B',
    )

    excel_final, _detalle_diario_completo = ejecutar_pipeline_promocional(
        df_historial=df_historial,
        df_caracterizacion=df_caracterizacion,
        df_promos_proy=df_promos_proy,
        configuracion=configuracion_pipeline
    )

    ### Pequeños ajustes de formato
    excel_final = excel_final.drop(columns=
                                        ['Estado_Historial',
                                        'Estado_Modelo',
                                        'Estado_Elasticidad',
                                        'Estado_fecha_proy',
                                        'Estado_proyección'])

    excel_final['Inicio Proy'] = excel_final['Inicio Proy'].dt.date
    excel_final['Fin Proy']    = excel_final['Fin Proy'].dt.date
    # TEMP print
    print('Excel final info: ', excel_final.info())

    output_buffer = generar_excel_buffer(excel_final)  # noqa: F841

    subir_archivo_sharepoint(
       contenido=output_buffer,  # noqa: ERA001
        nombre_archivo=nombre_output,  # noqa: ERA001
       outputs_dir=outputs_dir,  # noqa: ERA001
       sp_cred=sp_cred  # noqa: ERA001
    )  # noqa: ERA001




if __name__ == '__main__':
    main()
