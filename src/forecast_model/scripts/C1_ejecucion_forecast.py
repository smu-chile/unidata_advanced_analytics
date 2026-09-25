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
