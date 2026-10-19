# =========================================================================
# 0. IMPORTS
# =========================================================================
import io  # noqa: F401
import os
import re
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

from common.gcp_extended.bigquery import uploadFrame


directorio_actual = os.path.abspath(os.curdir)

while directorio_actual != os.path.sep:
    try:
        sys.path.append(directorio_actual)
        from credentials import credentials  # noqa: F401
        break  # Si la importación es exitosa, sale del bucle
    except ModuleNotFoundError:
        sys.path.pop()  # Remueve el directorio que no contenía el módulo
        directorio_actual = os.path.dirname(directorio_actual)  # Retrocede

import common.gcp_extended.secretsmanager as secretmanager  # noqa: E402, F401
import common.office365_extended.sharepoint as sp  # noqa: E402
from common.constants import LOGGING_CONFIG  # noqa: E402, F401
from common.databases.queries import QueryDict  # noqa: E402, F401
from common.gcp_extended.bigquery import (  # noqa: E402
    uploadFrame,  # noqa: F401, F811
    readBigQuery,  # noqa: F401
    deleteFromTable,  # noqa: F401
    createTableAsSelect,  # noqa: F401
)


# Imports internos: mantener tal cual en tu repo
# (sp, secretmanager, readBigQuery, deleteFromTable, uploadFrame,
#  QueryDict, LOGGING_CONFIG)


# =========================================================================
# 0.1 CONSTANTES
# =========================================================================
SUFIJO_INPUT = '_input_proyeccion.xlsx'
SUFIJO_OUTPUT = '_resultado_proyeccion.xlsx'

COLUMNAS_TRANSVERSALES = [
    'TENDENCIA_LINEAL', 'SIN_ANUAL', 'COS_ANUAL',
    'FLAG_FERIADO', 'FLAG_PRE_FERIADO',
]

MAPA_DIAS_SEMANA = {
    'dow_martes': 1, 'dow_miercoles': 2, 'dow_jueves': 3,
    'dow_viernes': 4, 'dow_sabado': 5, 'dow_domingo': 6,
}

COLUMNAS_DIA_SEMANA = list(MAPA_DIAS_SEMANA)

COLUMNAS_CANIBALIZACION = [
    'N Sustitutos Evaluados', 'N Sustitutos Canibalizados',
    'UV Canibalizada', 'Venta Canibalizada',
    'UV Incremental Neta', 'Venta Incremental Neta',
    'Comentario Canibalización',
]

COLUMNAS_EXCEL_FINAL = [
    'N° promoción', 'Nombre promoción', 'Categoría', 'Subcategoría',
    'Descripcion', 'Material', 'UMV', 'EAN', 'R²', 'Elasticidad',
    'Estable', 'TIPOLOGIA_DEMANDA', 'Inicio Proy', 'Fin Proy',
    'Precio Modal', 'Precio Promocional', 'Baseline_UV',
    'UV Incremental Real', 'UV Incremental Proy', 'UV Real', 'UV Proy',
    'Baseline Venta', 'Venta Incremental Real', 'Venta Incremental Proy',
    'Venta Real', 'Venta Proy',
    *COLUMNAS_CANIBALIZACION,
    'Estado_Historial', 'Estado_Modelo',
    'Estado_Elasticidad', 'Estado_fecha_proy', 'Estado_proyección',
    'Comentario', 'WMAPE Train', 'Bias Train', 'MAE Train',
    'Coverage Train', 'WMAPE Test', 'Bias Test', 'MAE Test',
    'Coverage Test', 'Win Rate Test', 'Intensidad Promocional',
    'SEGMENTO_ABCD', 'ADI', 'CV2', 'DIAS_CON_VENTA',
]

COLUMNAS_DETALLE_DIARIO = [
    'N_PROMOCION', 'EAN', 'P_DATE', 'ESCENARIO', 'PRECIO_ESCENARIO',
    'CANTIDAD_OBSERVADA', 'CANTIDAD_MODELO', 'CANTIDAD_FINAL',
    'ORIGEN_CANTIDAD', 'FLAG_DATO_REAL', 'FLAG_DIA_FUTURO',
    'FLAG_DIA_PASADO_SIN_DATO',
]

COLUMNAS_DETALLE_CANIBALIZACION = [
    'N_PROMOCION', 'EAN_FOCAL', 'EAN_RELACIONADO', 'ELASTICIDAD_CRUZADA',
    'DELTA_LN_PRECIO_FOCAL', 'DIAS_PROMOCION', 'DIAS_EVALUADOS',
    'DIAS_EXCLUIDOS_PROMO', 'DIAS_SIN_DATO', 'UV_OBSERVADA_SUSTITUTO',
    'UV_CANIBALIZADA', 'VENTA_CANIBALIZADA',
]


# =========================================================================
# 0.2 SHAREPOINT
# =========================================================================
def obtener_rutas_sharepoint(ruta_base: str) -> tuple[str, str]:
    """Rutas de las carpetas Inputs y Outputs."""
    return (
        posixpath.join(ruta_base, 'Inputs'),
        posixpath.join(ruta_base, 'Outputs'),
    )


def obtener_carpetas_sharepoint(
    credenciales_sharepoint: dict,
    ruta_base: str,
) -> tuple[sp.SharePointFolder, sp.SharePointFolder, str, str]:
    """Inicializa las carpetas Inputs y Outputs de SharePoint."""
    ruta_inputs, ruta_outputs = obtener_rutas_sharepoint(ruta_base)

    carpeta_inputs = sp.SharePointFolder(
        **credenciales_sharepoint, server_relative_folder=ruta_inputs,
    )
    carpeta_outputs = sp.SharePointFolder(
        **credenciales_sharepoint, server_relative_folder=ruta_outputs,
    )

    return carpeta_inputs, carpeta_outputs, ruta_inputs, ruta_outputs


def filtrar_archivos_excel(archivos: list[str], patron: re.Pattern) -> list[str]:
    """Filtra Excel válidos (sin temporales ~$) que cumplen el patrón."""
    return sorted(
        archivo
        for archivo in archivos
        if (
            archivo.lower().endswith('.xlsx')
            and not archivo.startswith('~$')
            and patron.fullmatch(archivo)
        )
    )


def obtener_clave_input(nombre_input: str) -> str:
    """2025_11_05_v5_input_proyeccion.xlsx -> 2025_11_05_v5"""
    if not nombre_input.endswith(SUFIJO_INPUT):
        msg = (
            f'El archivo no cumple el sufijo esperado: {SUFIJO_INPUT}. '
            f'Archivo recibido: {nombre_input}.'
        )
        raise ValueError(msg)

    return nombre_input.removesuffix(SUFIJO_INPUT)


def obtener_nombre_output(nombre_input: str) -> str:
    """Nombre del output asociado a un input."""
    return f'{obtener_clave_input(nombre_input)}{SUFIJO_OUTPUT}'


def obtener_inputs_pendientes(
    inputs_validos: list[str],
    outputs_validos: set[str],
) -> list[str]:
    """Inputs que aún no tienen output."""
    outputs_normalizados = {nombre.lower() for nombre in outputs_validos}

    return [
        nombre_input
        for nombre_input in inputs_validos
        if obtener_nombre_output(nombre_input).lower() not in outputs_normalizados
    ]


def cargar_input_desde_sharepoint(
    credenciales_sharepoint: dict,
    ruta_inputs: str,
    nombre_input: str,
) -> pd.DataFrame:
    """Carga un Excel desde Inputs."""
    archivo_sharepoint = sp.SharePointFile(
        **credenciales_sharepoint,
        server_relative_path=posixpath.join(ruta_inputs, nombre_input),
    )

    return archivo_sharepoint.toFrame()


def validar_input_promociones(tabla_input: pd.DataFrame) -> list[str]:
    """Extrae promociones con generar_proyeccion = si."""
    columnas_faltantes = {'n_promocion', 'generar_proyeccion'}.difference(
        tabla_input.columns
    )

    if columnas_faltantes:
        msg = f'Faltan columnas en el Excel: {sorted(columnas_faltantes)}.'
        raise ValueError(msg)

    promociones = (
        tabla_input.loc[
            tabla_input['generar_proyeccion']
            .astype('string').str.strip().str.lower().eq('si'),
            'n_promocion',
        ]
        .dropna()
        .astype('string')
        .str.strip()
        .unique()
        .tolist()
    )

    if not promociones:
        msg = 'No se encontraron promociones con generar_proyeccion = si.'
        raise ValueError(msg)

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
    """Prepara el input (modo automático o manual)."""
    usa_input_manual = input_manual is not None

    if usa_input_manual != (nombre_input_manual is not None):
        msg = 'input_manual y nombre_input_manual deben entregarse juntos.'
        raise ValueError(msg)

    ruta_inputs, ruta_outputs = obtener_rutas_sharepoint(ruta_base_sharepoint)

    if usa_input_manual:
        if not patron_input.fullmatch(nombre_input_manual):
            msg = (
                'nombre_input_manual no cumple el patrón de input válido: '
                f'{nombre_input_manual}.'
            )
            raise ValueError(msg)

        tabla_input = input_manual.copy()
        nombre_input = nombre_input_manual
        logging.info('Procesando input manual: %s', nombre_input)
    else:
        carpeta_inputs, carpeta_outputs, ruta_inputs, ruta_outputs = (
            obtener_carpetas_sharepoint(credenciales_sharepoint, ruta_base_sharepoint)
        )

        inputs_validos = filtrar_archivos_excel(carpeta_inputs.fileList(), patron_input)
        outputs_validos = set(
            filtrar_archivos_excel(carpeta_outputs.fileList(), patron_output)
        )
        inputs_pendientes = obtener_inputs_pendientes(inputs_validos, outputs_validos)

        if not inputs_pendientes:
            mensaje = 'No hay inputs pendientes por procesar.'
            logging.info(mensaje)

            if abortar_si_no_pendientes:
                raise ValueError(mensaje)

            return None, [], None, None

        nombre_input = inputs_pendientes[-1]
        logging.info('Procesando último input pendiente: %s', nombre_input)

        tabla_input = cargar_input_desde_sharepoint(
            credenciales_sharepoint, ruta_inputs, nombre_input
        )

    nombre_output = obtener_nombre_output(nombre_input)
    logging.info('Nombre de output esperado: %s', nombre_output)

    return tabla_input, validar_input_promociones(tabla_input), ruta_outputs, nombre_output


logger = logging.getLogger('modelo_promocional')


def configurar_logging(nivel: int = logging.INFO) -> None:
    """Configura logging por consola."""
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
    """Configuración central del modelo promocional y de canibalización.

    Evaluación ex post
    ------------------
    - entrenar_hasta_inicio_promocion: el modelo se entrena con datos
      estrictamente anteriores a FECHA_INICIO_DE_PROMOCION (T0 - 1).
    - La venta observada se usa hasta FECHA_FIN_DE_PROMOCION.

    Canibalización
    --------------
    - calcular_canibalizacion: activa el cálculo.
    - top_sustitutos_canibalizacion: máximo de sustitutos por EAN focal.
    - confiabilidades_validas: valores aceptados de CONFIABILIDAD
      (None = se aceptan todos).
    - excluir_sustitutos_en_promocion: excluye días en que el sustituto
      también estaba en promoción.
    """

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

    entrenar_hasta_inicio_promocion: bool = True

    calcular_canibalizacion: bool = True
    top_sustitutos_canibalizacion: int = 10
    confiabilidades_validas: tuple[str, ...] | None = None
    excluir_sustitutos_en_promocion: bool = True

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
        if self.familia_promocional.upper() not in {'B', 'T'}:
            msg = 'familia_promocional debe ser B o T.'
            raise ValueError(msg)

        if not 0 < self.proporcion_test < 1:
            msg = 'proporcion_test debe estar entre 0 y 1.'
            raise ValueError(msg)

        if not (
            0 <= self.umbral_intensidad_baja < self.umbral_intensidad_alta <= 100
        ):
            msg = (
                'Los umbrales de intensidad deben cumplir: '
                '0 <= umbral bajo < umbral alto <= 100.'
            )
            raise ValueError(msg)

        if self.top_sustitutos_canibalizacion < 1:
            msg = 'top_sustitutos_canibalizacion debe ser >= 1.'
            raise ValueError(msg)


# =========================================================================
# 2. COLUMNAS DE LA FAMILIA PROMOCIONAL
# =========================================================================
@dataclass(frozen=True)
class ColumnasFamiliaPromocional:
    """Nombres de columnas históricas de la familia B o T."""

    familia: str
    precio_modal: str
    precio_promocional: str
    precio_promocional_minimo: str
    numero_promocion: str
    nombre_promocion: str
    descripcion_evento: str
    porcentaje_descuento: str
    flag_promocion: str


def resolver_columnas_familia(familia_promocional: str) -> ColumnasFamiliaPromocional:
    """Construye los nombres de columnas de la familia."""
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
    """Fuentes preparadas para entrenar, proyectar y canibalizar."""

    historial: pd.DataFrame
    caracterizacion: pd.DataFrame
    promociones_futuras: pd.DataFrame
    columnas_familia: ColumnasFamiliaPromocional
    elasticidades: pd.DataFrame
    historial_sustitutos: pd.DataFrame


@dataclass(frozen=True)
class ResultadoElegibilidad:
    """Reglas mínimas para entrenar/proyectar un EAN."""

    elegible: bool
    es_producto_nuevo: bool
    dias_con_venta: int
    estado_historial: str
    comentario: str


@dataclass(frozen=True)
class RegimenPromocional:
    """Tratamiento metodológico asignado a un producto."""

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
    """Matrices y metadatos para entrenar y evaluar."""

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
    """Métricas de error en unidades originales."""

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
    """Resultado del doble entrenamiento (validación + productivo)."""

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
    fecha_fin_entrenamiento: pd.Timestamp


@dataclass
class ResultadoEscenarios:
    """Resultado diario y agregado de una combinación promoción-EAN."""

    detalle_diario: pd.DataFrame
    resumen: dict[str, object]


# =========================================================================
# 4. AUXILIARES DE NORMALIZACIÓN
# =========================================================================
def normalizar_ean(serie_ean: pd.Series) -> pd.Series:
    """EAN como texto, sin espacios ni sufijo .0."""
    return (
        serie_ean.astype('string')
        .str.strip()
        .str.replace(r'\.0$', '', regex=True)
    )


def normalizar_flag(serie_flag: pd.Series) -> pd.Series:
    """Flags de texto en mayúsculas y sin espacios."""
    return serie_flag.astype('string').str.strip().str.upper()


def normalizar_nombre_mecanica(serie_mecanica: pd.Series) -> pd.Series:
    """Normaliza mecánicas igual que asignar_mecanicas_escenario."""
    return (
        serie_mecanica.astype('string')
        .fillna('SIN_MECANICA')
        .str.strip()
        .replace('', 'SIN_MECANICA')
        .str.upper()
        .str.replace(' ', '_', regex=False)
    )


def limpiar_porcentaje_descuento(serie_descuento: pd.Series) -> pd.Series:
    """Descuento float32 en [0, inf)."""
    descuento = pd.to_numeric(serie_descuento, errors='coerce').fillna(0.0)

    return descuento.clip(lower=0.0).astype('float32')


def es_valor_afirmativo(valor: object, configuracion: ConfiguracionModeloPromo) -> bool:
    """True si el flag es afirmativo."""
    if valor is None or pd.isna(valor):
        return False

    return str(valor).strip().upper() in configuracion.valores_afirmativos


def convertir_float(valor: object) -> float:
    """Convierte a float finito; NaN si no es posible."""
    try:
        numero = float(valor)
    except (TypeError, ValueError):
        return np.nan

    return numero if np.isfinite(numero) else np.nan


def formatear_lista_sql(valores: list | pd.Series) -> str:
    """Lista de strings para IN (...) usando comillas simples."""
    return ','.join(repr(str(valor)) for valor in valores)


def _parsear_mecanicas_validas(
    valor_mecanicas: object,
    separadores: tuple[str, ...] = (';', ',', '|'),
) -> list[str]:
    """Normaliza MECANICAS_VALIDAS a lista de strings en mayúsculas."""
    if valor_mecanicas is None:
        return []

    if isinstance(valor_mecanicas, list | tuple | set):
        items = [str(item) for item in valor_mecanicas]
    else:
        try:
            if pd.isna(valor_mecanicas):
                return []
        except (TypeError, ValueError):
            pass

        texto = str(valor_mecanicas).strip()

        if not texto:
            return []

        separador = next((sep for sep in separadores if sep in texto), None)
        items = texto.split(separador) if separador else [texto]

    mecanicas = [
        item.strip().upper().replace(' ', '_') for item in items if str(item).strip()
    ]

    return list(dict.fromkeys(mecanicas))


# =========================================================================
# 5. PREPARACIÓN DE FUENTES
# =========================================================================
def preparar_promociones_futuras(promociones_futuras: pd.DataFrame) -> pd.DataFrame:
    """Prepara las combinaciones promoción-EAN (una fila por combinación).
    """
    columnas_requeridas = [
        'N_PROMOCION', 'NOMBRE_PROMOCION', 'EAN',
        'FECHA_INICIO_DE_PROMOCION', 'FECHA_FIN_DE_PROMOCION',
        'PRECIO_PROMOCIONAL', 'PRECIO_MODAL', 'PORCENTAJE_DESCUENTO',
        'DESCRIPCION_EVENTO_PROMOCIONAL',
    ]

    columnas_faltantes = set(columnas_requeridas).difference(promociones_futuras.columns)

    if columnas_faltantes:
        msg = f'Faltan columnas en promociones: {sorted(columnas_faltantes)}'
        raise ValueError(msg)

    promociones = promociones_futuras.loc[:, columnas_requeridas].copy()

    promociones['EAN'] = normalizar_ean(promociones['EAN'])
    promociones['N_PROMOCION'] = promociones['N_PROMOCION'].astype('string').str.strip()

    for columna in ('FECHA_INICIO_DE_PROMOCION', 'FECHA_FIN_DE_PROMOCION'):
        promociones[columna] = pd.to_datetime(
            promociones[columna], errors='coerce'
        ).dt.normalize()

    for columna in ('PRECIO_PROMOCIONAL', 'PRECIO_MODAL'):
        promociones[columna] = pd.to_numeric(
            promociones[columna], errors='coerce'
        ).astype('float64')

    promociones['PORCENTAJE_DESCUENTO'] = limpiar_porcentaje_descuento(
        promociones['PORCENTAJE_DESCUENTO']
    )
    promociones['DESCRIPCION_EVENTO_PROMOCIONAL'] = (
        promociones['DESCRIPCION_EVENTO_PROMOCIONAL']
        .astype('string').fillna('SIN_MECANICA').str.strip()
    )

    fechas_invalidas = (
        promociones['FECHA_INICIO_DE_PROMOCION'].isna()
        | promociones['FECHA_FIN_DE_PROMOCION'].isna()
        | promociones['FECHA_FIN_DE_PROMOCION'].lt(
            promociones['FECHA_INICIO_DE_PROMOCION']
        )
    )

    if fechas_invalidas.any():
        msg = 'Existen promociones con fechas inválidas o incompletas.'
        raise ValueError(msg)

    precios_invalidos = (
        promociones['PRECIO_PROMOCIONAL'].isna()
        | promociones['PRECIO_MODAL'].isna()
        | promociones['PRECIO_PROMOCIONAL'].le(0)
        | promociones['PRECIO_MODAL'].le(0)
    )

    if precios_invalidos.any():
        msg = 'Existen promociones con precios nulos o no positivos.'
        raise ValueError(msg)

    promociones['DURACION_PROMOCION_DIAS'] = (
        (
            promociones['FECHA_FIN_DE_PROMOCION']
            - promociones['FECHA_INICIO_DE_PROMOCION']
        ).dt.days + 1
    ).astype('int16')

    return promociones.reset_index(drop=True)


def preparar_caracterizacion(
    caracterizacion: pd.DataFrame,
    ean_requeridos: pd.Index,
) -> pd.DataFrame:
    """Caracterización indexada por EAN, un registro por EAN."""
    columnas_seleccionadas = [
        'EAN', 'PRODUCT_DESCRIPTION', 'CATEGORY_DESCRIPTION',
        'SUB_CATEGORY_DESCRIPTION', 'MATERIAL', 'SALES_UOM', 'SALES_UNIT',
        'DIAS_CON_VENTA', 'ES_PRODUCTO_NUEVO', 'INTENSIDAD_PROMOCIONAL',
        'TIPOLOGIA_DEMANDA', 'MECANICAS_VALIDAS', 'N_MECANICAS_VALIDAS',
        'CASO_MODELO', 'INCLUIR_MECANICA', 'MECANICA_REFERENCIA',
        'SEGMENTO_ABCD', 'ADI', 'CV2', 'CLASIFICACION_ADI_CV2',
    ]

    columnas_disponibles = [
        columna for columna in columnas_seleccionadas if columna in caracterizacion.columns
    ]

    columnas_faltantes = {
        'EAN', 'DIAS_CON_VENTA', 'ES_PRODUCTO_NUEVO', 'INTENSIDAD_PROMOCIONAL',
    }.difference(columnas_disponibles)

    if columnas_faltantes:
        msg = f'Faltan columnas en caracterización: {sorted(columnas_faltantes)}'
        raise ValueError(msg)

    ean_normalizado = normalizar_ean(caracterizacion['EAN'])
    mascara_ean = ean_normalizado.isin(ean_requeridos)

    caracterizacion_preparada = caracterizacion.loc[mascara_ean, columnas_disponibles].copy()
    caracterizacion_preparada['EAN'] = ean_normalizado.loc[mascara_ean]

    caracterizacion_preparada['DIAS_CON_VENTA'] = (
        pd.to_numeric(caracterizacion_preparada['DIAS_CON_VENTA'], errors='coerce')
        .fillna(0).astype('int32')
    )
    caracterizacion_preparada['INTENSIDAD_PROMOCIONAL'] = (
        pd.to_numeric(caracterizacion_preparada['INTENSIDAD_PROMOCIONAL'], errors='coerce')
        .fillna(0.0).clip(0.0, 100.0).astype('float32')
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


def preparar_historial(
    historial: pd.DataFrame,
    ean_requeridos: pd.Index,
    columnas_familia: ColumnasFamiliaPromocional,
) -> pd.DataFrame:
    """Historial de los EAN focales, ordenado por EAN y fecha."""
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
        raise ValueError(msg)

    ean_normalizado = normalizar_ean(historial['EAN'])
    mascara_ean = ean_normalizado.isin(ean_requeridos)

    historial_preparado = historial.loc[mascara_ean, columnas_modelo].copy()
    historial_preparado['EAN'] = ean_normalizado.loc[mascara_ean]
    historial_preparado['P_DATE'] = pd.to_datetime(
        historial_preparado['P_DATE'], errors='coerce'
    ).dt.normalize()

    historial_preparado[columnas_familia.porcentaje_descuento] = (
        limpiar_porcentaje_descuento(historial_preparado[columnas_familia.porcentaje_descuento])
    )

    for columna in (columnas_familia.flag_promocion, 'FLAG_FERIADO', 'FLAG_PRE_FERIADO'):
        historial_preparado[columna] = (
            pd.to_numeric(historial_preparado[columna], errors='coerce')
            .fillna(0).clip(0, 1).astype('int8')
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
        .drop_duplicates(['EAN', 'P_DATE'], keep='last')
        .reset_index(drop=True)
    )


def preparar_elasticidades_cruzadas(
    tabla_elasticidades: pd.DataFrame | None,
    ean_focales: pd.Index,
) -> pd.DataFrame:
    """Normaliza la tabla EAN_FOCAL -> EAN_RELACIONADO.

    Un EAN focal ausente en esta tabla no tiene elasticidad reportable.
    """
    columnas_salida = [
        'EAN_FOCAL', 'EAN_RELACIONADO', 'ELASTICIDAD_CRUZADA', 'CONFIABILIDAD',
    ]

    if tabla_elasticidades is None or tabla_elasticidades.empty:
        return pd.DataFrame(columns=columnas_salida)

    elasticidades = tabla_elasticidades.copy()
    elasticidades.columns = [str(columna).strip().upper() for columna in elasticidades.columns]

    columnas_faltantes = {
        'EAN_FOCAL', 'EAN_RELACIONADO', 'ELASTICIDAD_CRUZADA',
    }.difference(elasticidades.columns)

    if columnas_faltantes:
        msg = f'Faltan columnas en elasticidades: {sorted(columnas_faltantes)}'
        raise ValueError(msg)

    if 'CONFIABILIDAD' not in elasticidades.columns:
        elasticidades['CONFIABILIDAD'] = pd.NA

    elasticidades['EAN_FOCAL'] = normalizar_ean(elasticidades['EAN_FOCAL'])
    elasticidades['EAN_RELACIONADO'] = normalizar_ean(elasticidades['EAN_RELACIONADO'])
    elasticidades['ELASTICIDAD_CRUZADA'] = pd.to_numeric(
        elasticidades['ELASTICIDAD_CRUZADA'], errors='coerce'
    ).astype('float64')
    elasticidades['CONFIABILIDAD'] = normalizar_flag(elasticidades['CONFIABILIDAD'])

    mascara = (
        elasticidades['EAN_FOCAL'].isin(ean_focales)
        & elasticidades['EAN_RELACIONADO'].notna()
        & np.isfinite(elasticidades['ELASTICIDAD_CRUZADA'])
        & elasticidades['EAN_FOCAL'].ne(elasticidades['EAN_RELACIONADO'])
    )

    return (
        elasticidades.loc[mascara, columnas_salida]
        .drop_duplicates(['EAN_FOCAL', 'EAN_RELACIONADO'], keep='first')
        .reset_index(drop=True)
    )


def preparar_historial_sustitutos(
    historial_sustitutos: pd.DataFrame | None,
    ean_sustitutos: pd.Index,
    columnas_familia: ColumnasFamiliaPromocional,
) -> pd.DataFrame:
    """Venta diaria observada, precio y flag promo de los sustitutos."""
    columnas_salida = [
        'EAN', 'P_DATE', 'CANTIDAD_TOTAL', 'PRECIO_PROMEDIO', 'FLAG_PROMO_SUSTITUTO',
    ]

    if historial_sustitutos is None or historial_sustitutos.empty:
        return pd.DataFrame({
            'EAN': pd.Series(dtype='string'),
            'P_DATE': pd.Series(dtype='datetime64[ns]'),
            'CANTIDAD_TOTAL': pd.Series(dtype='float64'),
            'PRECIO_PROMEDIO': pd.Series(dtype='float64'),
            'FLAG_PROMO_SUSTITUTO': pd.Series(dtype='int8'),
        })

    sustitutos = historial_sustitutos.copy()
    sustitutos['EAN'] = normalizar_ean(sustitutos['EAN'])
    sustitutos = sustitutos.loc[sustitutos['EAN'].isin(ean_sustitutos)].copy()

    sustitutos['P_DATE'] = pd.to_datetime(sustitutos['P_DATE'], errors='coerce').dt.normalize()
    sustitutos['CANTIDAD_TOTAL'] = (
        pd.to_numeric(sustitutos['CANTIDAD_TOTAL'], errors='coerce')
        .fillna(0.0).clip(lower=0.0).astype('float64')
    )
    sustitutos['PRECIO_PROMEDIO'] = pd.to_numeric(
        sustitutos['PRECIO_PROMEDIO'], errors='coerce'
    ).astype('float64')

    columna_flag = columnas_familia.flag_promocion
    sustitutos['FLAG_PROMO_SUSTITUTO'] = (
        pd.to_numeric(sustitutos[columna_flag], errors='coerce')
        .fillna(0).clip(0, 1).astype('int8')
        if columna_flag in sustitutos.columns
        else np.int8(0)
    )

    return (
        sustitutos.dropna(subset=['EAN', 'P_DATE'])[columnas_salida]
        .sort_values(['EAN', 'P_DATE'], kind='mergesort')
        .drop_duplicates(['EAN', 'P_DATE'], keep='last')
        .reset_index(drop=True)
    )


def preparar_fuentes_modelo(
    historial: pd.DataFrame,
    caracterizacion: pd.DataFrame,
    promociones_futuras: pd.DataFrame,
    configuracion: ConfiguracionModeloPromo,
    tabla_elasticidades: pd.DataFrame | None = None,
    historial_sustitutos: pd.DataFrame | None = None,
) -> FuentesModeloPreparadas:
    """Prepara todas las fuentes antes del loop promoción-EAN."""
    promociones_preparadas = preparar_promociones_futuras(promociones_futuras)
    ean_requeridos = pd.Index(promociones_preparadas['EAN'].dropna().unique())
    columnas_familia = resolver_columnas_familia(configuracion.familia_promocional)

    elasticidades = preparar_elasticidades_cruzadas(
        tabla_elasticidades=tabla_elasticidades, ean_focales=ean_requeridos
    )

    return FuentesModeloPreparadas(
        historial=preparar_historial(historial, ean_requeridos, columnas_familia),
        caracterizacion=preparar_caracterizacion(caracterizacion, ean_requeridos),
        promociones_futuras=promociones_preparadas,
        columnas_familia=columnas_familia,
        elasticidades=elasticidades,
        historial_sustitutos=preparar_historial_sustitutos(
            historial_sustitutos=historial_sustitutos,
            ean_sustitutos=pd.Index(elasticidades['EAN_RELACIONADO'].unique()),
            columnas_familia=columnas_familia,
        ),
    )


# =========================================================================
# 6. ELEGIBILIDAD
# =========================================================================
def evaluar_elegibilidad(
    caracterizacion_ean: pd.Series | None,
    configuracion: ConfiguracionModeloPromo,
) -> ResultadoElegibilidad:
    """Reglas: existe en caracterización, no es nuevo, días mínimos."""
    if caracterizacion_ean is None:
        return ResultadoElegibilidad(
            elegible=False, es_producto_nuevo=False, dias_con_venta=0,
            estado_historial='Sin caracterización',
            comentario='EAN no encontrado en caracterización',
        )

    dias_con_venta = convertir_float(caracterizacion_ean.get('DIAS_CON_VENTA', 0))
    dias_con_venta = 0 if np.isnan(dias_con_venta) else int(dias_con_venta)

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
            elegible=False, es_producto_nuevo=es_producto_nuevo,
            dias_con_venta=dias_con_venta, estado_historial='Inválido',
            comentario='; '.join(causas_exclusion),
        )

    return ResultadoElegibilidad(
        elegible=True, es_producto_nuevo=False, dias_con_venta=dias_con_venta,
        estado_historial='Válido', comentario='-',
    )


# =========================================================================
# 7. RÉGIMEN PROMOCIONAL
# =========================================================================
def determinar_uso_mecanica(
    caracterizacion_ean: pd.Series,
    configuracion: ConfiguracionModeloPromo,
) -> bool:
    """Mecánica habilitada global y por EAN, con mecánicas suficientes."""
    if not configuracion.incluir_mecanica:
        return False

    if not es_valor_afirmativo(caracterizacion_ean.get('INCLUIR_MECANICA'), configuracion):
        return False

    numero_mecanicas = convertir_float(caracterizacion_ean.get('N_MECANICAS_VALIDAS'))

    if np.isnan(numero_mecanicas):
        numero_mecanicas = len(
            _parsear_mecanicas_validas(caracterizacion_ean.get('MECANICAS_VALIDAS'))
        )

    return int(numero_mecanicas) >= configuracion.minimo_mecanicas_distintas


def clasificar_regimen_promocional(
    caracterizacion_ean: pd.Series,
    configuracion: ConfiguracionModeloPromo,
) -> RegimenPromocional:
    """0 -> BASELINE; (0, bajo] -> BASE_UPLIFT; (bajo, alto] ->
    BASE_UPLIFT_REGULARIZADO; (alto, 100] -> BASELINE_PROMOCIONAL."""
    intensidad = convertir_float(caracterizacion_ean.get('INTENSIDAD_PROMOCIONAL', 0.0))
    intensidad = 0.0 if np.isnan(intensidad) else float(np.clip(intensidad, 0.0, 100.0))

    usar_mecanica = determinar_uso_mecanica(caracterizacion_ean, configuracion)

    if intensidad == 0.0:
        return RegimenPromocional(
            nombre='BASELINE', intensidad_promocional=intensidad,
            usar_flag_promocion=False, usar_porcentaje_descuento=False,
            usar_mecanica=False, usar_configuracion_regularizada=False,
            baseline_claro=True, tipo_baseline='BASELINE_OBSERVADO',
            confianza_baseline='ALTA', comentario_baseline='-',
        )

    if intensidad <= configuracion.umbral_intensidad_baja:
        return RegimenPromocional(
            nombre='BASE_UPLIFT', intensidad_promocional=intensidad,
            usar_flag_promocion=configuracion.incluir_promocion,
            usar_porcentaje_descuento=configuracion.incluir_descuento,
            usar_mecanica=usar_mecanica, usar_configuracion_regularizada=False,
            baseline_claro=True, tipo_baseline='BASELINE_CONVENCIONAL',
            confianza_baseline='ALTA', comentario_baseline='-',
        )

    if intensidad <= configuracion.umbral_intensidad_alta:
        return RegimenPromocional(
            nombre='BASE_UPLIFT_REGULARIZADO', intensidad_promocional=intensidad,
            usar_flag_promocion=configuracion.incluir_promocion,
            usar_porcentaje_descuento=configuracion.incluir_descuento,
            usar_mecanica=usar_mecanica, usar_configuracion_regularizada=True,
            baseline_claro=True, tipo_baseline='BASELINE_CONVENCIONAL',
            confianza_baseline='MEDIA',
            comentario_baseline=(
                'Baseline estimado con menor disponibilidad relativa '
                'de observaciones sin promoción'
            ),
        )

    return RegimenPromocional(
        nombre='BASELINE_PROMOCIONAL', intensidad_promocional=intensidad,
        usar_flag_promocion=False, usar_porcentaje_descuento=False,
        usar_mecanica=False, usar_configuracion_regularizada=False,
        baseline_claro=False, tipo_baseline='CONTRAFACTUAL_PRECIO_MODAL',
        confianza_baseline='BAJA',
        comentario_baseline=(
            'Baseline referencial por alta intensidad promocional; '
            'existe evidencia limitada de ventas sin promoción'
        ),
    )


def evaluar_ean(
    ean: str,
    caracterizacion: pd.DataFrame,
    configuracion: ConfiguracionModeloPromo,
) -> tuple[ResultadoElegibilidad, RegimenPromocional | None]:
    """Elegibilidad y régimen de un EAN."""
    if ean not in caracterizacion.index:
        return evaluar_elegibilidad(None, configuracion), None

    caracterizacion_ean = caracterizacion.loc[ean]
    elegibilidad = evaluar_elegibilidad(caracterizacion_ean, configuracion)

    if not elegibilidad.elegible:
        return elegibilidad, None

    return elegibilidad, clasificar_regimen_promocional(caracterizacion_ean, configuracion)


# =========================================================================
# 8. VARIABLES: DÍA DE SEMANA, MECÁNICA, PESOS
# =========================================================================
def agregar_dummies_dia_semana(
    historial: pd.DataFrame,
    columna_fecha: str = 'P_DATE',
) -> pd.DataFrame:
    """Dummies de día de semana (lunes = referencia)."""
    resultado = historial.copy()
    dia_semana = pd.to_datetime(resultado[columna_fecha], errors='raise').dt.dayofweek

    for nombre_columna, codigo_dia in MAPA_DIAS_SEMANA.items():
        resultado[nombre_columna] = dia_semana.eq(codigo_dia).astype('int8')

    return resultado


def obtener_mecanicas_validas(
    historial_ean: pd.DataFrame,
    columna_mecanica: str,
    columna_flag_promocion: str,
    configuracion: ConfiguracionModeloPromo,
) -> list[str]:
    """Mecánicas con soporte histórico suficiente."""
    mascara_promocion = pd.to_numeric(
        historial_ean[columna_flag_promocion], errors='coerce'
    ).fillna(0).eq(1)

    conteo_mecanicas = normalizar_nombre_mecanica(
        historial_ean.loc[mascara_promocion, columna_mecanica]
    ).value_counts()

    mecanicas_validas = [
        valor
        for valor in conteo_mecanicas[
            conteo_mecanicas >= configuracion.minimo_observaciones_por_mecanica
        ].index
        if valor != 'SIN_MECANICA'
    ]

    if len(mecanicas_validas) < configuracion.minimo_mecanicas_distintas:
        return []

    return sorted(mecanicas_validas)


def agregar_variables_mecanica(
    datos_ean: pd.DataFrame,
    columna_mecanica: str,
    columna_flag_promocion: str,
    mecanicas_validas: list[str],
) -> tuple[pd.DataFrame, list[str]]:
    """Dummies de mecánica (SIN_MECANICA = referencia; raras -> OTRA)."""
    resultado = datos_ean.reset_index(drop=True).copy()

    mecanica_original = normalizar_nombre_mecanica(resultado[columna_mecanica])
    es_promocion = pd.to_numeric(
        resultado[columna_flag_promocion], errors='coerce'
    ).fillna(0).eq(1)

    mecanica_agrupada = mecanica_original.where(es_promocion, 'SIN_MECANICA')
    mecanica_agrupada = mecanica_agrupada.where(
        mecanica_agrupada.isin(mecanicas_validas) | mecanica_agrupada.eq('SIN_MECANICA'),
        'MECANICA_OTRA',
    )

    variables_mecanica = pd.get_dummies(
        pd.Categorical(
            mecanica_agrupada,
            categories=['SIN_MECANICA', *mecanicas_validas, 'MECANICA_OTRA'],
        ),
        prefix='MECANICA',
        dtype='int8',
    ).drop(columns='MECANICA_SIN_MECANICA', errors='ignore')

    resultado = pd.concat([resultado, variables_mecanica], axis=1)

    return resultado, variables_mecanica.columns.tolist()


def calcular_pesos_recencia(
    fechas: pd.Series,
    configuracion: ConfiguracionModeloPromo,
) -> np.ndarray | None:
    """Pesos exponenciales por antigüedad (promedio 1)."""
    if not configuracion.usar_peso_recencia or fechas.empty:
        return None

    antiguedad_dias = (fechas.max() - fechas).dt.days.to_numpy(dtype='float64')
    pesos = np.exp(-configuracion.tasa_decaimiento_recencia * antiguedad_dias)

    return pesos / pesos.mean() if pesos.mean() > 0 else pesos


def construir_pesos_temporales(
    fechas: pd.Series,
    configuracion: ConfiguracionModeloPromo,
) -> np.ndarray | None:
    """Pesos cronológicos opcionales (crecen con la recencia)."""
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
        raise ValueError(msg)

    orden_temporal = (
        pd.to_datetime(fechas, errors='raise').rank(method='dense').to_numpy(dtype='float64')
    )

    if metodo == 'lineal':
        pesos = orden_temporal
    elif metodo == 'raiz':
        pesos = np.sqrt(orden_temporal)
    elif metodo == 'logaritmico':
        pesos = np.log1p(orden_temporal)
    else:
        pesos = np.exp(
            -configuracion.tasa_decaimiento_exponencial
            * (orden_temporal.max() - orden_temporal)
        )

    if configuracion.normalizar_pesos_temporales and pesos.mean() > 0:
        pesos = pesos / pesos.mean()

    return pesos


def combinar_pesos(
    fechas: pd.Series,
    configuracion: ConfiguracionModeloPromo,
) -> np.ndarray | None:
    """Combina pesos temporales y de recencia (antes recencia se ignoraba)
    """
    pesos_temporales = construir_pesos_temporales(fechas, configuracion)
    pesos_recencia = calcular_pesos_recencia(fechas, configuracion)

    if pesos_temporales is None:
        return pesos_recencia

    if pesos_recencia is None:
        return pesos_temporales

    pesos = pesos_temporales * pesos_recencia

    return pesos / pesos.mean() if pesos.mean() > 0 else pesos


def seleccionar_columnas_modelo(
    regimen: RegimenPromocional,
    columnas_familia: ColumnasFamiliaPromocional,
    configuracion: ConfiguracionModeloPromo,
    columnas_mecanica: list[str],
) -> list[str]:
    """Lista final de variables explicativas."""
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


def calcular_indice_corte_temporal(numero_observaciones: int, proporcion_test: float) -> int:
    """Posición del corte train/test (al menos 1 obs por lado)."""
    if numero_observaciones < 2:
        msg = 'Se requieren al menos dos observaciones para la división temporal.'
        raise ValueError(msg)

    numero_test = max(1, int(np.ceil(numero_observaciones * proporcion_test)))
    numero_test = min(numero_test, numero_observaciones - 1)

    return numero_observaciones - numero_test


def separar_historial_entrenamiento(
    historial_ean: pd.DataFrame,
    fecha_inicio_promocion: pd.Timestamp,
    configuracion: ConfiguracionModeloPromo,
) -> pd.DataFrame:
    """Historial de entrenamiento: P_DATE <= T0 - 1."""
    if not configuracion.entrenar_hasta_inicio_promocion:
        return historial_ean

    fecha_corte = pd.Timestamp(fecha_inicio_promocion).normalize()

    return historial_ean.loc[historial_ean[configuracion.columna_fecha].lt(fecha_corte)]


# =========================================================================
# 9. PREPARACIÓN DE ENTRENAMIENTO POR EAN
# =========================================================================
def preparar_datos_entrenamiento(
    historial_ean: pd.DataFrame,
    regimen: RegimenPromocional,
    columnas_familia: ColumnasFamiliaPromocional,
    configuracion: ConfiguracionModeloPromo,
) -> DatosEntrenamiento:
    """Matrices train/test de un EAN (historial ya cortado en T0 - 1)."""
    datos_modelo = (
        historial_ean.sort_values(configuracion.columna_fecha, kind='stable')
        .reset_index(drop=True)
        .copy()
    )

    if configuracion.incluir_dia_semana:
        datos_modelo = agregar_dummies_dia_semana(datos_modelo, configuracion.columna_fecha)

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
            comentario_mecanica = 'Mecánica incluida con soporte histórico suficiente'
        else:
            usar_mecanica = False
            comentario_mecanica = (
                'Mecánica excluida por falta de categorías u observaciones suficientes'
            )
    else:
        comentario_mecanica = 'Mecánica no requerida para el régimen'

    columnas_modelo = seleccionar_columnas_modelo(
        regimen=regimen,
        columnas_familia=columnas_familia,
        configuracion=configuracion,
        columnas_mecanica=columnas_mecanica if usar_mecanica else [],
    )

    columnas_faltantes = set(columnas_modelo).difference(datos_modelo.columns)

    if columnas_faltantes:
        msg = f'No fue posible construir las variables: {sorted(columnas_faltantes)}'
        raise ValueError(msg)

    matriz_variables = (
        datos_modelo[columnas_modelo].apply(pd.to_numeric, errors='coerce').astype('float32')
    )
    objetivo = pd.to_numeric(
        datos_modelo[configuracion.columna_target], errors='coerce'
    ).astype('float32')
    fechas = datos_modelo[configuracion.columna_fecha]

    mascara_valida = objetivo.notna()
    matriz_variables = matriz_variables.loc[mascara_valida].reset_index(drop=True)
    objetivo = objetivo.loc[mascara_valida].reset_index(drop=True)
    fechas = fechas.loc[mascara_valida].reset_index(drop=True)

    indice_corte = calcular_indice_corte_temporal(len(objetivo), configuracion.proporcion_test)

    fechas_train = fechas.iloc[:indice_corte].reset_index(drop=True)
    fechas_test = fechas.iloc[indice_corte:].reset_index(drop=True)

    return DatosEntrenamiento(
        X_train=matriz_variables.iloc[:indice_corte].reset_index(drop=True),
        y_train=objetivo.iloc[:indice_corte].reset_index(drop=True),
        X_test=matriz_variables.iloc[indice_corte:].reset_index(drop=True),
        y_test=objetivo.iloc[indice_corte:].reset_index(drop=True),
        pesos_train=combinar_pesos(fechas_train, configuracion),
        fechas_train=fechas_train,
        fechas_test=fechas_test,
        columnas_modelo=columnas_modelo,
        columnas_mecanica=columnas_mecanica if usar_mecanica else [],
        mecanicas_validas=mecanicas_validas if usar_mecanica else [],
        fecha_inicio_train=fechas_train.min(),
        fecha_fin_train=fechas_train.max(),
        fecha_inicio_test=fechas_test.min() if not fechas_test.empty else None,
        fecha_fin_test=fechas_test.max() if not fechas_test.empty else None,
        numero_observaciones=len(objetivo),
        numero_observaciones_train=len(fechas_train),
        numero_observaciones_test=len(fechas_test),
        usar_mecanica=usar_mecanica,
        comentario_mecanica=comentario_mecanica,
    )


# =========================================================================
# 10. MÉTRICAS Y BENCHMARK
# =========================================================================
def calcular_metricas_modelo(
    valores_reales: pd.Series | np.ndarray,
    valores_predichos: np.ndarray,
    configuracion: ConfiguracionModeloPromo,
    benchmark: np.ndarray | None = None,
) -> MetricasModelo:
    """WMAPE, Bias, MAE, Coverage y Win Rate."""
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
    error_absoluto_total = float(errores_absolutos.sum())

    wmape = error_absoluto_total / suma_real if suma_real > 0 else np.nan
    bias = float(errores.sum()) / suma_real if suma_real > 0 else np.nan

    tolerancia = np.maximum(
        configuracion.tolerancia_cobertura_absoluta,
        configuracion.tolerancia_cobertura_relativa * np.abs(reales),
    )

    win_rate = None

    if benchmark is not None:
        benchmark_valido = np.asarray(benchmark, dtype='float64')[mascara_valida]
        mascara_benchmark = np.isfinite(benchmark_valido)

        if mascara_benchmark.any():
            win_rate = float(np.mean(
                errores_absolutos[mascara_benchmark]
                < np.abs(benchmark_valido[mascara_benchmark] - reales[mascara_benchmark])
            ))

    return MetricasModelo(
        wmape=float(wmape), bias=float(bias),
        mae=float(errores_absolutos.mean()),
        coverage=float(np.mean(errores_absolutos <= tolerancia)),
        win_rate=win_rate, suma_real=suma_real,
        suma_predicha=float(predichos.sum()),
        error_absoluto_total=error_absoluto_total,
        numero_observaciones=int(reales.size),
        numero_predicciones_validas=int(reales.size),
    )


def construir_benchmark_dia_semana(
    fechas_train: pd.Series,
    target_train: pd.Series,
    fechas_objetivo: pd.Series,
) -> np.ndarray:
    """Benchmark ingenuo: promedio histórico por día de semana."""
    target = pd.Series(np.asarray(target_train, dtype='float64'))
    dias_train = pd.to_datetime(fechas_train, errors='raise').dt.dayofweek.to_numpy()

    promedio_por_dia = target.groupby(dias_train).mean()
    dias_objetivo = pd.to_datetime(fechas_objetivo, errors='raise').dt.dayofweek

    return (
        dias_objetivo.map(promedio_por_dia)
        .fillna(float(target.mean()))
        .to_numpy(dtype='float64')
    )


# =========================================================================
# 11. CONSTRUCCIÓN Y ENTRENAMIENTO DEL MODELO
# =========================================================================
def construir_modelo_hist_gradient_boosting(
    regimen: RegimenPromocional,
    configuracion: ConfiguracionModeloPromo,
) -> HistGradientBoostingRegressor:
    """Estimador con configuración regular o conservadora."""
    regularizado = regimen.usar_configuracion_regularizada

    return HistGradientBoostingRegressor(
        loss=configuracion.loss,
        learning_rate=configuracion.learning_rate,
        max_iter=configuracion.max_iter,
        max_leaf_nodes=(
            configuracion.max_leaf_nodes_regularizado
            if regularizado else configuracion.max_leaf_nodes
        ),
        max_depth=configuracion.max_depth,
        min_samples_leaf=(
            configuracion.min_samples_leaf_regularizado
            if regularizado else configuracion.min_samples_leaf
        ),
        l2_regularization=(
            configuracion.l2_regularization_regularizado
            if regularizado else configuracion.l2_regularization
        ),
        max_bins=configuracion.max_bins,
        early_stopping=configuracion.early_stopping,
        validation_fraction=configuracion.validation_fraction,
        n_iter_no_change=configuracion.n_iter_no_change,
        tol=configuracion.tol,
        random_state=configuracion.random_state,
    )


def entrenar_modelo_producto(
    datos_entrenamiento: DatosEntrenamiento,
    regimen: RegimenPromocional,
    configuracion: ConfiguracionModeloPromo,
) -> ResultadoEntrenamiento:
    """Validación (train) + productivo (train + test, todo <= T0 - 1)."""
    modelo_validacion = construir_modelo_hist_gradient_boosting(regimen, configuracion)
    modelo_validacion.fit(
        datos_entrenamiento.X_train,
        datos_entrenamiento.y_train,
        sample_weight=datos_entrenamiento.pesos_train,
    )

    predicciones_train = np.clip(modelo_validacion.predict(datos_entrenamiento.X_train), 0.0, None)
    predicciones_test = np.clip(modelo_validacion.predict(datos_entrenamiento.X_test), 0.0, None)

    benchmark_test = construir_benchmark_dia_semana(
        datos_entrenamiento.fechas_train,
        datos_entrenamiento.y_train,
        datos_entrenamiento.fechas_test,
    )

    metricas_train = calcular_metricas_modelo(
        datos_entrenamiento.y_train, predicciones_train, configuracion
    )
    metricas_test = calcular_metricas_modelo(
        datos_entrenamiento.y_test, predicciones_test, configuracion, benchmark_test
    )

    fechas_completas = pd.concat(
        [datos_entrenamiento.fechas_train, datos_entrenamiento.fechas_test],
        ignore_index=True,
    )
    pesos_productivo = combinar_pesos(fechas_completas, configuracion)

    if configuracion.reentrenar_con_historial_completo:
        modelo_productivo = construir_modelo_hist_gradient_boosting(regimen, configuracion)
        modelo_productivo.fit(
            pd.concat(
                [datos_entrenamiento.X_train, datos_entrenamiento.X_test], ignore_index=True
            ),
            pd.concat(
                [datos_entrenamiento.y_train, datos_entrenamiento.y_test], ignore_index=True
            ),
            sample_weight=pesos_productivo,
        )
    else:
        modelo_productivo = modelo_validacion

    return ResultadoEntrenamiento(
        modelo_validacion=modelo_validacion,
        modelo_productivo=modelo_productivo,
        metricas_train=metricas_train,
        metricas_test=metricas_test,
        predicciones_train=predicciones_train,
        predicciones_test=predicciones_test,
        benchmark_test=benchmark_test,
        columnas_modelo=datos_entrenamiento.columnas_modelo.copy(),
        parametros_modelo={
            'loss': configuracion.loss,
            'learning_rate': configuracion.learning_rate,
            'max_iter': configuracion.max_iter,
            'usar_configuracion_regularizada': regimen.usar_configuracion_regularizada,
            'usar_pesos': pesos_productivo is not None,
        },
        iteraciones_validacion=int(modelo_validacion.n_iter_),
        iteraciones_productivo=int(modelo_productivo.n_iter_),
        uso_pesos_temporales=pesos_productivo is not None,
        metodo_pesos_temporales=(
            configuracion.metodo_pesos_temporales
            if pesos_productivo is not None else 'sin_pesos'
        ),
        fecha_fin_entrenamiento=fechas_completas.max(),
    )


# =========================================================================
# 12. CALENDARIO, VARIABLES FUTURAS Y DATOS OBSERVADOS
# =========================================================================
def construir_calendario_promocion(promocion_ean: pd.Series) -> pd.DataFrame:
    """Una fila por día de la promoción."""
    calendario = pd.DataFrame({
        'P_DATE': pd.date_range(
            start=promocion_ean['FECHA_INICIO_DE_PROMOCION'],
            end=promocion_ean['FECHA_FIN_DE_PROMOCION'],
            freq='D',
        )
    })
    calendario['N_PROMOCION'] = promocion_ean['N_PROMOCION']
    calendario['EAN'] = str(promocion_ean['EAN'])

    return calendario


def agregar_variables_temporales_futuras(
    calendario: pd.DataFrame,
    historial_entrenamiento: pd.DataFrame,
    calendario_futuro: pd.DataFrame | None = None,
) -> tuple[pd.DataFrame, bool]:
    """Tendencia, estacionalidad anual, feriados y día de semana."""
    resultado = calendario.copy()

    fecha_origen = pd.to_datetime(historial_entrenamiento['P_DATE']).min()
    resultado['TENDENCIA_LINEAL'] = (resultado['P_DATE'] - fecha_origen).dt.days.astype('int32')

    dia_anio = resultado['P_DATE'].dt.dayofyear
    resultado['SIN_ANUAL'] = np.sin(2.0 * np.pi * dia_anio / 365.25)
    resultado['COS_ANUAL'] = np.cos(2.0 * np.pi * dia_anio / 365.25)

    if calendario_futuro is not None:
        calendario_auxiliar = (
            calendario_futuro[['P_DATE', 'FLAG_FERIADO', 'FLAG_PRE_FERIADO']]
            .drop_duplicates('P_DATE', keep='last')
        )
        resultado = resultado.merge(
            calendario_auxiliar, on='P_DATE', how='left', validate='one_to_one'
        )
        calendario_incompleto = bool(resultado['FLAG_FERIADO'].isna().any())
        resultado[['FLAG_FERIADO', 'FLAG_PRE_FERIADO']] = (
            resultado[['FLAG_FERIADO', 'FLAG_PRE_FERIADO']].fillna(0).astype('int8')
        )
    else:
        resultado['FLAG_FERIADO'] = np.int8(0)
        resultado['FLAG_PRE_FERIADO'] = np.int8(0)
        calendario_incompleto = True

    return agregar_dummies_dia_semana(resultado), calendario_incompleto


def incorporar_datos_observados(
    calendario: pd.DataFrame,
    historial_observado: pd.DataFrame,
    configuracion: ConfiguracionModeloPromo,
    fecha_corte_observada: pd.Timestamp | None = None,
) -> pd.DataFrame:
    """Incorpora la venta real hasta la fecha de corte global.

    Días <= corte con dato: OBSERVADO; <= corte sin dato:
    PASADO_SIN_REGISTRO; > corte: PROYECTADO.
    """
    resultado = calendario.copy()

    ultima_fecha = (
        pd.Timestamp(fecha_corte_observada)
        if fecha_corte_observada is not None
        else historial_observado['P_DATE'].max()
    )

    historial_target = (
        historial_observado.loc[
            historial_observado['P_DATE'].le(ultima_fecha),
            ['P_DATE', configuracion.columna_target],
        ]
        .drop_duplicates('P_DATE', keep='last')
        .rename(columns={configuracion.columna_target: 'CANTIDAD_OBSERVADA'})
    )

    resultado = resultado.merge(historial_target, on='P_DATE', how='left', validate='one_to_one')

    mascara_observada = resultado['CANTIDAD_OBSERVADA'].notna()
    mascara_futura = resultado['P_DATE'].gt(ultima_fecha)
    mascara_sin_registro = ~mascara_observada & ~mascara_futura

    resultado['ORIGEN_CANTIDAD'] = np.select(
        [mascara_observada, mascara_futura, mascara_sin_registro],
        ['OBSERVADO', 'PROYECTADO', 'PASADO_SIN_REGISTRO'],
        default='PROYECTADO',
    )
    resultado['FLAG_DATO_REAL'] = mascara_observada.astype('int8')
    resultado['FLAG_DIA_FUTURO'] = mascara_futura.astype('int8')
    resultado['FLAG_DIA_PASADO_SIN_DATO'] = mascara_sin_registro.astype('int8')

    if configuracion.tratar_pasado_sin_registro_como_cero:
        resultado.loc[mascara_sin_registro, 'CANTIDAD_OBSERVADA'] = 0.0

    return resultado


def asignar_mecanicas_escenario(
    escenario: pd.DataFrame,
    columnas_modelo: list[str],
    nombre_mecanica: object,
    activar_mecanica: bool,
) -> pd.DataFrame:
    """Activa la dummy de mecánica del escenario (baseline en cero)."""
    resultado = escenario.copy()
    columnas_mecanica = [columna for columna in columnas_modelo if columna.startswith('MECANICA_')]

    for columna in columnas_mecanica:
        resultado[columna] = np.int8(0)

    if not activar_mecanica or nombre_mecanica is None or pd.isna(nombre_mecanica):
        return resultado

    columna_objetivo = 'MECANICA_' + str(nombre_mecanica).strip().upper().replace(' ', '_')

    if columna_objetivo in columnas_mecanica:
        resultado[columna_objetivo] = np.int8(1)
    elif 'MECANICA_OTRA' in columnas_mecanica:
        resultado['MECANICA_OTRA'] = np.int8(1)

    return resultado


# =========================================================================
# 13. ESCENARIOS Y PREDICCIÓN
# =========================================================================
def construir_escenarios_promocional_baseline(
    promocion_ean: pd.Series,
    historial_entrenamiento: pd.DataFrame,
    historial_observado: pd.DataFrame,
    resultado_entrenamiento: ResultadoEntrenamiento,
    regimen: RegimenPromocional,
    columnas_familia: ColumnasFamiliaPromocional,
    configuracion: ConfiguracionModeloPromo,
    calendario_futuro: pd.DataFrame | None = None,
    fecha_corte_observada: pd.Timestamp | None = None,
) -> tuple[pd.DataFrame, pd.DataFrame, bool]:
    """Escenario promocional (precio promo) y baseline (precio modal)."""
    calendario, calendario_incompleto = agregar_variables_temporales_futuras(
        calendario=construir_calendario_promocion(promocion_ean),
        historial_entrenamiento=historial_entrenamiento,
        calendario_futuro=calendario_futuro,
    )
    calendario = incorporar_datos_observados(
        calendario=calendario,
        historial_observado=historial_observado,
        configuracion=configuracion,
        fecha_corte_observada=fecha_corte_observada,
    )

    precio_promocional = float(promocion_ean['PRECIO_PROMOCIONAL'])
    precio_modal = float(promocion_ean['PRECIO_MODAL'])
    porcentaje_descuento = max(convertir_float(promocion_ean['PORCENTAJE_DESCUENTO']), 0.0)
    porcentaje_descuento = 0.0 if np.isnan(porcentaje_descuento) else porcentaje_descuento

    columnas_modelo = resultado_entrenamiento.columnas_modelo

    escenario_promocional = calendario.copy()
    escenario_promocional[configuracion.columna_precio] = precio_promocional

    if columnas_familia.flag_promocion in columnas_modelo:
        escenario_promocional[columnas_familia.flag_promocion] = np.int8(1)

    if columnas_familia.porcentaje_descuento in columnas_modelo:
        escenario_promocional[columnas_familia.porcentaje_descuento] = porcentaje_descuento

    escenario_promocional = asignar_mecanicas_escenario(
        escenario=escenario_promocional,
        columnas_modelo=columnas_modelo,
        nombre_mecanica=promocion_ean.get('DESCRIPCION_EVENTO_PROMOCIONAL', pd.NA),
        activar_mecanica=regimen.usar_mecanica,
    )
    escenario_promocional['ESCENARIO'] = 'PROMOCIONAL'
    escenario_promocional['PRECIO_ESCENARIO'] = precio_promocional

    escenario_baseline = calendario.copy()
    escenario_baseline[configuracion.columna_precio] = precio_modal

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
    escenario_baseline['PRECIO_ESCENARIO'] = precio_modal

    for columna in columnas_modelo:
        if columna not in escenario_promocional.columns:
            escenario_promocional[columna] = 0.0

        if columna not in escenario_baseline.columns:
            escenario_baseline[columna] = 0.0

    return escenario_promocional, escenario_baseline, calendario_incompleto


def predecir_escenario(
    escenario: pd.DataFrame,
    resultado_entrenamiento: ResultadoEntrenamiento,
    usar_observados_en_total: bool,
    configuracion: ConfiguracionModeloPromo,
) -> pd.DataFrame:
    """Predice con el modelo productivo; el promocional usa lo observado.
    """
    resultado = escenario.copy()

    resultado['CANTIDAD_MODELO'] = np.clip(
        resultado_entrenamiento.modelo_productivo.predict(
            resultado[resultado_entrenamiento.columnas_modelo].astype('float64')
        ),
        0.0, None,
    )

    if not usar_observados_en_total:
        resultado['CANTIDAD_FINAL'] = resultado['CANTIDAD_MODELO']
        return resultado

    cantidad_final = resultado['CANTIDAD_OBSERVADA'].where(
        resultado['FLAG_DATO_REAL'].eq(1), resultado['CANTIDAD_MODELO']
    )

    if configuracion.tratar_pasado_sin_registro_como_cero:
        cantidad_final = cantidad_final.where(resultado['FLAG_DIA_PASADO_SIN_DATO'].eq(0), 0.0)

    resultado['CANTIDAD_FINAL'] = cantidad_final

    return resultado


# =========================================================================
# 14. UTILIDADES DE ACCESO SEGURO
# =========================================================================
def obtener_valor(
    registro: pd.Series | Mapping | None,
    columna: str,
    valor_default: object = '-',
) -> object:
    """Valor seguro desde Series o Mapping; default si falta o es nulo."""
    if registro is None:
        return valor_default

    if isinstance(registro, pd.Series):
        valor = registro[columna] if columna in registro.index else valor_default
    elif isinstance(registro, Mapping):
        valor = registro.get(columna, valor_default)
    else:
        msg = f'registro debe ser pd.Series, Mapping o None: {type(registro).__name__}.'
        raise TypeError(msg)

    if valor is None:
        return valor_default

    if isinstance(valor, list | tuple | dict | set | np.ndarray):
        return valor

    return valor_default if pd.isna(valor) else valor


def redondear_total(valor: object, decimales: int = 2) -> float | str:
    """Redondea; - si no es numérico."""
    numero = convertir_float(valor)

    return '-' if np.isnan(numero) else round(numero, decimales)


def obtener_motivo_elegibilidad(elegibilidad: ResultadoElegibilidad) -> str:
    """Motivo textual de no elegibilidad."""
    comentario = str(elegibilidad.comentario or '').strip()

    return comentario or 'EAN no elegible o sin régimen asignado.'


def obtener_metadata_ean(caracterizacion_ean: pd.Series | None) -> dict[str, object]:
    """Metadata descriptiva desde caracterización."""
    return {
        'Categoría': obtener_valor(caracterizacion_ean, 'CATEGORY_DESCRIPTION'),
        'Subcategoría': obtener_valor(caracterizacion_ean, 'SUB_CATEGORY_DESCRIPTION'),
        'Descripcion': obtener_valor(caracterizacion_ean, 'PRODUCT_DESCRIPTION'),
        'Material': obtener_valor(caracterizacion_ean, 'MATERIAL'),
        'UMV': obtener_valor(caracterizacion_ean, 'SALES_UOM'),
        'TIPOLOGIA_DEMANDA': obtener_valor(caracterizacion_ean, 'TIPOLOGIA_DEMANDA'),
        'SEGMENTO_ABCD': obtener_valor(caracterizacion_ean, 'SEGMENTO_ABCD'),
        'ADI': obtener_valor(caracterizacion_ean, 'ADI'),
        'CV2': obtener_valor(caracterizacion_ean, 'CV2'),
        'Estable': obtener_valor(caracterizacion_ean, 'CLASIFICACION_ADI_CV2'),
        'Intensidad Promocional': obtener_valor(caracterizacion_ean, 'INTENSIDAD_PROMOCIONAL'),
        'DIAS_CON_VENTA': obtener_valor(caracterizacion_ean, 'DIAS_CON_VENTA'),
    }


# =========================================================================
# 15. RESUMEN DE ESCENARIOS
# =========================================================================
def construir_resumen_escenarios(
    detalle_promocional: pd.DataFrame,
    detalle_baseline: pd.DataFrame,
    promocion_ean: pd.Series,
    resultado_entrenamiento: ResultadoEntrenamiento,
    regimen: RegimenPromocional,
    calendario_incompleto: bool,
) -> dict[str, object]:
    """Resumen agregado de una combinación promoción-EAN."""
    total_promocional = float(detalle_promocional['CANTIDAD_FINAL'].sum())
    total_baseline = float(detalle_baseline['CANTIDAD_FINAL'].sum())

    contiene_dias_futuros = bool(detalle_promocional['FLAG_DIA_FUTURO'].eq(1).any())
    contiene_pasados_sin_dato = bool(detalle_promocional['FLAG_DIA_PASADO_SIN_DATO'].eq(1).any())

    comentarios = []

    if not regimen.baseline_claro:
        comentarios.append(regimen.comentario_baseline)

    if contiene_dias_futuros:
        comentarios.append('Promoción no finalizada: incluye días proyectados')

    if contiene_pasados_sin_dato:
        comentarios.append('Existen días pasados sin registro histórico')

    if calendario_incompleto:
        comentarios.append('Calendario sin cobertura completa de feriados')

    metricas_train = resultado_entrenamiento.metricas_train
    metricas_test = resultado_entrenamiento.metricas_test

    return {
        'N_PROMOCION': promocion_ean['N_PROMOCION'],
        'EAN': str(promocion_ean['EAN']),
        'FECHA_INICIO_DE_PROMOCION': promocion_ean['FECHA_INICIO_DE_PROMOCION'],
        'FECHA_FIN_DE_PROMOCION': promocion_ean['FECHA_FIN_DE_PROMOCION'],
        'FECHA_FIN_ENTRENAMIENTO': resultado_entrenamiento.fecha_fin_entrenamiento,
        'CASO_MODELO': regimen.nombre,
        'CANTIDAD_PROMOCIONAL_TOTAL': total_promocional,
        'CANTIDAD_BASELINE_TOTAL': total_baseline,
        'WMAPE_TRAIN': metricas_train.wmape,
        'BIAS_TRAIN': metricas_train.bias,
        'MAE_TRAIN': metricas_train.mae,
        'COVERAGE_TRAIN': metricas_train.coverage,
        'WMAPE_TEST': metricas_test.wmape,
        'BIAS_TEST': metricas_test.bias,
        'MAE_TEST': metricas_test.mae,
        'COVERAGE_TEST': metricas_test.coverage,
        'WIN_RATE_TEST': metricas_test.win_rate,
        'FLAG_MODELO_PRODUCTIVO': 1,
        'FLAG_CONTIENE_DIAS_FUTUROS': int(contiene_dias_futuros),
        'FLAG_BASELINE_REFERENCIAL': int(not regimen.baseline_claro),
        'COMENTARIO': '; '.join(comentarios) if comentarios else 'Proyección generada',
    }


def proyectar_promocion_ean(
    promocion_ean: pd.Series,
    historial_entrenamiento: pd.DataFrame,
    historial_observado: pd.DataFrame,
    resultado_entrenamiento: ResultadoEntrenamiento,
    regimen: RegimenPromocional,
    columnas_familia: ColumnasFamiliaPromocional,
    configuracion: ConfiguracionModeloPromo,
    calendario_futuro: pd.DataFrame | None = None,
    fecha_corte_observada: pd.Timestamp | None = None,
) -> ResultadoEscenarios:
    """Construye, predice y consolida escenarios de una promoción-EAN."""
    escenario_promocional, escenario_baseline, calendario_incompleto = (
        construir_escenarios_promocional_baseline(
            promocion_ean=promocion_ean,
            historial_entrenamiento=historial_entrenamiento,
            historial_observado=historial_observado,
            resultado_entrenamiento=resultado_entrenamiento,
            regimen=regimen,
            columnas_familia=columnas_familia,
            configuracion=configuracion,
            calendario_futuro=calendario_futuro,
            fecha_corte_observada=fecha_corte_observada,
        )
    )

    detalle_promocional = predecir_escenario(
        escenario_promocional, resultado_entrenamiento, True, configuracion  # noqa: FBT003
    )
    detalle_baseline = predecir_escenario(
        escenario_baseline, resultado_entrenamiento, False, configuracion  # noqa: FBT003
    )

    resumen = construir_resumen_escenarios(
        detalle_promocional=detalle_promocional,
        detalle_baseline=detalle_baseline,
        promocion_ean=promocion_ean,
        resultado_entrenamiento=resultado_entrenamiento,
        regimen=regimen,
        calendario_incompleto=calendario_incompleto,
    )

    detalle_diario = (
        pd.concat(
            [
                detalle_promocional[COLUMNAS_DETALLE_DIARIO],
                detalle_baseline[COLUMNAS_DETALLE_DIARIO],
            ],
            ignore_index=True,
        )
        .sort_values(['P_DATE', 'ESCENARIO'], kind='stable')
        .reset_index(drop=True)
    )

    return ResultadoEscenarios(detalle_diario=detalle_diario, resumen=resumen)


# =========================================================================
# 16. CANIBALIZACIÓN VÍA ELASTICIDADES CRUZADAS (EX POST)
# =========================================================================
def expandir_promociones_diarias(promociones: pd.DataFrame) -> pd.DataFrame:
    """Expande cada promoción-EAN a una fila por día (vectorizado)."""
    promociones_base = promociones.reset_index(drop=True)
    expandidas = promociones_base.loc[
        promociones_base.index.repeat(promociones_base['DURACION_PROMOCION_DIAS'].astype(int))
    ].copy()

    desfase_dias = expandidas.groupby(level=0).cumcount()
    expandidas['P_DATE'] = (
        expandidas['FECHA_INICIO_DE_PROMOCION'] + pd.to_timedelta(desfase_dias, unit='D')
    )

    return expandidas.reset_index(drop=True)


def filtrar_elasticidades_validas(
    elasticidades: pd.DataFrame,
    configuracion: ConfiguracionModeloPromo,
) -> tuple[pd.DataFrame, set[str], set[str], set[str]]:
    """Filtra confiabilidad, signo sustituto (E > 0) y top N.

    Returns
    -------
    (validas, focales_con_registro, focales_confiables, focales_sustitutos)
    """
    focales_con_registro = set(elasticidades['EAN_FOCAL'].dropna())

    if configuracion.confiabilidades_validas is None:
        confiables = elasticidades
    else:
        valores_validos = {
            str(valor).strip().upper() for valor in configuracion.confiabilidades_validas
        }
        confiables = elasticidades.loc[elasticidades['CONFIABILIDAD'].isin(valores_validos)]

    focales_confiables = set(confiables['EAN_FOCAL'].dropna())

    sustitutos = confiables.loc[confiables['ELASTICIDAD_CRUZADA'].gt(0)]
    focales_sustitutos = set(sustitutos['EAN_FOCAL'].dropna())

    validas = (
        sustitutos.sort_values(
            ['EAN_FOCAL', 'ELASTICIDAD_CRUZADA'], ascending=[True, False], kind='stable'
        )
        .groupby('EAN_FOCAL', sort=False)
        .head(configuracion.top_sustitutos_canibalizacion)
        [['EAN_FOCAL', 'EAN_RELACIONADO', 'ELASTICIDAD_CRUZADA']]
        .reset_index(drop=True)
    )

    return validas, focales_con_registro, focales_confiables, focales_sustitutos


def calcular_canibalizacion(
    promociones: pd.DataFrame,
    elasticidades: pd.DataFrame,
    historial_sustitutos: pd.DataFrame,
    configuracion: ConfiguracionModeloPromo,
    fecha_corte_observada: pd.Timestamp,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Canibalización por promoción-EAN focal y detalle focal-sustituto.

    Por sustituto B y día t:
        efecto_total(B,t) = sum_i E(i,B) * ln(P_promo_i / P_modal_i)
        Q_cf(B,t)         = Q_obs(B,t) * exp(-efecto_total(B,t))
        canibal(B,t)      = Q_cf(B,t) - Q_obs(B,t)
    El total se reparte entre focales según su participación en
    efecto_total.
    """
    resumen = (
        promociones[['N_PROMOCION', 'EAN', 'PRECIO_PROMOCIONAL', 'PRECIO_MODAL']]
        .drop_duplicates(['N_PROMOCION', 'EAN'])
        .rename(columns={'EAN': 'EAN_FOCAL'})
        .reset_index(drop=True)
    )
    resumen['DELTA_LN_PRECIO_FOCAL'] = np.log(
        resumen['PRECIO_PROMOCIONAL'] / resumen['PRECIO_MODAL']
    )

    detalle = pd.DataFrame(columns=COLUMNAS_DETALLE_CANIBALIZACION)

    if not configuracion.calcular_canibalizacion:
        resumen = resumen.assign(
            N_SUSTITUTOS_EVALUADOS=0, N_SUSTITUTOS_CANIBALIZADOS=0,
            UV_CANIBALIZADA=np.nan, VENTA_CANIBALIZADA=np.nan,
            COMENTARIO_CANIBALIZACION='Cálculo de canibalización desactivado',
        )
        return resumen, detalle

    validas, focales_registro, focales_confiables, focales_sustitutos = (
        filtrar_elasticidades_validas(elasticidades, configuracion)
    )

    promos_diarias = expandir_promociones_diarias(promociones).rename(
        columns={'EAN': 'EAN_FOCAL'}
    )
    promos_diarias['DELTA_LN_PRECIO_FOCAL'] = np.log(
        promos_diarias['PRECIO_PROMOCIONAL'] / promos_diarias['PRECIO_MODAL']
    )

    panel = (
        promos_diarias.loc[
            promos_diarias['DELTA_LN_PRECIO_FOCAL'].lt(0),
            ['N_PROMOCION', 'EAN_FOCAL', 'P_DATE', 'DELTA_LN_PRECIO_FOCAL'],
        ]
        .merge(validas, on='EAN_FOCAL', how='inner')
        .sort_values('DELTA_LN_PRECIO_FOCAL', kind='stable')
        .drop_duplicates(['EAN_FOCAL', 'EAN_RELACIONADO', 'P_DATE'], keep='first')
        .reset_index(drop=True)
    )

    if not panel.empty:
        panel['EFECTO_LOG'] = panel['ELASTICIDAD_CRUZADA'] * panel['DELTA_LN_PRECIO_FOCAL']

        panel = panel.merge(
            historial_sustitutos.rename(columns={'EAN': 'EAN_RELACIONADO'}),
            on=['EAN_RELACIONADO', 'P_DATE'],
            how='left',
        )

        sustitutos_en_lote = (
            promos_diarias[['EAN_FOCAL', 'P_DATE']]
            .drop_duplicates()
            .rename(columns={'EAN_FOCAL': 'EAN_RELACIONADO'})
            .assign(EN_PROMO_LOTE=1)
        )
        panel = panel.merge(sustitutos_en_lote, on=['EAN_RELACIONADO', 'P_DATE'], how='left')

        dia_con_dato = panel['P_DATE'].le(fecha_corte_observada)
        en_promocion = (
            panel['FLAG_PROMO_SUSTITUTO'].fillna(0).eq(1) | panel['EN_PROMO_LOTE'].notna()
        )
        excluido = (
            en_promocion & dia_con_dato
            if configuracion.excluir_sustitutos_en_promocion
            else pd.Series(False, index=panel.index)  # noqa: FBT003
        )
        evaluado = dia_con_dato & ~excluido

        panel['DIA_SIN_DATO'] = (~dia_con_dato).astype('int8')
        panel['DIA_EXCLUIDO_PROMO'] = excluido.astype('int8')
        panel['DIA_EVALUADO'] = evaluado.astype('int8')

        # Día sin registro del sustituto (dentro del corte) = venta 0.
        cantidad_observada = panel['CANTIDAD_TOTAL'].fillna(0.0).where(evaluado, 0.0)

        precio_mediano = historial_sustitutos.groupby('EAN')['PRECIO_PROMEDIO'].median()
        precio_sustituto = panel['PRECIO_PROMEDIO'].fillna(
            panel['EAN_RELACIONADO'].map(precio_mediano)
        )

        efecto_total = (
            panel['EFECTO_LOG'].where(evaluado, 0.0)
            .groupby([panel['EAN_RELACIONADO'], panel['P_DATE']])
            .transform('sum')
        )
        participacion = (panel['EFECTO_LOG'] / efecto_total).where(
            evaluado & efecto_total.lt(0), 0.0
        )
        cantidad_contrafactual = cantidad_observada * np.exp(-efecto_total)

        panel['UV_OBSERVADA_SUSTITUTO'] = cantidad_observada
        panel['UV_CANIBALIZADA'] = (cantidad_contrafactual - cantidad_observada) * participacion
        panel['VENTA_CANIBALIZADA'] = (panel['UV_CANIBALIZADA'] * precio_sustituto).fillna(0.0)

        detalle = panel.groupby(
            ['N_PROMOCION', 'EAN_FOCAL', 'EAN_RELACIONADO'], as_index=False, sort=False
        ).agg(
            ELASTICIDAD_CRUZADA=('ELASTICIDAD_CRUZADA', 'first'),
            DELTA_LN_PRECIO_FOCAL=('DELTA_LN_PRECIO_FOCAL', 'first'),
            DIAS_PROMOCION=('P_DATE', 'nunique'),
            DIAS_EVALUADOS=('DIA_EVALUADO', 'sum'),
            DIAS_EXCLUIDOS_PROMO=('DIA_EXCLUIDO_PROMO', 'sum'),
            DIAS_SIN_DATO=('DIA_SIN_DATO', 'sum'),
            UV_OBSERVADA_SUSTITUTO=('UV_OBSERVADA_SUSTITUTO', 'sum'),
            UV_CANIBALIZADA=('UV_CANIBALIZADA', 'sum'),
            VENTA_CANIBALIZADA=('VENTA_CANIBALIZADA', 'sum'),
        )[COLUMNAS_DETALLE_CANIBALIZACION]

        agregado = (
            detalle.assign(
                SUSTITUTO_EVALUADO=detalle['DIAS_EVALUADOS'].gt(0).astype(int),
                SUSTITUTO_CANIBALIZADO=detalle['UV_CANIBALIZADA'].gt(0).astype(int),
            )
            .groupby(['N_PROMOCION', 'EAN_FOCAL'], as_index=False)
            .agg(
                N_SUSTITUTOS_EVALUADOS=('SUSTITUTO_EVALUADO', 'sum'),
                N_SUSTITUTOS_CANIBALIZADOS=('SUSTITUTO_CANIBALIZADO', 'sum'),
                UV_CANIBALIZADA=('UV_CANIBALIZADA', 'sum'),
                VENTA_CANIBALIZADA=('VENTA_CANIBALIZADA', 'sum'),
            )
        )
        resumen = resumen.merge(agregado, on=['N_PROMOCION', 'EAN_FOCAL'], how='left')
    else:
        resumen = resumen.assign(
            N_SUSTITUTOS_EVALUADOS=0, N_SUSTITUTOS_CANIBALIZADOS=0,
            UV_CANIBALIZADA=np.nan, VENTA_CANIBALIZADA=np.nan,
        )

    resumen[['N_SUSTITUTOS_EVALUADOS', 'N_SUSTITUTOS_CANIBALIZADOS']] = (
        resumen[['N_SUSTITUTOS_EVALUADOS', 'N_SUSTITUTOS_CANIBALIZADOS']]
        .fillna(0).astype(int)
    )

    focal = resumen['EAN_FOCAL']
    condiciones = [
        ~focal.isin(focales_registro),
        ~focal.isin(focales_confiables),
        ~focal.isin(focales_sustitutos),
        ~resumen['DELTA_LN_PRECIO_FOCAL'].lt(0),
        resumen['N_SUSTITUTOS_EVALUADOS'].eq(0),
        resumen['UV_CANIBALIZADA'].fillna(0).le(0),
    ]
    mensajes = [
        'Sin elasticidades cruzadas reportables para el EAN focal',
        'Elasticidades cruzadas sin confiabilidad válida',
        'Sin sustitutos con elasticidad cruzada positiva',
        'Precio promocional no es menor al precio modal',
        'Sustitutos sin días evaluables (en promoción o sin dato observado)',
        'Sin canibalización registrada en el período',
    ]

    resumen['COMENTARIO_CANIBALIZACION'] = np.select(
        condiciones, mensajes, default='Canibalización calculada'
    )

    no_calculable = np.logical_or.reduce(condiciones[:5])
    sin_canibalizacion = condiciones[5] & ~no_calculable
    calculada = ~no_calculable & ~sin_canibalizacion

    resumen.loc[no_calculable, ['UV_CANIBALIZADA', 'VENTA_CANIBALIZADA']] = np.nan
    resumen.loc[sin_canibalizacion, ['UV_CANIBALIZADA', 'VENTA_CANIBALIZADA']] = 0.0
    resumen.loc[calculada, 'COMENTARIO_CANIBALIZACION'] = (
        'Canibalización calculada con '
        + resumen.loc[calculada, 'N_SUSTITUTOS_CANIBALIZADOS'].astype(str)
        + ' sustituto(s)'
    )

    return resumen, detalle


def construir_campos_canibalizacion(
    canibalizacion: Mapping | None,
    uv_incremental: object,
    venta_incremental: object,
) -> dict[str, object]:
    """Columnas de canibalización para una fila del Excel final."""
    if canibalizacion is None:
        return {
            'N Sustitutos Evaluados': 0, 'N Sustitutos Canibalizados': 0,
            'UV Canibalizada': '-', 'Venta Canibalizada': '-',
            'UV Incremental Neta': '-', 'Venta Incremental Neta': '-',
            'Comentario Canibalización': 'Canibalización no calculada',
        }

    uv_canibalizada = convertir_float(canibalizacion.get('UV_CANIBALIZADA'))
    venta_canibalizada = convertir_float(canibalizacion.get('VENTA_CANIBALIZADA'))
    uv_neta = convertir_float(uv_incremental) - uv_canibalizada
    venta_neta = convertir_float(venta_incremental) - venta_canibalizada

    return {
        'N Sustitutos Evaluados': int(canibalizacion.get('N_SUSTITUTOS_EVALUADOS', 0)),
        'N Sustitutos Canibalizados': int(canibalizacion.get('N_SUSTITUTOS_CANIBALIZADOS', 0)),
        'UV Canibalizada': redondear_total(uv_canibalizada),
        'Venta Canibalizada': redondear_total(venta_canibalizada),
        'UV Incremental Neta': redondear_total(uv_neta),
        'Venta Incremental Neta': redondear_total(venta_neta),
        'Comentario Canibalización': canibalizacion.get(
            'COMENTARIO_CANIBALIZACION', 'Canibalización no calculada'
        ),
    }


# =========================================================================
# 17. FILAS DEL EXCEL FINAL
# =========================================================================
def construir_fila_no_proyectable(
    promocion_ean: pd.Series,
    caracterizacion_ean: pd.Series | None,
    motivo: str,
    canibalizacion: Mapping | None = None,
) -> dict[str, object]:
    """Fila para un EAN no entrenado/proyectado (canibalización igual)."""
    metadata_ean = obtener_metadata_ean(caracterizacion_ean)

    fila = {
        'N° promoción': promocion_ean['N_PROMOCION'],
        'Nombre promoción': obtener_valor(promocion_ean, 'NOMBRE_PROMOCION'),
        'Categoría': metadata_ean['Categoría'],
        'Subcategoría': metadata_ean['Subcategoría'],
        'Descripcion': metadata_ean['Descripcion'],
        'Material': metadata_ean['Material'],
        'UMV': metadata_ean['UMV'],
        'EAN': obtener_valor(promocion_ean, 'EAN'),
        'R²': '-', 'Elasticidad': '-',
        'Estable': metadata_ean['Estable'],
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
        'WMAPE Train': '-', 'Bias Train': '-', 'MAE Train': '-',
        'Coverage Train': '-', 'WMAPE Test': '-', 'Bias Test': '-',
        'MAE Test': '-', 'Coverage Test': '-', 'Win Rate Test': '-',
        'Intensidad Promocional': metadata_ean['Intensidad Promocional'],
        'SEGMENTO_ABCD': metadata_ean['SEGMENTO_ABCD'],
        'ADI': redondear_total(metadata_ean['ADI'], decimales=4),
        'CV2': redondear_total(metadata_ean['CV2'], decimales=4),
        'DIAS_CON_VENTA': metadata_ean['DIAS_CON_VENTA'],
    }
    fila.update(construir_campos_canibalizacion(canibalizacion, np.nan, np.nan))

    return fila


def construir_fila_excel_final(
    resultado_escenarios: ResultadoEscenarios,
    promocion_ean: pd.Series,
    caracterizacion_ean: pd.Series,
    canibalizacion: Mapping | None = None,
) -> dict[str, object]:
    """Fila del Excel final para una promoción-EAN proyectada."""
    resumen = resultado_escenarios.resumen
    detalle = resultado_escenarios.detalle_diario

    precio_modal = float(promocion_ean['PRECIO_MODAL'])
    precio_promocional = float(promocion_ean['PRECIO_PROMOCIONAL'])

    es_promocional = detalle['ESCENARIO'].eq('PROMOCIONAL')
    es_real = detalle['FLAG_DATO_REAL'].eq(1)
    cantidad = pd.to_numeric(detalle['CANTIDAD_FINAL'], errors='coerce').fillna(0.0)

    uv_proy = float(cantidad[es_promocional].sum())
    baseline_uv = float(cantidad[~es_promocional].sum())
    uv_real = float(cantidad[es_promocional & es_real].sum())
    baseline_uv_real = float(cantidad[~es_promocional & es_real].sum())

    uv_incremental_proy = uv_proy - baseline_uv
    uv_incremental_real = uv_real - baseline_uv_real

    baseline_venta = baseline_uv * precio_modal
    venta_proy = uv_proy * precio_promocional
    venta_incremental_proy = venta_proy - baseline_venta

    venta_real = uv_real * precio_promocional
    venta_incremental_real = venta_real - baseline_uv_real * precio_modal

    metadata_ean = obtener_metadata_ean(caracterizacion_ean)

    fila = {
        'N° promoción': resumen['N_PROMOCION'],
        'Nombre promoción': obtener_valor(promocion_ean, 'NOMBRE_PROMOCION'),
        'Categoría': metadata_ean['Categoría'],
        'Subcategoría': metadata_ean['Subcategoría'],
        'Descripcion': metadata_ean['Descripcion'],
        'Material': metadata_ean['Material'],
        'UMV': metadata_ean['UMV'],
        'EAN': resumen['EAN'],
        'R²': '-', 'Elasticidad': '-',
        'Estable': metadata_ean['Estable'],
        'TIPOLOGIA_DEMANDA': metadata_ean['TIPOLOGIA_DEMANDA'],
        'Inicio Proy': resumen['FECHA_INICIO_DE_PROMOCION'],
        'Fin Proy': resumen['FECHA_FIN_DE_PROMOCION'],
        'Precio Modal': precio_modal,
        'Precio Promocional': precio_promocional,
        'Baseline_UV': redondear_total(baseline_uv),
        'UV Incremental Real': redondear_total(uv_incremental_real),
        'UV Incremental Proy': redondear_total(uv_incremental_proy),
        'UV Real': redondear_total(uv_real),
        'UV Proy': redondear_total(uv_proy),
        'Baseline Venta': redondear_total(baseline_venta),
        'Venta Incremental Real': redondear_total(venta_incremental_real),
        'Venta Incremental Proy': redondear_total(venta_incremental_proy),
        'Venta Real': redondear_total(venta_real),
        'Venta Proy': redondear_total(venta_proy),
        'Estado_Historial': resumen['FLAG_BASELINE_REFERENCIAL'],
        'Estado_Modelo': resumen['FLAG_MODELO_PRODUCTIVO'],
        'Estado_Elasticidad': '-',
        'Estado_fecha_proy': resumen['FLAG_CONTIENE_DIAS_FUTUROS'],
        'Estado_proyección': 'Proyectado',
        'Comentario': resumen['COMENTARIO'],
        'WMAPE Train': redondear_total(resumen['WMAPE_TRAIN'], 4),
        'Bias Train': redondear_total(resumen['BIAS_TRAIN'], 4),
        'MAE Train': redondear_total(resumen['MAE_TRAIN'], 4),
        'Coverage Train': redondear_total(resumen['COVERAGE_TRAIN'], 4),
        'WMAPE Test': redondear_total(resumen['WMAPE_TEST'], 4),
        'Bias Test': redondear_total(resumen['BIAS_TEST'], 4),
        'MAE Test': redondear_total(resumen['MAE_TEST'], 4),
        'Coverage Test': redondear_total(resumen['COVERAGE_TEST'], 4),
        'Win Rate Test': redondear_total(resumen['WIN_RATE_TEST'], 4),
        'Intensidad Promocional': metadata_ean['Intensidad Promocional'],
        'SEGMENTO_ABCD': metadata_ean['SEGMENTO_ABCD'],
        'ADI': redondear_total(metadata_ean['ADI'], decimales=4),
        'CV2': redondear_total(metadata_ean['CV2'], decimales=4),
        'DIAS_CON_VENTA': metadata_ean['DIAS_CON_VENTA'],
    }
    fila.update(
        construir_campos_canibalizacion(
            canibalizacion, uv_incremental_proy, venta_incremental_proy
        )
    )

    return fila


# =========================================================================
# 18. FERIADOS DE CHILE Y CALENDARIO
# =========================================================================
def calcular_domingo_pascua(anio: int) -> pd.Timestamp:
    """Domingo de Pascua (Gauss/Meeus)."""
    a = anio % 19
    b = anio // 100
    c = anio % 100
    d = b // 4
    e = b % 4
    f = (b + 8) // 25
    g = (b - f + 1) // 3
    h = (19 * a + b - d - g + 15) % 30
    i = c // 4
    k = c % 4
    lu = (32 + 2 * e + 2 * i - h - k) % 7
    m = (a + 11 * h + 22 * lu) // 451
    mes = (h + lu - 7 * m + 114) // 31
    dia = ((h + lu - 7 * m + 114) % 31) + 1

    return pd.Timestamp(year=anio, month=mes, day=dia)


def procesar_feriados_chile(historial_diario: pd.DataFrame) -> pd.DataFrame:
    """Reconstruye FLAG_FERIADO y FLAG_PRE_FERIADO para Chile."""
    resultado = historial_diario.copy()
    resultado['P_DATE'] = pd.to_datetime(resultado['P_DATE'])

    feriados_fijos = {
        (1, 1), (5, 1), (5, 21), (6, 20), (6, 29), (7, 16), (8, 15),
        (9, 18), (9, 19), (10, 12), (10, 31), (11, 1), (12, 8), (12, 25),
    }

    fechas_unicas = resultado['P_DATE'].drop_duplicates()
    anios = set(fechas_unicas.dt.year) | set((fechas_unicas + pd.Timedelta(days=1)).dt.year)

    feriados_moviles = set()

    for anio in anios:
        pascua = calcular_domingo_pascua(int(anio))
        feriados_moviles.update({pascua - pd.Timedelta(days=2), pascua - pd.Timedelta(days=1)})

    def es_fecha_feriado(fecha: pd.Timestamp) -> bool:
        return fecha.normalize() in feriados_moviles or (fecha.month, fecha.day) in feriados_fijos

    mapa_feriados = pd.DataFrame({
        'P_DATE': fechas_unicas,
        'FLAG_FERIADO': fechas_unicas.apply(es_fecha_feriado).astype(np.int8),
        'FLAG_PRE_FERIADO': (
            (fechas_unicas + pd.Timedelta(days=1)).apply(es_fecha_feriado).astype(np.int8)
        ),
    })

    resultado = resultado.drop(
        columns=['FLAG_FERIADO', 'FLAG_PRE_FERIADO', 'FERIADO_IRRENUNCIABLE'],
        errors='ignore',
    )

    return resultado.merge(mapa_feriados, on='P_DATE', how='left')


def construir_calendario_futuro(
    promociones: pd.DataFrame,
    columna_fecha_inicio: str = 'FECHA_INICIO_DE_PROMOCION',
    columna_fecha_fin: str = 'FECHA_FIN_DE_PROMOCION',
) -> pd.DataFrame:
    """Calendario diario con feriados para el rango de las promociones."""
    fechas_inicio = pd.to_datetime(promociones[columna_fecha_inicio], errors='coerce')
    fechas_fin = pd.to_datetime(promociones[columna_fecha_fin], errors='coerce')

    if (fechas_inicio.isna() | fechas_fin.isna() | fechas_fin.lt(fechas_inicio)).any():
        msg = 'Existen promociones con fechas nulas o inválidas.'
        raise ValueError(msg)

    calendario_futuro = procesar_feriados_chile(pd.DataFrame({
        'P_DATE': pd.date_range(
            start=fechas_inicio.min().normalize(),
            end=fechas_fin.max().normalize(),
            freq='D',
        )
    }))

    calendario_futuro[['FLAG_FERIADO', 'FLAG_PRE_FERIADO']] = (
        calendario_futuro[['FLAG_FERIADO', 'FLAG_PRE_FERIADO']].fillna(0).astype(np.int8)
    )

    return calendario_futuro


# =========================================================================
# 19. ORQUESTACIÓN: LOOP PROMOCIÓN-EAN
# =========================================================================
def loop_promociones(
    promociones_modelo: pd.DataFrame,
    historial_modelo: pd.DataFrame,
    caracterizacion_modelo: pd.DataFrame,
    columnas_familia: ColumnasFamiliaPromocional,
    configuracion: ConfiguracionModeloPromo,
    resumen_canibalizacion: dict[tuple[str, str], dict],
    fecha_corte_observada: pd.Timestamp,
    calendario_futuro: pd.DataFrame | None = None,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Entrena (<= T0 - 1), evalúa con lo observado y agrega canibalización."""  # noqa: W505
    historial_por_ean = dict(tuple(historial_modelo.groupby('EAN', sort=False)))

    filas_excel_final: list[dict[str, object]] = []
    detalles_diarios: list[pd.DataFrame] = []

    promociones_agrupadas = promociones_modelo.groupby('N_PROMOCION', sort=False)
    total_promociones = promociones_agrupadas.ngroups

    for indice_promocion, (numero_promocion, grupo_promocion) in enumerate(
        promociones_agrupadas, start=1
    ):
        total_ean = len(grupo_promocion)
        logging.info(
            'Promoción %s/%s (N_PROMOCION=%s) — %s EAN',
            indice_promocion, total_promociones, numero_promocion, total_ean,
        )

        for indice_ean, (_, promocion_ean) in enumerate(grupo_promocion.iterrows(), start=1):
            ean = str(promocion_ean['EAN'])
            canibalizacion = resumen_canibalizacion.get((str(numero_promocion), ean))
            logging.info('  EAN %s/%s — %s', indice_ean, total_ean, ean)

            caracterizacion_ean = (
                caracterizacion_modelo.loc[ean] if ean in caracterizacion_modelo.index else None
            )

            def registrar_no_proyectable(
                motivo: str,
                caracterizacion_actual: pd.Series | None = caracterizacion_ean,
                promocion_actual: pd.Series = promocion_ean,
                canibalizacion_actual: Mapping | None = canibalizacion,
            ) -> None:
                filas_excel_final.append(
                    construir_fila_no_proyectable(
                        promocion_ean=promocion_actual,
                        caracterizacion_ean=caracterizacion_actual,
                        motivo=motivo,
                        canibalizacion=canibalizacion_actual,
                    )
                )

            if caracterizacion_ean is None:
                registrar_no_proyectable('EAN sin registro en caracterización.')
                continue

            historial_ean = historial_por_ean.get(ean)

            if historial_ean is None or historial_ean.empty:
                registrar_no_proyectable('EAN sin historial disponible.')
                continue

            try:
                elegibilidad, regimen = evaluar_ean(ean, caracterizacion_modelo, configuracion)

                if not elegibilidad.elegible or regimen is None:
                    registrar_no_proyectable(obtener_motivo_elegibilidad(elegibilidad))
                    continue

                historial_entrenamiento = separar_historial_entrenamiento(
                    historial_ean=historial_ean,
                    fecha_inicio_promocion=promocion_ean['FECHA_INICIO_DE_PROMOCION'],
                    configuracion=configuracion,
                )

                dias_venta_entrenamiento = int(
                    historial_entrenamiento[configuracion.columna_target].gt(0).sum()
                )

                if dias_venta_entrenamiento < configuracion.minimo_dias_con_venta:
                    registrar_no_proyectable(
                        'Historial previo al inicio de la promoción insuficiente: '
                        f'{dias_venta_entrenamiento} días con venta'
                    )
                    continue

                datos_entrenamiento = preparar_datos_entrenamiento(
                    historial_ean=historial_entrenamiento,
                    regimen=regimen,
                    columnas_familia=columnas_familia,
                    configuracion=configuracion,
                )
                resultado_entrenamiento = entrenar_modelo_producto(
                    datos_entrenamiento, regimen, configuracion
                )
                resultado_escenarios = proyectar_promocion_ean(
                    promocion_ean=promocion_ean,
                    historial_entrenamiento=historial_entrenamiento,
                    historial_observado=historial_ean,
                    resultado_entrenamiento=resultado_entrenamiento,
                    regimen=regimen,
                    columnas_familia=columnas_familia,
                    configuracion=configuracion,
                    calendario_futuro=calendario_futuro,
                    fecha_corte_observada=fecha_corte_observada,
                )

                filas_excel_final.append(
                    construir_fila_excel_final(
                        resultado_escenarios=resultado_escenarios,
                        promocion_ean=promocion_ean,
                        caracterizacion_ean=caracterizacion_ean,
                        canibalizacion=canibalizacion,
                    )
                )

                if configuracion.guardar_detalle_diario:
                    detalles_diarios.append(resultado_escenarios.detalle_diario)

            except Exception as error:  # noqa: BLE001
                logging.warning(
                    '  Error en Promoción %s / EAN %s: %s: %s',
                    numero_promocion, ean, type(error).__name__, error,
                )
                registrar_no_proyectable(
                    f'Error durante entrenamiento o proyección: {type(error).__name__}: {error}'
                )

    excel_final = pd.DataFrame(filas_excel_final, columns=COLUMNAS_EXCEL_FINAL)
    detalle_diario_completo = (
        pd.concat(detalles_diarios, ignore_index=True)
        if detalles_diarios
        else pd.DataFrame(columns=COLUMNAS_DETALLE_DIARIO)
    )

    logging.info(
        'Proceso finalizado: %s filas en Excel final, %s filas en detalle diario.',
        len(excel_final), len(detalle_diario_completo),
    )

    return excel_final, detalle_diario_completo


# =========================================================================
# 20. EJECUCIÓN FINAL
# =========================================================================
def ejecutar_pipeline_promocional(
    tabla_historial: pd.DataFrame,
    tabla_caracterizacion: pd.DataFrame,
    tabla_promos_proy: pd.DataFrame,
    tabla_elasticidades: pd.DataFrame | None = None,
    tabla_historial_sustitutos: pd.DataFrame | None = None,
    configuracion: ConfiguracionModeloPromo | None = None,
    ruta_salida_excel: str | None = None,
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """Pipeline completo: forecast ex post + canibalización.

    Returns
    -------
    (excel_final, detalle_diario_completo, detalle_canibalizacion)
    """
    configurar_logging()
    configuracion = configuracion or ConfiguracionModeloPromo()

    logging.info('Preparando fuentes del modelo...')
    fuentes = preparar_fuentes_modelo(
        historial=tabla_historial,
        caracterizacion=tabla_caracterizacion,
        promociones_futuras=tabla_promos_proy,
        configuracion=configuracion,
        tabla_elasticidades=tabla_elasticidades,
        historial_sustitutos=tabla_historial_sustitutos,
    )

    fechas_disponibles = [fuentes.historial['P_DATE'].max()]

    if not fuentes.historial_sustitutos.empty:
        fechas_disponibles.append(fuentes.historial_sustitutos['P_DATE'].max())

    fecha_corte_observada = max(fecha for fecha in fechas_disponibles if pd.notna(fecha))
    logging.info('Fecha de corte de datos observados: %s', fecha_corte_observada.date())

    logging.info('Calculando canibalización...')
    resumen_canibalizacion, detalle_canibalizacion = calcular_canibalizacion(
        promociones=fuentes.promociones_futuras,
        elasticidades=fuentes.elasticidades,
        historial_sustitutos=fuentes.historial_sustitutos,
        configuracion=configuracion,
        fecha_corte_observada=fecha_corte_observada,
    )
    mapa_canibalizacion = {
        (str(registro['N_PROMOCION']), str(registro['EAN_FOCAL'])): registro
        for registro in resumen_canibalizacion.to_dict('records')
    }

    logging.info('Construyendo calendario (feriados Chile)...')
    calendario_futuro = construir_calendario_futuro(fuentes.promociones_futuras)

    logging.info('Iniciando loop de promociones...')
    excel_final, detalle_diario_completo = loop_promociones(
        promociones_modelo=fuentes.promociones_futuras,
        historial_modelo=fuentes.historial,
        caracterizacion_modelo=fuentes.caracterizacion,
        columnas_familia=fuentes.columnas_familia,
        configuracion=configuracion,
        resumen_canibalizacion=mapa_canibalizacion,
        fecha_corte_observada=fecha_corte_observada,
        calendario_futuro=calendario_futuro,
    )

    if ruta_salida_excel is not None:
        logging.info('Guardando Excel final en %s', ruta_salida_excel)
        excel_final.to_excel(ruta_salida_excel, index=False)

    return excel_final, detalle_diario_completo, detalle_canibalizacion


def generar_excel_buffer(hojas: dict[str, pd.DataFrame]) -> io.BytesIO:
    """Excel en memoria con una hoja por tabla."""
    buffer = io.BytesIO()

    with pd.ExcelWriter(buffer, engine='xlsxwriter') as escritor:
        for nombre_hoja, tabla in hojas.items():
            tabla.to_excel(escritor, index=False, sheet_name=nombre_hoja)
            escritor.sheets[nombre_hoja].freeze_panes(1, 0)

    buffer.seek(0)

    return buffer


def subir_archivo_sharepoint(
    contenido: io.BytesIO,
    nombre_archivo: str,
    outputs_dir: str,
    sp_cred: dict,
) -> None:
    """Sube un buffer a SharePoint."""
    contenido.seek(0)
    ruta_remota = posixpath.join(outputs_dir, nombre_archivo)
    logging.info('Subiendo archivo a SharePoint: %s', ruta_remota)

    sp.SharePointFile(**sp_cred, server_relative_path=ruta_remota).upload(content=contenido)

    logging.info('Archivo subido correctamente a SharePoint')


# =========================================================================
# 21. MODO INTERACTIVO / EXPLORACIÓN POR PRODUCTO (Jupyter)
# =========================================================================
@dataclass(frozen=True)
class EntradaExploracion:
    """Parámetros manuales para proyectar un producto."""

    ean: str
    precio_modal: float
    precio_promocional: float
    porcentaje_descuento: float
    fecha_inicio: str | pd.Timestamp
    fecha_fin: str | pd.Timestamp
    mecanica: str | None = None
    n_promocion: str = 'EXPLORACION'
    nombre_promocion: str = 'Proyeccion exploratoria'


@dataclass
class ResultadoExploracionProducto:
    """Salida del modo interactivo, lista para graficar."""

    ean: str
    elegible: bool
    comentario: str
    metadata_producto: dict[str, object]
    inputs_modelo: dict[str, object]
    validacion_mecanica: dict[str, object]
    regimen: RegimenPromocional | None
    columnas_modelo: list[str]
    metricas_train: MetricasModelo | None
    metricas_test: MetricasModelo | None
    datos_test: pd.DataFrame
    ajuste_historico: pd.DataFrame
    proyeccion_diaria: pd.DataFrame
    resumen_proyeccion: dict[str, object]
    modelo_validacion: HistGradientBoostingRegressor | None
    modelo_productivo: HistGradientBoostingRegressor | None


def validar_mecanica_interactiva(
    mecanica: str | None,
    caracterizacion_ean: pd.Series,
    regimen: RegimenPromocional,
    configuracion: ConfiguracionModeloPromo,
) -> dict[str, object]:
    """Diagnóstico de la mecánica entregada (no interrumpe)."""
    mecanica_normalizada = (
        str(mecanica).strip().upper().replace(' ', '_') if mecanica not in (None, '') else None
    )
    mecanicas_validas = _parsear_mecanicas_validas(
        obtener_valor(caracterizacion_ean, 'MECANICAS_VALIDAS', valor_default=None)
    )
    incluir_mecanica_ean = es_valor_afirmativo(
        obtener_valor(caracterizacion_ean, 'INCLUIR_MECANICA', valor_default=None),
        configuracion,
    )

    es_valida = False
    mecanica_aplicada = None

    if mecanica_normalizada is None:
        aviso = 'No se entregó mecánica; se proyecta sin mecánica.'
    elif not configuracion.incluir_mecanica:
        aviso = 'Mecánica ignorada: deshabilitada globalmente.'
    elif not incluir_mecanica_ean:
        aviso = 'Mecánica ignorada: EAN no habilitado para mecánica.'
    elif not regimen.usar_mecanica:
        aviso = f'Mecánica ignorada: el régimen {regimen.nombre} no la utiliza.'
    elif mecanica_normalizada not in mecanicas_validas:
        aviso = (
            f'Mecánica no reconocida: {mecanica_normalizada!r}. '
            f'Válidas: {mecanicas_validas}. Se proyecta como MECANICA_OTRA.'
        )
    else:
        es_valida = True
        mecanica_aplicada = mecanica_normalizada
        aviso = 'Mecánica válida y aplicada.'

    return {
        'mecanica_entregada': mecanica_normalizada,
        'mecanicas_validas': mecanicas_validas,
        'mecanica_aplicada': mecanica_aplicada,
        'es_valida': es_valida,
        'aviso': aviso,
    }


def _construir_promos_una_fila(entrada: EntradaExploracion) -> pd.DataFrame:
    """Promoción de una fila compatible con el pipeline."""
    return pd.DataFrame([{
        'N_PROMOCION': entrada.n_promocion,
        'NOMBRE_PROMOCION': entrada.nombre_promocion,
        'EAN': entrada.ean,
        'FECHA_INICIO_DE_PROMOCION': entrada.fecha_inicio,
        'FECHA_FIN_DE_PROMOCION': entrada.fecha_fin,
        'PRECIO_PROMOCIONAL': entrada.precio_promocional,
        'PRECIO_MODAL': entrada.precio_modal,
        'PORCENTAJE_DESCUENTO': entrada.porcentaje_descuento,
        'DESCRIPCION_EVENTO_PROMOCIONAL': entrada.mecanica or 'SIN_MECANICA',
    }])


def _construir_datos_test(
    datos_entrenamiento: DatosEntrenamiento,
    resultado_entrenamiento: ResultadoEntrenamiento,
) -> pd.DataFrame:
    """Test del modelo 80/20 listo para graficar."""
    return pd.DataFrame({
        'P_DATE': datos_entrenamiento.fechas_test.to_numpy(),
        'Y_REAL': datos_entrenamiento.y_test.to_numpy(dtype='float64'),
        'Y_PRED_8020': resultado_entrenamiento.predicciones_test,
        'BENCHMARK': resultado_entrenamiento.benchmark_test,
    })


def _extraer_contexto_promocional(
    historial_crudo: pd.DataFrame | None,
    ean: str,
    columnas_familia: ColumnasFamiliaPromocional,
) -> pd.DataFrame:
    """Precio, precio promocional y flag promo desde el historial crudo."""
    vacio = pd.DataFrame({
        'P_DATE': pd.Series(dtype='datetime64[ns]'),
        'PRECIO': pd.Series(dtype='float64'),
        'PRECIO_PROMO': pd.Series(dtype='float64'),
        'FLAG_PROMO': pd.Series(dtype='int8'),
    })

    if historial_crudo is None or historial_crudo.empty:
        return vacio

    contexto = historial_crudo.loc[normalizar_ean(historial_crudo['EAN']).eq(ean)]

    if contexto.empty:
        return vacio

    columna_precio_promo = columnas_familia.precio_promocional_minimo
    columna_flag = columnas_familia.flag_promocion

    salida = pd.DataFrame({
        'P_DATE': pd.to_datetime(contexto['P_DATE']).dt.normalize().to_numpy(),
        'PRECIO': pd.to_numeric(contexto['PRECIO_PROMEDIO'], errors='coerce').to_numpy(),
        'PRECIO_PROMO': (
            pd.to_numeric(contexto[columna_precio_promo], errors='coerce').to_numpy()
            if columna_precio_promo in contexto.columns else np.nan
        ),
        'FLAG_PROMO': (
            pd.to_numeric(contexto[columna_flag], errors='coerce').fillna(0).astype('int8').to_numpys()  # noqa: E501
            if columna_flag in contexto.columns else 0
        ),
    })

    return salida.drop_duplicates('P_DATE', keep='last')


def _construir_ajuste_historico(
    datos_entrenamiento: DatosEntrenamiento,
    resultado_entrenamiento: ResultadoEntrenamiento,
    columnas_familia: ColumnasFamiliaPromocional,
    historial_crudo: pd.DataFrame | None = None,
    ean: str | None = None,
) -> pd.DataFrame:
    """Historial con reales, predicciones, precios y promociones."""
    columnas_modelo = resultado_entrenamiento.columnas_modelo
    matriz = pd.concat(
        [datos_entrenamiento.X_train, datos_entrenamiento.X_test], ignore_index=True
    )[columnas_modelo]
    mascara_test = np.r_[
        np.zeros(len(datos_entrenamiento.X_train), dtype=bool),
        np.ones(len(datos_entrenamiento.X_test), dtype=bool),
    ]

    ajuste = pd.DataFrame({
        'P_DATE': pd.concat(
            [datos_entrenamiento.fechas_train, datos_entrenamiento.fechas_test],
            ignore_index=True,
        ).dt.normalize(),
        'Y_REAL': pd.concat(
            [datos_entrenamiento.y_train, datos_entrenamiento.y_test], ignore_index=True
        ).astype('float64'),
        'PARTICION': np.where(mascara_test, 'test', 'train'),
    })
    ajuste['Y_PRED_COMPLETO'] = np.clip(
        resultado_entrenamiento.modelo_productivo.predict(matriz), 0.0, None
    )
    ajuste['Y_PRED_TEST'] = np.nan

    if mascara_test.any():
        ajuste.loc[mascara_test, 'Y_PRED_TEST'] = np.clip(
            resultado_entrenamiento.modelo_validacion.predict(matriz.loc[mascara_test]),
            0.0, None,
        )

    ajuste = ajuste.merge(
        _extraer_contexto_promocional(historial_crudo, ean or '', columnas_familia),
        on='P_DATE',
        how='left',
    )
    ajuste['FLAG_PROMO'] = pd.to_numeric(ajuste['FLAG_PROMO'], errors='coerce').fillna(0).astype('int8')  # noqa: E501

    return ajuste.sort_values('P_DATE', kind='stable').reset_index(drop=True)


def _resultado_no_proyectable(
    ean: str,
    caracterizacion_ean: pd.Series | None,
    comentario: str,
    inputs_modelo: dict[str, object] | None = None,
) -> ResultadoExploracionProducto:
    """Salida de exploración cuando el EAN no es proyectable."""
    return ResultadoExploracionProducto(
        ean=ean,
        elegible=False,
        comentario=comentario,
        metadata_producto=obtener_metadata_ean(caracterizacion_ean),
        inputs_modelo=inputs_modelo or {},
        validacion_mecanica={},
        regimen=None,
        columnas_modelo=[],
        metricas_train=None,
        metricas_test=None,
        datos_test=pd.DataFrame(columns=['P_DATE', 'Y_REAL', 'Y_PRED_8020', 'BENCHMARK']),
        ajuste_historico=pd.DataFrame(columns=['P_DATE', 'Y_REAL', 'Y_PRED_COMPLETO', 'PARTICION']),  # noqa: E501
        proyeccion_diaria=pd.DataFrame(),
        resumen_proyeccion={},
        modelo_validacion=None,
        modelo_productivo=None,
    )


def explorar_producto(
    entrada: EntradaExploracion,
    historial_crudo: pd.DataFrame,
    caracterizacion_cruda: pd.DataFrame,
    configuracion: ConfiguracionModeloPromo | None = None,
    calendario_futuro: pd.DataFrame | None = None,
) -> ResultadoExploracionProducto:
    """Proyecta un producto en modo interactivo (entrena <= T0 - 1)."""
    configuracion = configuracion or ConfiguracionModeloPromo()
    ean = normalizar_ean(pd.Series([entrada.ean])).iloc[0]

    fuentes = preparar_fuentes_modelo(
        historial=historial_crudo,
        caracterizacion=caracterizacion_cruda,
        promociones_futuras=_construir_promos_una_fila(entrada),
        configuracion=configuracion,
    )

    if calendario_futuro is None:
        calendario_futuro = construir_calendario_futuro(fuentes.promociones_futuras)

    promocion_ean = fuentes.promociones_futuras.iloc[0]

    inputs_modelo = {
        'EAN': ean,
        'PRECIO_MODAL': float(promocion_ean['PRECIO_MODAL']),
        'PRECIO_PROMOCIONAL': float(promocion_ean['PRECIO_PROMOCIONAL']),
        'PORCENTAJE_DESCUENTO': float(promocion_ean['PORCENTAJE_DESCUENTO']),
        'MECANICA': entrada.mecanica,
        'FECHA_INICIO': promocion_ean['FECHA_INICIO_DE_PROMOCION'],
        'FECHA_FIN': promocion_ean['FECHA_FIN_DE_PROMOCION'],
        'DURACION_DIAS': int(promocion_ean['DURACION_PROMOCION_DIAS']),
        'FAMILIA_PROMOCIONAL': configuracion.familia_promocional,
    }

    if ean not in fuentes.caracterizacion.index:
        return _resultado_no_proyectable(
            ean, None, 'EAN sin registro en caracterización.', inputs_modelo
        )

    caracterizacion_ean = fuentes.caracterizacion.loc[ean]
    historial_ean = fuentes.historial.loc[fuentes.historial['EAN'].eq(ean)]

    if historial_ean.empty:
        return _resultado_no_proyectable(
            ean, caracterizacion_ean, 'EAN sin historial disponible.', inputs_modelo
        )

    elegibilidad, regimen = evaluar_ean(ean, fuentes.caracterizacion, configuracion)

    if not elegibilidad.elegible or regimen is None:
        return _resultado_no_proyectable(
            ean, caracterizacion_ean, obtener_motivo_elegibilidad(elegibilidad), inputs_modelo
        )

    historial_entrenamiento = separar_historial_entrenamiento(
        historial_ean, promocion_ean['FECHA_INICIO_DE_PROMOCION'], configuracion
    )

    if len(historial_entrenamiento) < 2:
        return _resultado_no_proyectable(
            ean, caracterizacion_ean,
            'Sin historial suficiente antes del inicio de la promoción.', inputs_modelo,
        )

    datos_entrenamiento = preparar_datos_entrenamiento(
        historial_entrenamiento, regimen, fuentes.columnas_familia, configuracion
    )
    resultado_entrenamiento = entrenar_modelo_producto(
        datos_entrenamiento, regimen, configuracion
    )
    resultado_escenarios = proyectar_promocion_ean(
        promocion_ean=promocion_ean,
        historial_entrenamiento=historial_entrenamiento,
        historial_observado=historial_ean,
        resultado_entrenamiento=resultado_entrenamiento,
        regimen=regimen,
        columnas_familia=fuentes.columnas_familia,
        configuracion=configuracion,
        calendario_futuro=calendario_futuro,
        fecha_corte_observada=fuentes.historial['P_DATE'].max(),
    )

    return ResultadoExploracionProducto(
        ean=ean,
        elegible=True,
        comentario='-',
        metadata_producto=obtener_metadata_ean(caracterizacion_ean),
        inputs_modelo=inputs_modelo,
        validacion_mecanica=validar_mecanica_interactiva(
            entrada.mecanica, caracterizacion_ean, regimen, configuracion
        ),
        regimen=regimen,
        columnas_modelo=resultado_entrenamiento.columnas_modelo,
        metricas_train=resultado_entrenamiento.metricas_train,
        metricas_test=resultado_entrenamiento.metricas_test,
        datos_test=_construir_datos_test(datos_entrenamiento, resultado_entrenamiento),
        ajuste_historico=_construir_ajuste_historico(
            datos_entrenamiento=datos_entrenamiento,
            resultado_entrenamiento=resultado_entrenamiento,
            columnas_familia=fuentes.columnas_familia,
            historial_crudo=historial_crudo,
            ean=ean,
        ),
        proyeccion_diaria=resultado_escenarios.detalle_diario,
        resumen_proyeccion=resultado_escenarios.resumen,
        modelo_validacion=resultado_entrenamiento.modelo_validacion,
        modelo_productivo=resultado_entrenamiento.modelo_productivo,
    )


# -------------------------------------------------------------------------
#  Config
# -------------------------------------------------------------------------
config.dictConfig(LOGGING_CONFIG)

parser = argparse.ArgumentParser()
parser.add_argument('--project_id', type=str, help='GCP project')
parser.add_argument('--execution_date', type=str, help='DAG execution date')
parser.add_argument('--store_banner', type=str, help='Store banner')

PATRON_INPUT = re.compile(
    r'^\d{4}_\d{2}_\d{2}_v\d+_input_proyeccion\.xlsx$', re.IGNORECASE
)
PATRON_OUTPUT = re.compile(
    r'^\d{4}_\d{2}_\d{2}_v\d+_resultado_proyeccion\.xlsx$', re.IGNORECASE
)


QUERY_HISTORIAL = QueryDict({
    'query_historial':
    """
    WITH productos_objetivo AS (
        SELECT DISTINCT(EAN) FROM `tabla_promo`
        WHERE organizacion_ventas = '1000'
            AND canal_distribucion = '10'
            AND registro_valido = 'X'
            AND n_promocion IN (${promos})
    )
    SELECT * EXCEPT(STORE_BANNER, SALES_UOM, SALES_UNIT, MULTIPLICADOR_X05,
        APO, PROPORCION_CATEGORIA, VARIACION_PORCENTUAL_SUBCATEGORIA,
        EAN_SUSTITUTO_1, EAN_SUSTITUTO_2, EAN_SUSTITUTO_3, EAN_SUSTITUTO_4,
        EAN_SUSTITUTO_5, EAN_SUSTITUTO_6, EAN_SUSTITUTO_7, EAN_SUSTITUTO_8,
        EAN_SUSTITUTO_9, EAN_SUSTITUTO_10, EAN_SUSTITUTO_11, EAN_SUSTITUTO_12,
        EAN_SUSTITUTO_13, EAN_SUSTITUTO_14, EAN_SUSTITUTO_15, EAN_SUSTITUTO_16,
        EAN_SUSTITUTO_17, EAN_SUSTITUTO_18, EAN_SUSTITUTO_19, EAN_SUSTITUTO_20)
    FROM `hist_data`
    WHERE EAN IN (SELECT EAN FROM productos_objetivo)
    """})


QUERY_CARACTERIZACION = QueryDict({
    'query_caracterizacion':
    """
    SELECT * FROM `caract_data`
    WHERE EAN IN (${lista_eans})
    """})


QUERY_INFO_PROMOS = QueryDict({
    'query_info_promos':
    """
    SELECT
        N_PROMOCION, NOMBRE_PROMOCION, DESCRIPCION_EVENTO_PROMOCIONAL,
        EAN, MATERIAL, FECHA_INICIO_DE_PROMOCION, FECHA_FIN_DE_PROMOCION,
        PORCENTAJE_DESCUENTO, PRECIO_MODAL, PRECIO_PROMOCIONAL
    FROM `tabla_promos`
    WHERE ORGANIZACION_VENTAS = '1000'
        AND CANAL_DISTRIBUCION = '10'
        AND REGISTRO_VALIDO = 'X'
        AND N_PROMOCION IN (${promos})
    """})


# Reemplazar `tabla_elasticidades_cruzadas` por la tabla real.
QUERY_ELASTICIDADES = QueryDict({
    'query_elasticidades':
    """
    SELECT *
    FROM `tabla_elasticidades_cruzadas`
    WHERE EAN_FOCAL IN (${lista_eans})
    """})


QUERY_HISTORIAL_SUSTITUTOS = QueryDict({
    'query_historial_sustitutos':
    """
    SELECT EAN, P_DATE, CANTIDAD_TOTAL, PRECIO_PROMEDIO, ${columna_flag}
    FROM `hist_data`
    WHERE EAN IN (${lista_eans})
        AND P_DATE BETWEEN '${fecha_min}' AND '${fecha_max}'
    """})


def main() -> None:
    args = vars(parser.parse_args())
    proyecto: str = args['project_id']
    execution_date: str = args['execution_date']

    file_site = 'nada'
    file_site += 'Pricing/Forecast Promociones'
    outputs_dir = posixpath.join(file_site, 'Outputs')

    sp_cred = secretmanager.getSecret('bdaa_sharepoint_credentials', project=proyecto)
    gbq_client = Client()

    configuracion_pipeline = ConfiguracionModeloPromo(familia_promocional='B')
    columna_flag_promo = f'FLAG_PROMO_{configuracion_pipeline.familia_promocional}'

    # 1.1 PROMOCIONES
    _, promociones, _, nombre_output = preparar_input_promociones(
        credenciales_sharepoint=sp_cred,
        ruta_base_sharepoint=file_site,
        patron_input=PATRON_INPUT,
        patron_output=PATRON_OUTPUT,
    )
    logging.info('Archivo: %s | Promociones: %s', nombre_output, len(promociones))

    # 1.2 HISTORIAL FOCALES
    tabla_historial = readBigQuery(
        query=QUERY_HISTORIAL['query_historial'].substitute(promos=','.join(promociones)),
        user='pricing', gbq_client=gbq_client,
    )
    fecha_min_historial = tabla_historial['P_DATE'].min()
    fecha_max_historial = tabla_historial['P_DATE'].max()
    logging.info(
        'Historial: %s | EAN: %s | %s a %s',
        tabla_historial.shape, tabla_historial['EAN'].nunique(),
        fecha_min_historial, fecha_max_historial,
    )

    # 1.3 CARACTERIZACIÓN
    lista_eans = normalizar_ean(pd.Series(tabla_historial['EAN'].unique())).tolist()
    tabla_caracterizacion = readBigQuery(
        query=QUERY_CARACTERIZACION['query_caracterizacion'].substitute(
            lista_eans=formatear_lista_sql(lista_eans)
        ),
        user='pricing', gbq_client=gbq_client,
    )
    logging.info('Caracterización: %s', tabla_caracterizacion.shape)

    # 1.4 PROMOS A EVALUAR
    tabla_promos_proy = readBigQuery(
        query=QUERY_INFO_PROMOS['query_info_promos'].substitute(promos=','.join(promociones)),
        user='pricing', gbq_client=gbq_client,
    )
    logging.info('Promos: %s', tabla_promos_proy.shape)

    # 1.5 ELASTICIDADES CRUZADAS
    ean_focales = normalizar_ean(tabla_promos_proy['EAN']).dropna().unique().tolist()
    tabla_elasticidades = readBigQuery(
        query=QUERY_ELASTICIDADES['query_elasticidades'].substitute(
            lista_eans=formatear_lista_sql(ean_focales)
        ),
        user='pricing', gbq_client=gbq_client,
    )
    logging.info('Elasticidades: %s', tabla_elasticidades.shape)

    # 1.6 HISTORIAL SUSTITUTOS (ventana de las promociones)
    tabla_historial_sustitutos = None
    columna_relacionado = next(
        (
            columna for columna in tabla_elasticidades.columns
            if str(columna).strip().upper() == 'EAN_RELACIONADO'
        ),
        None,
    )

    if columna_relacionado is not None and not tabla_elasticidades.empty:
        ean_sustitutos = (
            normalizar_ean(tabla_elasticidades[columna_relacionado]).dropna().unique().tolist()
        )
        fecha_min_promos = pd.to_datetime(tabla_promos_proy['FECHA_INICIO_DE_PROMOCION']).min()
        fecha_max_promos = pd.to_datetime(tabla_promos_proy['FECHA_FIN_DE_PROMOCION']).max()

        tabla_historial_sustitutos = readBigQuery(
            query=QUERY_HISTORIAL_SUSTITUTOS['query_historial_sustitutos'].substitute(
                columna_flag=columna_flag_promo,
                lista_eans=formatear_lista_sql(ean_sustitutos),
                fecha_min=fecha_min_promos.strftime('%Y-%m-%d'),
                fecha_max=fecha_max_promos.strftime('%Y-%m-%d'),
            ),
            user='pricing', gbq_client=gbq_client,
        )
        logging.info('Historial sustitutos: %s', tabla_historial_sustitutos.shape)

    # 2. PIPELINE
    excel_final, _detalle_diario, detalle_canibalizacion = ejecutar_pipeline_promocional(
        tabla_historial=tabla_historial,
        tabla_caracterizacion=tabla_caracterizacion,
        tabla_promos_proy=tabla_promos_proy,
        tabla_elasticidades=tabla_elasticidades,
        tabla_historial_sustitutos=tabla_historial_sustitutos,
        configuracion=configuracion_pipeline,
    )

    excel_final = excel_final.drop(columns=[
        'Estado_Historial', 'Estado_Modelo', 'Estado_Elasticidad',
        'Estado_fecha_proy', 'Estado_proyección',
    ])
    excel_final['Inicio Proy'] = pd.to_datetime(excel_final['Inicio Proy'], errors='coerce').dt.date  # noqa: E501
    excel_final['Fin Proy'] = pd.to_datetime(excel_final['Fin Proy'], errors='coerce').dt.date

    # 3. SHAREPOINT (hoja principal + detalle de canibalización)
    subir_archivo_sharepoint(
        contenido=generar_excel_buffer({
            'Resultados_proyeccion': excel_final,
            'Detalle_canibalizacion': detalle_canibalizacion,
        }),
        nombre_archivo=nombre_output,
        outputs_dir=outputs_dir,
        sp_cred=sp_cred,
    )

    # 4. GCP
    excel_final.insert(loc=0, column='execution_date', value=execution_date)

    columnas_metricas = [
        'R²', 'Elasticidad', 'Baseline_UV', 'UV Incremental Real',
        'UV Incremental Proy', 'UV Real', 'UV Proy', 'Baseline Venta',
        'Venta Incremental Real', 'Venta Incremental Proy', 'Venta Real',
        'Venta Proy', 'N Sustitutos Evaluados', 'N Sustitutos Canibalizados',
        'UV Canibalizada', 'Venta Canibalizada', 'UV Incremental Neta',
        'Venta Incremental Neta', 'WMAPE Train', 'Bias Train', 'MAE Train',
        'Coverage Train', 'WMAPE Test', 'Bias Test', 'MAE Test',
        'Coverage Test', 'Win Rate Test', 'Intensidad Promocional', 'ADI',
        'CV2', 'DIAS_CON_VENTA',
    ]
    excel_final[columnas_metricas] = (
        excel_final[columnas_metricas]
        .replace('-', np.nan)
        .apply(pd.to_numeric, errors='coerce')
    )
    excel_final['Material'] = pd.to_numeric(
        excel_final['Material'].replace('-', pd.NA), errors='coerce'
    ).astype('Int64')

    esquema = 'FORECAST_ML'
    tabla = 'PROYECCION_VENTAS_MODELO_CASSI'

    deleteFromTable(
        table_ref=f'{proyecto}.{esquema}.{tabla}',
        where_clause=f'execution_date = {execution_date!r}',
        gbq_client=gbq_client,
    )

    uploadFrame(
        excel_final,
        table_ddl_json_path=os.path.join('gbq_objects', 'C1_ingest_product_forecast.json'),
        project=proyecto,
        gbq_client=gbq_client,
        if_exists='append',
    )

    logging.info('Se sube la tabla a GCP')


if __name__ == '__main__':
    main()
