##### ----- IMPORTS ----- #####
from __future__ import annotations

import os
import sys
import logging
from string import Template
from logging import config
from pathlib import Path
from datetime import datetime
from textwrap import dedent

# Pip
import numpy as np
import pandas as pd
import pendulum
from boto3 import Session
from google.cloud.bigquery import Client
from dateutil.relativedelta import relativedelta


directorio_actual = os.path.abspath(os.curdir)

while directorio_actual != os.path.sep:
    try:
        sys.path.append(directorio_actual)
        from credentials import credentials  # noqa: F401
        break  # Si la importación es exitosa, sale del bucle
    except ModuleNotFoundError:
        sys.path.pop()  # Remueve el directorio que no contenía el módulo
        directorio_actual = os.path.dirname(directorio_actual)  # Retrocede

import posixpath

import statsmodels.api as sm

import common.gcp_extended.secretsmanager as secretmanager
import common.office365_extended.sharepoint as sp
from common.constants import LOGGING_CONFIG
from common.databases.queries import QueryDict  # noqa: E402
from common.gcp_extended.bigquery import (  # noqa: E402
    uploadFrame,  # noqa: F401
    readBigQuery,
    deleteFromTable,  # noqa: F401
    createTableAsSelect,  # noqa: F401
)


sys.path.append(os.path.abspath('..'))
from config_forecast import tabla, gbq_client, path_table, store_banner  # noqa: E402
from scripts.forecast_promotion_auditoria import generarDataFrame  # noqa: E402


#######################################################
##########---------- 0. Auxiliares ----------##########
#######################################################


#########           0.1 Queries

# Query para obtener la data procesada de forecast para todos los
# productos disponibles.

QUERY_FORECAST= QueryDict({
    'query_data_procesada':
    """

    SELECT * EXCEPT(
        PRIMER_DIA_MES,
        ULTIMO_DIA_MES,
        P_WEEK,
        P_MONTH,
        VARIACION_TOP1_SUSTITUTO,
        VARIACION_TOP3_SUSTITUTOS
        )

    FROM `${path_table}`
    Where store_banner = 'Unimarc'
    """})

