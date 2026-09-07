##### ----- IMPORTS ----- #####
from __future__ import annotations

import os
import sys
from string import Template
from pathlib import Path
from datetime import datetime
from textwrap import dedent

import logging
from logging import config

# Pip
import numpy as np
import pandas as pd
import pendulum
import matplotlib.pyplot as plt
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

from common.constants import LOGGING_CONFIG
from common.databases.queries import QueryDict  # noqa: E402
from common.gcp_extended.bigquery import (  # noqa: E402
    uploadFrame,  # noqa: F401
    readBigQuery,
    deleteFromTable,  # noqa: F401
    createTableAsSelect,  # noqa: F401
)

import common.gcp_extended.secretsmanager as secretmanager
import common.office365_extended.sharepoint as sp
import posixpath
import re
import statsmodels.api as sm

sys.path.append(os.path.abspath('..'))
from scripts.forecast_promotion_auditoria import generarDataFrame

from config_forecast import (
        tabla, 
        path_table, 
        store_banner, 
        gbq_client
        )