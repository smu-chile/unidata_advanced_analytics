# Default
import json  # noqa: I001
import platform
import importlib
from datetime import timedelta
# Pip
import pendulum
from airflow.models import DAG
from airflow.configuration import conf
if platform.system() == 'Windows':
    from common.operators.dataproc_create_batch import (
        ExtendedDataprocCreateBatchOperator,
    )
elif platform.system() == 'Linux':
    ExtendedDataprocCreateBatchOperator = (
        importlib.import_module(
            'BRANCH_PLACEHOLDER.'
            'smu-chile.unidata_advanced_analytics.'
            'src.common.operators.dataproc_create_batch'
        )
    ).ExtendedDataprocCreateBatchOperator
else:
    msg = 'Only Linux and Windows are supported.'
    raise NotImplementedError(msg)
# -------------------------------------------------------------------------
# Configuración ambiente
# -------------------------------------------------------------------------
with open(
    f'{conf.get("core", "dags_folder")}/'
    'BRANCH_PLACEHOLDER/'
    'smu-chile/unidata_advanced_analytics/'
    'src/common/constants/dag_env_config.json'
) as f:
    dag_env_config = json.load(f)['BRANCH_PLACEHOLDER']

# Mismo proyecto/carpeta que elasticidad_general.py (ELASTICITY_PR) --
# los archivos de este DAG viven en pricing_effects/, no en una
# carpeta propia. Esto tambien significa que reutiliza la MISMA imagen
# Docker que pricing_effects (ya tiene pygam/joblib instalados, sin
# necesitar su propio Dockerfile/requirements.txt).
PROJECT_NAME = 'pricing_effects'

STORE_BANNER_LIST = [
    'Unimarc',
    'Alvi',
]

dag_args = {
    'dag_id': 'ecommerce_regression_data',
    'schedule_interval': None,
    'dagrun_timeout': None,
    'catchup': False,
    'max_active_runs': 1,
    'concurrency': 4,
    'tags': [
        PROJECT_NAME,
    ],
    'default_args': {
        'project_id': dag_env_config['project_id'],
        'region': dag_env_config['region'],
        'owner': 'BIGDATA_ANALYTICS',
        'email': [],
        'start_date': pendulum.datetime(
            2026,
            1,
            1,
            tz=pendulum.timezone(
                'America/Santiago'
            )
        ),
        'depends_on_past': False,
        'catchup': False,
        'email_on_failure': True,
        'email_on_retry': False,
        'retries': 0,
        'retry_delay': timedelta(
            minutes=5
        )
    }
}
# -------------------------------------------------------------------------
# DAG -- SOLO regenera la tabla de regresion de ecommerce. No dispara
# sensibilidad, balance_matrix, ni elasticidad -- cada tarea corre
# aislada, sin ningun encadenamiento >> hacia otra cosa.
# -------------------------------------------------------------------------
with DAG(**dag_args) as dag:
    EXECUTION_DATE = (
        "{{ dag_run.conf.get("
        "'execution_date', "
        "dag.timezone.convert("
        "data_interval_end"
        ").strftime('%Y-%m-%d')) }}"
    )

    regression_tasks = []

    for store_banner in STORE_BANNER_LIST:
        banner_suffix = store_banner.replace(' ', '_').lower()

        # ---------- Task: regeneracion de tabla de regresion ----------
        regression_task = (
            ExtendedDataprocCreateBatchOperator(
                task_id=f'ecommerce_regression_data_{banner_suffix}',
                python_script_path=(
                    f'{PROJECT_NAME}/'
                    'scripts/'
                    'ecommerce_processed_regression_data.py'
                ),
                dag_env_config=dag_env_config,
                docker_image_name=PROJECT_NAME,
                pyspark_batch_args=[
                    '--project_id',
                    dag_env_config['project_id'],
                    '--execution_date',
                    EXECUTION_DATE,
                    '--store_banner',
                    store_banner,
                    '--use',
                    'ELASTICITY',
                ],
                include_paths=[
                    'common/',
                    f'{PROJECT_NAME}/gbq_objects/'
                ],
            )
        )

        # Sin ningun >> -- tarea aislada, no dispara ni depende de nada
        # mas.
        regression_tasks.append(regression_task)
