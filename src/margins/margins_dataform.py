# Default
import json
from datetime import timedelta

# pip
import pendulum
from airflow.models import DAG
from airflow.configuration import conf
from airflow.providers.google.cloud.operators.dataform import (
    DataformCreateCompilationResultOperator,
    DataformCreateWorkflowInvocationOperator,
)


# Globals
PROJECT_NAME = 'margins'
with open(
    f'{conf.get("core", "dags_folder")}/'
    'BRANCH_PLACEHOLDER/'
    'smu-chile/unidata_advanced_analytics/src/common/constants/dag_env_config.json'
) as f:
    dag_env_config = json.load(f)['BRANCH_PLACEHOLDER']
GCP_PROJECT_ID = dag_env_config['project_id']
REGION = dag_env_config['region']
SCRIPTS_GCS =  dag_env_config['scripts_gcs']
SERVICE_ACCOUNT = dag_env_config['g_service_account']
NETWORK = dag_env_config['network']
SUBNETWORK = dag_env_config['subnetwork']


dag_args = {
    'dag_id': 'margins_dataform',
    'schedule_interval': None,
    'dagrun_timeout': None,
    'catchup': False,
    'max_active_runs': 1,
    'concurrency': 1,
    'tags': [PROJECT_NAME, 'csotob'],
    'default_args': {
        'project_id': GCP_PROJECT_ID,
        'region': REGION,
        'owner': 'BIGDATA_ANALYTICS',
        'email': ['csotob@unidata.cl'],
        'start_date': pendulum.datetime(
            2025, 6, 25,
            tz=pendulum.timezone('America/Santiago')
        ),
        'depends_on_past': False,
        'catchup': False,
        'email_on_failure': True,
        'email_on_retry': False,
        'retries': 0,
        'retry_delay': timedelta(minutes=5)
    }
}

with DAG(**dag_args) as dag:
    EXECUTION_DATE = "{{ dag_run.conf.get('execution_date', dag.timezone.convert(data_interval_end).strftime('%Y-%m-%d')) }}"  # noqa: E501

    margins_create_compilation_result = DataformCreateCompilationResultOperator(
        task_id = 'margins_create_compilation_result',
        project_id = 'cl-cda-unidata-dev',
        region = 'us-central1',
        repository_id = 'bi-dataform-uni-reporte-margen',
        compilation_result={
            'workspace': (
                'projects/cl-cda-unidata-dev/locations/us-central1/'
                'repositories/bi-dataform-uni-reporte-margen/workspaces/uni-dev'
            )
        },
    )

    margins_workflow_invocation =  DataformCreateWorkflowInvocationOperator(
         task_id='margins_workflow_invocation',
        project_id = 'cl-cda-unidata-dev',
        region = 'us-central1',
        repository_id = 'bi-dataform-uni-reporte-margen',
          workflow_invocation={
             'compilation_result': "{{ task_instance.xcom_pull('margins_create_compilation_result')['name'] }}",  # noqa: E501
             'invocation_config': { 'included_tags': ['PASO1'], 'serviceAccount': 'service-751841178454@gcp-sa-dataform.iam.gserviceaccount.com' }  # noqa: E501

         },
)

margins_create_compilation_result >> margins_workflow_invocation
