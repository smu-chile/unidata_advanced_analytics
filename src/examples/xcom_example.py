#-----------------------
# Librerias
#-----------------------
# Default
import os
from datetime import timedelta

# pip
import pendulum
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator


# Globals
PROJECT_NAME = 'examples'
GCP_PROJECT_ID =  '{{ var.value.develop_smu_unidata_default_project_id }}'

dag_args = {
    'dag_id': 'xcom_example',
    'schedule_interval': None,
    'dagrun_timeout': None,
    'catchup': False,
    'max_active_runs': 1,
    'concurrency': 1,
    'tags': [PROJECT_NAME, 'ecastrot'],
    'default_args': {
        'project_id': GCP_PROJECT_ID,
        'region': '{{ var.value.develop_smu_unidata_default_region }}',
        'owner': 'BIGDATA_ANALYTICS',
        'email': ['ecastrot@unidata.cl'],
        'start_date': None,
        'depends_on_past': False,
        'catchup': False,
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 0,
        'retry_delay': timedelta(minutes=5)
    }
}


def pushMonthID(**kwargs) -> None:
    """Calculate last p_month from execution_date and push it to XCom."""
    month_id = pendulum.date(
        *list(map(int, kwargs['templates_dict']['execution_date'].split('-')))
    ).add(
        months=-1
    ).strftime('%Y%m')

    kwargs['ti'].xcom_push(
        key='xcom_example_monthid',
        value=month_id,
    )


def dummyProcess(x, **kwargs) -> None:
    """Print its arguments."""
    # Barebones variables can be accesed directly
    print(f'x: {x}')
    # Templated variables must be accessed via kwargs
    print(f"templated execution_date: {kwargs['templates_dict']['execution_date']}")
    print(f"templated month_id: {kwargs['templates_dict']['month_id']}")


with DAG(**dag_args) as dag:
    # -------------
    # Variables
    # -------------
    env = os.getenv('DEPLOY_ENV')
    execution_date = "{{ dag.timezone.convert(data_interval_end).strftime('%Y-%m-%d') }}"
    month_id = "{{ ti.xcom_pull(task_ids='push_task', key='xcom_example_monthid') }}"

    push_task = PythonOperator(
        task_id='push_task',
        python_callable=pushMonthID,

        # Templated variables
        templates_dict={
            'execution_date': execution_date,
        }
    )

    # Example with PythonOperator
    pull_python_task = PythonOperator(
        task_id='pull_python_task',
        python_callable=pushMonthID,
        # Barebone variables
        op_kwargs={
            'x': 1,
        },
        # Templated variables
        templates_dict={
            'execution_date': execution_date,
            'month_id': month_id
        }
    )

    # Example with BashOperator
    pull_bash_task = BashOperator(
        task_id='pull_bash_task',
        bash_command=(
            'echo'
            f' {env}'
            f' {execution_date}'
            f' {month_id}'
        ),
    )

push_task >> [pull_python_task, pull_bash_task]
