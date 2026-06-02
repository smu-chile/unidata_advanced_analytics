
from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.python import PythonOperator
from airflow.providers.sftp.hooks.sftp import SFTPHook
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import (
    GCSToBigQueryOperator,
)


# =========================
# CONFIG
# =========================

PROJECT_ID = 'cl-bigdata-analytics-preprod'

BUCKET = 'cl-bigdata-analytics-preprod-us-sandbox-datasets'

REMOTE_FILE = 'Import/PublicationListAutomation/PUBLICATION_LIST_AUTOMATION_20260601.csv'

LOCAL_FILE = '/tmp/PUBLICATION_LIST_AUTOMATION_20260601.csv'  # noqa: S108

GCS_PATH = 'CRM/PUBLICATION_LIST_AUTOMATION_20260601.csv'

BQ_DATASET = 'crm'
BQ_TABLE = 'CRM_DATA_SFMC_PUBLIST'

# =========================
# DAG
# =========================

default_args = {
    'owner': 'ingest',
}

with DAG(  # noqa: AIR311
    dag_id='salesforce_sfmc_publist_sftp_to_gcs',
    default_args=default_args,
    start_date=days_ago(1),  # noqa: AIR301
    schedule_interval=None,  # noqa: AIR301
    catchup=False,
    tags=['sftp', 'gcs', 'bigquery', 'sfmc'],
) as dag:

    # =========================
    # 1. SFTP → LOCAL
    # =========================
    def download_from_sftp():
        hook = SFTPHook(ssh_conn_id='sftp_conn')
        hook.retrieve_file(
            remote_full_path=REMOTE_FILE,
            local_full_path=LOCAL_FILE,
        )

    task_sftp_download = PythonOperator(  # noqa: AIR001, AIR312
        task_id='sftp_to_local',
        python_callable=download_from_sftp,
    )

    # =========================
    # 2. LOCAL → GCS
    # =========================
    def upload_to_gcs():
        hook = GCSHook()
        hook.upload(
            bucket_name=BUCKET,
            object_name=GCS_PATH,
            filename=LOCAL_FILE,
        )

    task_upload_gcs = PythonOperator(  # noqa: AIR001, AIR312
        task_id='local_to_gcs',
        python_callable=upload_to_gcs,
    )

    # =========================
    # 3. GCS → BIGQUERY
    # =========================
    task_load_bq = GCSToBigQueryOperator(
        task_id='gcs_to_bigquery',
        bucket=BUCKET,
        source_objects=[GCS_PATH],
        destination_project_dataset_table=f'{PROJECT_ID}:{BQ_DATASET}.{BQ_TABLE}',
        source_format='CSV',
        skip_leading_rows=1,
        write_disposition='WRITE_TRUNCATE',
        autodetect=True,
    )

    # =========================
    # FLOW
    # =========================

    task_sftp_download >> task_upload_gcs >> task_load_bq  # noqa: W292
