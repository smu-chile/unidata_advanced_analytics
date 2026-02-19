import posixpath  # noqa: D100

from airflow.exceptions import AirflowSkipException
from airflow.providers.google.cloud.hooks.dataproc import DataprocHook
from airflow.providers.google.cloud.operators.dataproc import (
    DataprocCreateBatchOperator,
)


class ExtendedDataprocCreateBatchOperator(DataprocCreateBatchOperator):
    """Extended Dataproc Create Batch Operator.

    Wrapper over DataprocCreateBatchOperator. Keeps the same base
    functionality but exposes only relevant configuration and makes the
    general DAG code more readable.

    The tasks created by this operator set up an automatic batch_id
    extracting the uuid4 from the Airflow macros. Once up the task
    will defer its tracking to the Airflow Trigger.

    Parameters
    ----------
    task_id : str
        The unique task_id for the Airflow operator instance.
    python_script_path : str
        Relative path from the ``src`` directory to the file that will
        be executed.
    dag_env_config : dict[str, str]
        Environment variables to be set for the Dataproc job.
    docker_image_name : str
        The GCP project ID where the Dataproc batch job will execute.
    pyspark_batch_args : list
        Command-line arguments passed directly to the PySpark script.
    include_paths : list
        Relative path from the ``src`` directory to the additional
        files, jars or archives to be included.
    spark_driver_cores : int
        The number of CPU cores allocated for the Spark driver.
    spark_driver_memory : int
        The memory allocated for the Spark driver.
    ttl : int
        Time-to-live (TTL) for the Dataproc batch in seconds.
    **kwargs
        Other keyword arguments passed on to the
        DataprocCreateBatchOperator
    """
    template_fields = (
        *DataprocCreateBatchOperator.template_fields,
        'pyspark_batch_args', 'batch_id'
    )

    def __init__(
            self, task_id: str, python_script_path: str,
            dag_env_config: dict[str, str], docker_image_name: str,
            pyspark_batch_args: list = (), include_paths: list[str] = (),
            spark_driver_cores: int = 4, spark_driver_memory: int = 10,
            ttl: int = 14400,
            **kwargs
        ):
        # Set attributes
        self.task_id = task_id
        self.docker_image_name = docker_image_name
        self.python_script_path = python_script_path
        self.dag_env_config = dag_env_config
        self.pyspark_batch_args = pyspark_batch_args
        self.include_paths = include_paths
        self.spark_driver_cores = spark_driver_cores
        self.spark_driver_memory = spark_driver_memory
        self.ttl = ttl

        # Instanciate the DataprocCreateBatchOperator operator
        super().__init__(
            # External task configuration
            task_id=self.task_id,

            # Set batch identifyer
            batch_id='batch-{{ macros.uuid.uuid4() }}',

            # Name of the project
            project_id=self.dag_env_config['project_id'],

            # Leave task tracking to the Airflow Trigger
            deferrable=True,

            # Internal task configuration
            batch={
                'pyspark_batch': {
                    # Main file to run in the dataproc pod
                    'main_python_file_uri': (
                        f'gs://{self.dag_env_config["scripts_gcs"]}/'
                        f'{self.python_script_path}'
                    ),
                    # Include files so they can be imported on execution
                    'python_file_uris': [posixpath.join(
                            f'gs://{self.dag_env_config["scripts_gcs"]}/',
                            partial_path
                        )
                        for partial_path in self.include_paths
                    ],
                    # For Google Big Query read/write
                    'jar_file_uris': ['gs://spark-lib/bigquery/spark-3.5-bigquery-0.42.2.jar'],
                    # Main file arguments
                    'args': self.pyspark_batch_args,
                },

                # Docker image to be used in the dataproc pod
                'runtime_config': {
                    'version': '2.2',
                    'container_image': (
                        'us-east1-docker.pkg.dev/'
                        f'{self.dag_env_config["project_id"]}/'
                        'dataproc-worker-images/'
                        f"{self.docker_image_name.replace('_', '-')}:latest"
                    ),

                    # Executor hardware config
                    'properties': {
                        # Executor instances (Bare minimum config)
                        'spark.executor.instances': '2',
                        'spark.executor.cores': '4',
                        'spark.executor.memory': '4096m',
                        # Dirver instances
                        'spark.driver.cores': str(self.spark_driver_cores),
                        'spark.driver.memory': f'{self.spark_driver_memory}g',
                    },
                },

                # Privileges config
                'environment_config': {
                    'execution_config': {
                        'service_account': self.dag_env_config['g_service_account'],
                        'network_uri': self.dag_env_config['network'],
                        'subnetwork_uri': self.dag_env_config['subnetwork'],
                        'ttl': f'{self.ttl}s',
                    },
                },
            },

            **kwargs
        )

    def execute(self, context) -> None:  # noqa: ANN001, D102
        # Execute the parent operator
        super().execute(context=context)

    def execute_complete(self, context, event=None):
        if event and event.get('batch_state') == 'FAILED':
            hook = DataprocHook()
            batch = hook.get_batch(
                batch_id=event['batch_id'],
                region=self.region,
                project_id=self.project_id
            )

            # state_history is a list of status objects.
            # The latest one (index -1) contains the final failure reason.
            if batch.status.state_history:
                # Look at the most recent state transition details
                final_status = batch.status.state_history[-1]
                details = getattr(final_status, 'details', '').lower()

                self.log.info(f'Final state details: {details}')

                # GCP usually reports this as "Google Cloud Dataproc Agent
                # reports job failure... exitCode: 10"
                if 'exitcode: 10' in details or 'exit code: 10' in details:
                    self.log.info('Detected exit code 10 in state history. Skipping.')
                    msg = 'Process skipped via exit code 10.'
                    raise AirflowSkipException(msg)

        # Fallback to parent behavior if not a skip
        return super().execute_complete(context=context, event=event)


if __name__ == '__main__':
    err_msg = 'This file is only meant to be imported.'
    raise Exception(err_msg)
