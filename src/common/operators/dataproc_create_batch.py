import posixpath

from airflow.providers.google.cloud.operators.dataproc import (
    DataprocCreateBatchOperator,
)


class ExtendedDataprocCreateBatchOperator(DataprocCreateBatchOperator):
    template_fields = (
        'pyspark_batch_args', 'batch_id'
    )

    def __init__(
            self, task_id: str, project_name: str, python_script_path: str,
            dag_env_config: dict[str, str],
            pyspark_batch_args: list = (), include_paths: list[str] = (),
            spark_driver_cores: int = 4, spark_driver_memory: int = 10,
            ttl: int = 14400,
            **kwargs
        ):
        # Set attributes
        self.task_id = task_id
        self.project_name = project_name
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
                        f"{self.project_name.replace('_', '-')}:latest"
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

    def execute(self, context):
        # Execute the parent operator
        super().execute(context=context)


if __name__ == '__main__':
    err_msg = 'This file is only meant to be imported.'
    raise Exception(err_msg)
