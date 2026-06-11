import os
import json
import logging
import argparse

from google.cloud import bigquery
from google.cloud.exceptions import NotFound


# ---------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s')

def build_select(columns):

    select_fields = []

    for column in columns:
        field_name = column['name']

        field_type = (
            column['field_type']
            .upper()
            .strip()
        )
        if field_name == 'FECHA_CARGA':
            expression = 'CURRENT_DATE() AS FECHA_CARGA'
        elif field_type == 'STRING':
            expression = (
                f'TRIM({field_name}) '
                f'AS {field_name}'
            )
        elif field_type in (
            'INT64',
            'INTEGER'
        ):
            expression = (
                f'SAFE_CAST('
                f'NULLIF(TRIM({field_name}), "") '
                f'AS INT64'
                f') AS {field_name}'
            )
        elif field_type == 'FLOAT64':
            expression = (
                f'SAFE_CAST('
                f'NULLIF(TRIM({field_name}), "") '
                f'AS FLOAT64'
                f') AS {field_name}'
            )
        elif field_type == 'NUMERIC':
            expression = (
                f'SAFE_CAST('
                f'NULLIF(TRIM({field_name}), "") '
                f'AS NUMERIC'
                f') AS {field_name}'
            )
        elif field_type == 'DATE':
            expression = (
                f'SAFE_CAST('
                f'NULLIF(TRIM({field_name}), "") '
                f'AS DATE'
                f') AS {field_name}'
            )
        elif field_type == 'DATETIME':
            expression = (
                f'CASE '
                f'WHEN NULLIF(TRIM({field_name}), "") IS NULL '
                f'THEN NULL '
                f'ELSE PARSE_DATETIME('
                f'"%b %e %Y %l:%M%p",'
                f'TRIM({field_name})'
                f') '
                f'END AS {field_name}'
            )
        elif field_type == 'TIMESTAMP':
            expression = (
                f'SAFE_CAST('
                f'NULLIF(TRIM({field_name}), "") '
                f'AS TIMESTAMP'
                f') AS {field_name}'
            )
        else:
            expression = (
                f'{field_name}'
            )
        select_fields.append(expression)
    return ',\n'.join(select_fields)

# ---------------------------------------------------------------------
# Arguments
# ---------------------------------------------------------------------
parser = argparse.ArgumentParser()
parser.add_argument('--project_id', required=True)
parser.add_argument('--schema_file', required=True)
parser.add_argument('--execution_date', required=False)

# ---------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------
def main() -> None:

    args = vars(parser.parse_args())
    project_id = args['project_id']
    schema_file = args['schema_file']

    json_path = os.path.join(
        os.path.dirname(
            os.path.dirname(__file__)
        ),
        'gbq_objects',
        schema_file)

    logging.info(f'Loading schema: {json_path}')


    logging.info(f'__file__ = {__file__}')
    logging.info(f'parent = {os.path.dirname(__file__)}')
    logging.info(f'parent parent = '
        f'{os.path.dirname(os.path.dirname(__file__))}')
    logging.info(f'json_path = {json_path}')

    logging.info(
        f'current_dir_files = '
        f'{os.listdir(os.path.dirname(__file__))}'
    )

    with open(json_path, encoding='utf-8') as f:
        metadata = json.load(f)
    columns = metadata['columns']
    dataset_id = metadata['schema']
    final_table_name = metadata['table']
    stg_table_name = 'CRM_DATA_SFMC_PUBLIST_STG'

    final_table = (
        f'{project_id}.'
        f'{dataset_id}.'
        f'{final_table_name}')

    stg_table = (
        f'{project_id}.'
        f'{dataset_id}.'
        f'{stg_table_name}')

    client = bigquery.Client(project=project_id)

    try:
        client.get_table(final_table)
        logging.info('Target table exists')

    except NotFound:
        raise Exception(f'Table not found: {final_table}')  # noqa: B904, EM102

    select_sql = (build_select(columns))

    query = f"""
    TRUNCATE TABLE `{final_table}`;
    INSERT INTO `{final_table}`
    SELECT
    {select_sql}
    FROM `{stg_table}`
    """  # noqa: S608

    logging.info('Executing transformation')
    job = client.query(query)
    job.result()
    logging.info('Process completed successfully')


if __name__ == '__main__':
    main()
