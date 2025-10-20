import os
import re
import json
import logging
import argparse
from logging import config

# pip
import bs4 as bs
import pandas as pd
import pendulum
import requests
from google.cloud.bigquery import Client

from common.constants import LOGGING_CONFIG

# Own
from common.utils.requests import safeGet
from common.utils.data_transform import normalizeText
from common.gcp_extended.bigquery import uploadFrame, deleteFromTable


# -------------------------------------------------------------------------
#  Config
# -------------------------------------------------------------------------
# Logging config
config.dictConfig(LOGGING_CONFIG)
# Parser config
parser = argparse.ArgumentParser()
parser.add_argument(
    '--project_id', type=str,
    help='GCP project in which the script will be executed'
)
parser.add_argument(
    '--execution_date', type=str,
    help='DAG execution date'
)


# -------------------------------------------------------------------------
#  Main function
# -------------------------------------------------------------------------
def main() -> None:  # noqa: D103
    args = vars(parser.parse_args())
    gcp_project: str = args['project_id']
    execution_date: pendulum.Date = pendulum.date(*map(int, args['execution_date'].split('-')))

    # Static
    gbq_client = Client()

    months = {
        1: 'enero',
        2: 'febrero',
        3: 'marzo',
        4: 'abril',
        5: 'mayo',
        6: 'junio',
        7: 'julio',
        8: 'agosto',
        9: 'septiembre',
        10: 'octubre',
        11: 'noviembre',
        12: 'diciembre',
    }

    # Get url
    html_request = safeGet(requests.get, url='https://www.feriados.cl/index.php')
    html_content = bs.BeautifulSoup(html_request.content, features='lxml')

    # Set anchor as the holiday previous to new year
    new_year_anchor = html_content.find(string='Año Nuevo').parent.parent.previous_sibling
    # Iterate through the available holidays
    holidays_df = pd.DataFrame([(
            # Name
            html_row.find_all('td')[1].text.split('\n')[0],
            # Date
            execution_date.replace(
                month=next(
                    k for k, v in months.items()
                    if v in html_row.find('td').text.lower()
                ),
                day=int(re.search(r'\d+', html_row.find('td').text).group())
            ).isoformat(),
            # Essential
            'irrenunciable' in html_row.find_all('td')[1].text.lower()
        )
        for html_row in new_year_anchor.find_next_siblings('tr')
        ])

    logging.info('Normalizing holiday names...')
    holidays_df[0] = holidays_df[0].apply(
        normalizeText,
        lower=True,
        strip_accents=True,
        replace_spaces='_',
    )

    # Remove registers from the same year
    with open(os.path.join('gbq_objects', 'dim_holidays.json')) as f:
        tbl_config = json.load(f)
        deleteFromTable(
            table_ref=(
                gcp_project
                + '.' + tbl_config['schema']
                + '.' + tbl_config['table']
            ),
            where_clause=f'EXTRACT(YEAR FROM date) = {execution_date.year}',
            gbq_client=gbq_client
        )

    # Upload to GBQ
    uploadFrame(
        df=holidays_df,
        table_ddl_json_path=os.path.join('gbq_objects', 'dim_holidays.json'),
        project=gcp_project,
        gbq_client=gbq_client,
        if_exists='append',
    )
    logging.info('DataFrame uploaded to GBQ! :)')


if __name__ == '__main__':
    main()
