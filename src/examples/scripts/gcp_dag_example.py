"""Example GCP script.

Example on how to import modules from commons.
"""
# Default
import sys
import logging
import subprocess
from logging import config

# Own
from common.constants import LOGGING_CONFIG, SHORT_STORE_BANNERS
from common.utils.queries import QueryDict


# -------------------------------------------------------------------------
# Logging config
# -------------------------------------------------------------------------
config.dictConfig(LOGGING_CONFIG)


# -------------------------------------------------------------------------
# SQL Queries
# -------------------------------------------------------------------------
_SQL_QUERIES = QueryDict({
    'example_query':
    """
    SELECT * FROM ${table} LIMIT 10;
    """
})

def main() -> None:  # noqa: D103
    logging.info('Store banner mapping:')
    for og_store_banner, short_store_banner in SHORT_STORE_BANNERS.items():
        logging.info(f'{og_store_banner} -> {short_store_banner}')

    logging.info(
        f'Example query: {_SQL_QUERIES['example_query'].substitute(table='schema.table')}'
    )

    python_version = subprocess.run(
        ['python', '--version'],
        capture_output=True
    ).stdout.decode()
    logging.info(python_version)

    pip_version = subprocess.run(
        ['pip', '--version'],
        capture_output=True
    ).stdout.decode()
    logging.info(pip_version)

    pip_freeze = subprocess.run(
        ['pip', 'freeze'],
        capture_output=True
    ).stdout.decode()
    for lib in pip_freeze.split('\n'):
        logging.warning(lib)

    for p in sys.path:
        logging.error(p)


if __name__ == '__main__':
    main()
