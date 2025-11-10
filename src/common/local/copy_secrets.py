"""Copy Secret Manager secrets from one project to another."""
import os
import sys
import json
import logging
import argparse
from logging import config


sys.path.append(os.path.abspath(os.path.join(os.path.abspath(__file__), '..', '..')))
from constants import LOGGING_CONFIG  # noqa: TID252
from gcp_extended.secretsmanager import getSecret, setSecret  # noqa: TID252


# -------------------------------------------------------------------------
# Package config
# -------------------------------------------------------------------------
# Logging config
config.dictConfig(LOGGING_CONFIG)
# Parser
parser = argparse.ArgumentParser()
parser.add_argument(
    'source_project', type=str,
    help='Source project'
)
parser.add_argument(
    'target_project', type=str,
    help='Target project'
)
parser.add_argument(
    'secret_names', type=str, nargs='+',
    help='Names of the secrets to be copied'
)


# -------------------------------------------------------------------------
# Main
# -------------------------------------------------------------------------
def main() -> None:
    args = vars(parser.parse_args())
    # Environment
    source_project: str = args['source_project']
    target_project: str = args['target_project']
    secret_names: str = args['secret_names']

    # Same soruce and target
    if source_project == target_project:
        err_msg = 'Source and target projects cannot be the same.'
        raise Exception(err_msg)

    # Get variables
    for secret_name in secret_names:
        logging.info(f'Copying {secret_name} from {source_project} to {target_project}')
        setSecret(
            secret=json.dumps(
                getSecret(
                    secret_name=secret_name,
                    project=source_project,
                )
            ),
            secret_name=secret_name,
            project=target_project
        )
    logging.info('Done!')


if __name__ == '__main__':
    main()
