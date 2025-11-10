"""Copy Artifact Registry images from one project to another."""
import os
import sys
import logging
import argparse
import subprocess
from logging import config


sys.path.append(os.path.abspath(os.path.join(os.path.abspath(__file__), '..', '..')))
from constants import LOGGING_CONFIG  # noqa: TID252


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
    'image_names', type=str, nargs='+',
    help='Names of the images to be copied'
)


# -------------------------------------------------------------------------
# Main
# -------------------------------------------------------------------------
def main() -> None:
    args = vars(parser.parse_args())
    # Get variables
    source_project: str = args['source_project']
    target_project: str = args['target_project']
    image_names: str = args['image_names']

    # Fix alias from the dev project
    if source_project == 'cl-bigdata-analytics-dev':
        source_project = 'cl-bigdata-analytics'
    elif target_project == 'cl-bigdata-analytics-dev':
        target_project = 'cl-bigdata-analytics'

    if source_project == target_project:
        err_msg = 'Source and target projects cannot be the same.'
        raise Exception(err_msg)

    for image_name in image_names:
        logging.info(f'Copying {image_name} from {source_project} to {target_project}')
        subprocess.run([
            'gcrane', 'cp',
            f'us-east1-docker.pkg.dev/{source_project}/dataproc-worker-images/{image_name}',
            f'us-east1-docker.pkg.dev/{target_project}/dataproc-worker-images/{image_name}'
        ])
    logging.info('Done!')


if __name__ == '__main__':
    main()
