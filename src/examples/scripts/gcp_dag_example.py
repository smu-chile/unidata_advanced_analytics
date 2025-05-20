import os
import sys
import json
import logging
import subprocess
from logging import config


with open(os.path.join('common', 'config', 'logging.json')) as f:
    config.dictConfig(json.load(f))

def main():
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
