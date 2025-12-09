"""Extension of the google.cloud.secretmanager modules."""
import json

import google.cloud.secretmanager as secretmanager
from google.cloud.exceptions import NotFound
from google.api_core.exceptions import AlreadyExists


__PROJECT_NUMBERS = {
    'cl-bigdata-analytics': 156006129315,
    'cl-bigdata-analytics-dev': 156006129315,
    'cl-bigdata-analytics-preprod': 125582050147,
    'cl-bigdata-analytics-prod': 987985293469,
}


def _getProjectNumber(project_name: str) -> int:
    """Get internal GCP project number from project id or fantasy name.

    Parameters
    ----------
    project_name : str
        Project ID or fantasy name for the project

    Returns
    -------
    project_number : int
        Internal GCP project number if the project exists

    Raises
    ------
    NotFound
        When project ID or fantasy name is erroneous
    """
    # Raise exception when project name is not one of the expected
    if project_name not in __PROJECT_NUMBERS:
        err_msg = 'Project is misspeled or does not exist.'
        raise NotFound(err_msg)

    return __PROJECT_NUMBERS[project_name]


def getSecret(secret_name: str, project: str = 'cl-bigdata-analytics') -> dict:
    """Get secret value as `dict` from GCP Secrets Manager.

    Parameters
    ----------
    secret_name : str
        Name of the secret in GCP Secrets Manager
    project : str
        Name of the account in which the secret is hosted

    Returns
    -------
    secret : dict
        Dictionary with the secret.
    """
    # Build project number
    project_number = _getProjectNumber(project)
    # Get the secret as dict
    return json.loads(
        secretmanager.SecretManagerServiceClient().access_secret_version(
            name=f'projects/{project_number}/secrets/{secret_name}/versions/latest'
        ).payload.data
    )


def setSecret(secret: str, secret_name: str, project: str) -> None:
    """Set secret on GCP Secrets Manager, creating them if needed.

    Parameters
    ----------
    secret : str
        Content of the secret. Must be a json string
    secret_name : str
        Name of the secret in GCP Secrets Manager
    project : str
        Name of the account in which the secret wll be hosted
    """
    secret_manager_client = secretmanager.SecretManagerServiceClient()
    # Build project number
    project_number = _getProjectNumber(project)

    # Create secret if does not exist
    try:
        secret_manager_client.create_secret(
            request={
                'parent': f'projects/{project_number}',
                'secret_id': secret_name,
                'secret': secretmanager.Secret(
                    replication=secretmanager.Replication(
                        automatic=secretmanager.Replication.Automatic(),
                    ),
                )
            }
        )
    # Raise all exceptions except AlreadyExists
    except AlreadyExists:
        pass
    except Exception:
        raise

    # Add secret version
    secret_manager_client.add_secret_version(
        request={
            'parent': f'projects/{project_number}/secrets/{secret_name}',
            'payload': {
                'data': secret.encode(),
            },
        }
    )


if __name__ == '__main__':
    err_msg = 'This file is only meant to be imported.'
    raise Exception(err_msg)
