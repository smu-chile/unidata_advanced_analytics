"""Extension of the google.cloud.secretmanager modules."""
import json

from google.cloud.secretmanager import SecretManagerServiceClient


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
    if project == 'cl-bigdata-analytics':
        project_number = 156006129315
    elif project == 'cl-bigdata-analytics-preprod':
        project_number = 125582050147
    elif project == 'cl-bigdata-analytics-prod':
        project_number = 987985293469
    else:
        project_number = 0

    # Get the secret as dict
    return json.loads(
        SecretManagerServiceClient().access_secret_version(
            name=f'projects/{project_number}/secrets/{secret_name}/versions/latest'
        ).payload.data
    )

if __name__ == '__main__':
    err_msg = 'This file is only meant to be imported.'
    raise Exception(err_msg)
