"""Extends usage for the sharepoint part of the MS Office365 library."""
from __future__ import annotations

import os

# Default
import logging
import posixpath
from io import BytesIO

# pip
import pandas as pd
from office365.sharepoint.files.file import File
from office365.sharepoint.client_context import ClientContext
from cryptography.hazmat.primitives.serialization import (
    Encoding,
    NoEncryption,
    PrivateFormat,
    pkcs12,
)


class SharePointFile:
    """Sharepoint file.

    Parameters
    ----------
    tenant : str,
        Tenant name. Also known as directory ID when creating the `.pfx`
        certificate
    client_id : str,
        The OAuth client id of the calling application. Also known as
        API ID when creating the `.pfx` certificate
    thumbprint : str,
        Hex encoded thumbprint of the certificate
    private_key : str,
        A PEM encoded certificate private key
    server_relative_path : str
        Relative path to the file in Sharepoint e.g.
        `/sites/SiteName/SPDirectory1/SPDirectory2/file.xlsx`
    """
    _sharepoint_server = 'https://corpsmu.sharepoint.com'

    def __init__(
            self,
            tenant: str,
            client_id: str,
            thumbprint: str,
            private_key: str,
            server_relative_path: str,
        ):
        self.server_relative_path = server_relative_path

        # Construct site_url
        self.site_url = posixpath.join(
            self._sharepoint_server,
            *server_relative_path.split(posixpath.sep)[:3]
        )

        # Create client context for API connection
        self._client_context = ClientContext(
            self.site_url + posixpath.sep
        ).with_client_certificate(
            tenant=tenant,
            client_id=client_id,
            thumbprint=thumbprint,
            private_key=private_key,
        )


    def toFrame(self, **kwargs) -> pd.DataFrame:
        """Get the file as a Pandas DataFrame.

        Parameters
        ----------
        **kwargs
            Arguments passed on to the `pd.read_excell` function

        Returns
        -------
        file_df : pd.DataFrame
            Pandas DataFrame with the contents of the file
        """
        with File.open_binary(
            self._client_context,
            self.server_relative_path
        ) as response:
            # Handler of oppened file
            if response.status_code == 200:
                logging.info(f'File {self.server_relative_path} exists on SharePoint')
            elif response.status_code == 404:
                err_msg = f'File {self.server_relative_path} not found in SharePoint'
                logging.error(err_msg)
                raise FileNotFoundError(err_msg)
            else:
                err_msg = f'Error {response.status_code} on getting file {self.server_relative_path}'  # noqa: E501
                logging.error(err_msg)
                raise Exception(err_msg)

            # Read XLSX file as DF
            return pd.read_excel(
                BytesIO(response.content),
                **kwargs
            )


    def lastTimeModified(self) -> str:
        """Get the ISO8601 datetime of the last time the file was modified.

        Returns
        -------
        modification_datetime : str
            Last time the file was modified
        """
        target_file = self._client_context.web.get_file_by_server_relative_path(
            self.server_relative_path
        )
        self._client_context.load(target_file)
        self._client_context.execute_query()
        return target_file.time_last_modified


    def upload(self, content: BytesIO) -> None:
        """Upload a file to SharePoint.

        Parameters
        ----------
        content : bytes
            Contents of the file that will be uploaded as bytes.

        Notes
        -----
        This function will be slow on large files (>4Mb). Its possible to
        make it fast when it comes to uploading them but is a fucking pain
        to work with the microsoft API. Please ask the theam if you need
        this functionality.
        """
        path, filename = posixpath.split(self.server_relative_path)

        # Set target directory in SharePoint
        target_dir = self._client_context.web.get_folder_by_server_relative_url(
            posixpath.join(*path.split(posixpath.sep)[3:])
        )

        # Upload
        target_dir.files.upload(
            path_or_file=content, file_name=filename
        ).execute_query()


def unpackPFXCredentials(pfx_path: str, pfx_password: str) -> tuple[str, str]:
    """Unpack `.pfx` file with SharePoint credentials into `.pem` files

    Takes the path to a `.pfx` file and creates two `.pem` files with the
    same name as the original with the suffix:

    - _pk: for the `.pem` public key
    - _sk: for the `.pem` secret key

    Parameters
    ----------
    pfx_path : str
        Path to `.pfx` file with the SharePoint credentials
    pfx_password: str
        Password for the `.pfx` file

    Returns
    -------
    pk_path : str
        Path to the `.pem` file with the public key
    sk_path : str
        Path to the `.pem` file with the secret key
    """
    pk_path = os.path.splitext(pfx_path)[0] + '_pk.pem'
    sk_path = os.path.splitext(pfx_path)[0] + '_sk.pem'

    # Transform .pfx to .pem
    with open(pfx_path, 'rb') as pfx_file, \
        open(sk_path, 'wb') as sk_file, open(pk_path, 'wb') as pk_file:
        private_key, certificate, _ = pkcs12.load_key_and_certificates(
            data=pfx_file.read(),
            password=pfx_password.encode(),
        )

        sk_file.write(
            private_key.private_bytes(
                encoding=Encoding.PEM,
                format=PrivateFormat.PKCS8,
                encryption_algorithm=NoEncryption()
            )
        )

        pk_file.write(
            certificate.public_bytes(Encoding.PEM)
        )

    return pk_path, sk_path


if __name__ == '__main__':
    err_msg = 'This file is only meant to be imported.'
    raise Exception(err_msg)
