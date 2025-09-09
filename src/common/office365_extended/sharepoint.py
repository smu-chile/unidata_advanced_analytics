"""Extends usage for the sharepoint part of the MS Office365 library."""
from __future__ import annotations

# Default
import logging
import posixpath
from io import BytesIO

# pip
import pandas as pd
from office365.sharepoint.files.file import File
from office365.sharepoint.client_context import ClientContext
from office365.runtime.auth.client_credential import ClientCredential


class SharePointFile:
    """Sharepoint file.

    Parameters
    ----------
    client_id : str
        Sharepoint web API client id
    client_secret : str
        Sharepoint web API client secret
    server_relative_path : str
        Relative path to the file in Sharepoint e.g.
        `/sites/SiteName/SPDirectory1/SPDirectory2/file.xlsx`
    """
    _sharepoint_server = 'https://corpsmu.sharepoint.com'

    def __init__(
            self, client_id: str, client_secret: str, server_relative_path: str
        ):
        self.server_relative_path = server_relative_path

        # Construct site_url
        self.site_url = posixpath.join(
            self._sharepoint_server,
            *server_relative_path.split(posixpath.sep)[:3]
        )

        # Stablish client context
        self._client_context = ClientContext(
            self.site_url + posixpath.sep
        ).with_credentials(
            ClientCredential(
                client_id,
                client_secret
            )
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


    def upload(self, content: bytes, root_dir: str = 'Documentos') -> None:
        """Upload a file to SharePoint.

        ..warning:: This method deletes

        Parameters
        ----------
        content : bytes
            Contents of the file that will be uploaded as bytes.
        root_dir : str, default='Documentos'
            Name of the left side list (on SharePoint web interface) in
            which the file will be uploaded.
        """
        path, filename = posixpath.split(self.server_relative_path)

        # Set target directory in SharePoint
        target_dir = self._client_context.web.lists.get_by_title(root_dir).root_folder
        subdirs = path.split(posixpath.sep)[4:]
        for subdir in subdirs:
            target_dir = target_dir.folders.get_by_url(subdir)

        # Remove file if allready exists
        files = target_dir.files
        for file in files:
            if file.name == filename:
                file.delete_object()
                self._client_context.execute_query()

        # Upload
        target_dir.files.upload(
            filename, content
        ).execute_query()


if __name__ == '__main__':
    err_msg = 'This file is only meant to be imported.'
    raise Exception(err_msg)
