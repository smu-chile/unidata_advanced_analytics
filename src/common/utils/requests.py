"""Contains HTTP request common functions and classes."""

import json
import logging
import secrets

import requests
from requests import Timeout, Response, HTTPError, RequestException


# List with top 100 most used user agents in HTTP requests
__HTTP_AGENT_LIST = json.loads(
    requests.get(
        'https://raw.githubusercontent.com/microlinkhq/top-user-agents/master/src/index.json',
        timeout=60,
    ).content.decode()
)


def safeGet(request_function: callable, *, error_handling: str = 'raise',
            timeout: int = 60, **kwargs) -> Response:
    """Make a HTTP request rotating the user agent.

    User agents are taken from microlink.io list of the
    [top 100 HTTP user-agents most used over the Internet](https://raw.githubusercontent.com/microlinkhq/top-user-agents/refs/heads/master/src/index.json)
    . The function will allways update the ``User-Agent`` key from the
    headers passed.

    Parameters
    ----------
    request_function : callable
        ``Requests`` package function to be used for the HTTP request
    error_handling : {'raise', 'warn', 'silent'}, default='raise'
        Error handling mode
    timeout : int
        Time for the ``request_function`` to wait a response
    **kwargs
        Arguments passed on to the ``request_function`` to be called

    Returns
    -------
    request_response : Response
        ``request_function`` response
    """
    # If headers were given, updates only User-Agent key
    if 'headers' in kwargs:
        kwargs['headers']['User-Agent'] = secrets.choice(__HTTP_AGENT_LIST)
    # If headers weren't given, uses header with User-Agent only
    else:
        kwargs['headers'] = {'User-Agent': secrets.choice(__HTTP_AGENT_LIST)}

    try:
        # Get the the URL data
        return request_function(
            timeout=timeout, **kwargs
        )
    except (HTTPError, ConnectionError, RequestException, Timeout) as err_msg:
        if error_handling == 'warn':
            url = kwargs['url']
            logging.warning(f'{type(err_msg).__name__} when trying to access {url}')
        elif error_handling == 'raise':
            raise
        elif error_handling == 'silent':
            pass


if __name__ == '__main__':
    err_msg = 'This file is only meant to be imported.'
    raise Exception(err_msg)
