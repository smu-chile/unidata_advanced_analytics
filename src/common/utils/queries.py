from __future__ import annotations

from string import Template
from textwrap import dedent


class QueryDict:
    """Wrapper over the Template class for SQL Queries.

    Parameters
    ----------
    query_dict : dict [str, str]
        Dictionary with the SQL query names as keys and text as values
    common_query : str
        Common part of all the queries in the corpus. All queries will be
        appendend to this common query
    """
    def __init__(self, query_dict: dict[str, str], common_query: str = ''):
        self.common_query = common_query
        self._query_dict = {
            k: Template(
                dedent(common_query) + '\n' + dedent(v) if common_query
                else dedent(v)
            ) for k, v in query_dict.items()
        }

    def __getitem__(self, query_name: str) -> Template:
        return self._query_dict[query_name]

    def __setitem__(self, key: str, value: str) -> None:
        self._query_dict[key] = Template(
            dedent(self.common_query) + '\n' + dedent(value) if self.common_query
            else dedent(value)
        )


    def substitute(self, query_name: str, **kwargs) -> str:
        """Query parameter substitution.

        Calls ``substitute()`` over a query Template. Returns the
        query text as string.

        Parameters
        ----------
        query_name : str
            Name of the query
        **kwargs : dict | keywords
            Arguments passed to the ``substitute()`` method of the Template
            class
        """
        return self[query_name].substitute(kwargs)


    def safe_substitute(self, query_name: str, **kwargs) -> str | Template:
        """Safe query parameter substitution.

        Calls ``safe_substitute()`` over a query Template. Returns the
        query text as string if all the placeholders were filled, in other
        case returns a the query text as Template with the remaining
        placeholders.

        Parameters
        ----------
        query_name : str
            Name of the query
        **kwargs : dict | keywords
            Arguments passed to the ``safe_substitute()`` method of the
            Template class
        """
        query = self[query_name].safe_substitute(kwargs)
        return query if '$' not in query else Template(query)


if __name__ == '__main__':
    err_msg = 'This file is only meant to be imported.'
    raise Exception(err_msg)
