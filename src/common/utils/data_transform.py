from __future__ import annotations

import re
import unicodedata
from typing import Iterable, Generator

import numpy as np


def zeroLevelDict(dictionary: dict, rename: dict | None = None, __prev_k: str='') -> Generator:
    """Turn any multi-level dictionary to a zero-level dictionary.

    For example, turns the dict: {'a': 1, 'b': {'c':{'e': 3}, 'd': 2}} into
    the dict: {'a': 1, 'b_c_e': 3, 'b_d': 2}.
    This function returns a generator of the new keys and values.


    Parameters
    ----------
    dictionary: dict
        The original multi-level dict
    rename: dict, optional
        Dictionary with mapping of individual keys in the original
        dictionary to new ones

    Returns
    -------
    new_k: str
        Joined key in the generated zero-level dict
    v: str
        Value corresponding to the last joined key in the original
        multi-level dict
    """
    # Assign rename default
    if rename is None:
        rename = {}
    # Verify rename type if given
    if not isinstance(rename, dict):
        err_msg = (
            'The rename argument must be of dict type. '
            f'You pass {type(rename)}'
        )
        raise TypeError(err_msg)

    for k, v in dictionary.items():
        # Handles the key exposed to the user
        joined_k = f'{__prev_k}_{rename.get(k, k)}' if __prev_k else rename.get(k, k)
        # Iterates through all dict and subdicts
        if type(v) is not dict:
            yield (joined_k, v)
        else:
            yield from zeroLevelDict(v, rename=rename, __prev_k=joined_k)


def normalizeText(text: str, lower: bool = True, strip_accents : bool = True,
                  replace_spaces: str = '_', first_word: bool = False) -> str:
    r"""Normalize text.

    Contains several ways to normalize texts e.g. remove spaces,
    strip accents and others.

    Parameters
    ----------
    text : str
        The text to normalize
    lower : bool, default = True
        Lower the text
    strip_accents : bool, default = True
        Remove all accents from the text
    replace_spaces : str, default = '_'
        Replace all non "word chars" (in regex \w) with some char.
        Any non \w chars at the end or beggining of the text will be
        removed
    first_word : bool, default = False
        Returns only the first word in the text

    Returns
    -------
    normalized_text : str
        The text with the applied normalizations
    """
    if lower:
        text = text.lower()
    if strip_accents:
        text = ''.join(c for c in unicodedata.normalize('NFD', text)
                       if unicodedata.category(c) != 'Mn')
    if first_word:
        try:
            search = re.search(r'\w+', text).span()
        except AttributeError as e:
            err_msg = 'No word was founded'
            raise Exception(err_msg) from e
        else:
            text = text[search[0]:search[1]]
    if replace_spaces:
        text = re.sub(r'\W+', replace_spaces, text).strip(replace_spaces)
    return text


def batchList(l: Iterable, batch_size: int, mode: str = 'soft') -> Generator:
    """Generate batches from a list.

    Parameters
    ----------
    l : Iterable
        List from which the batches will be taken
    batch_size : int
        Lenght of the batches. If ``mode='hard'`` it needs to be a multiple
        of ``len(l)``.
    mode : ['soft', 'hard'], default='soft'
        Mode used to create the last batch:
        -  ``soft``: The last batch will have the ``len(l) % batch_size``
           elements.
        -  ``hard``: All batches will have the same number of elements.

    Returns
    -------
    batch : Iterable
        Batches from the list one by one.

    Raises
    ------
    ValueError
        When the mode is ``hard`` and ``batch_size`` does not divide
        ``len(l)`` exactly.
    """
    # Get len of the Iterable
    len_l = len(l)

    # Verifications
    if mode not in ('soft', 'hard'):
        err_msg = 'Mode must be either soft or hard.'
        raise ValueError(err_msg)
    if mode == 'hard' and len_l % batch_size != 0:
        err_msg = 'batch_size must divide len(l) exactly in hard mode'
        raise ValueError(err_msg)

    # Ensure generator is not broken when batch_size > len_l
    if batch_size > len_l:
        batch_size = len_l

    for i in np.arange(0, len_l, batch_size):
        yield l[i:i+batch_size]


if __name__ == '__main__':
    err_msg = 'This file is only meant to be imported.'
    raise Exception(err_msg)
