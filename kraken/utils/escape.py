import html

from types import NoneType
from typing import Union


def escape(s: Union[str, NoneType]) -> str:
    """Replaces each ASCII NULL character(\x00) in :attr:`s` with the ASCII
    Replacement Character(\uFFFD) and applies :func:`html.escape` to :attr:`s`.
    If :attr:`s` is of type :class:`NoneType`, it is returned as it is.

    Parameters
    ----------
    s : str
        The string to escape

    Returns
    -------
    str
        The escaped string

    Raises
    ------
    TypeError
        If :attr:`s` is not a :class:`str` or :class:`NoneType`
    """

    if isinstance(s, str):
        return html.escape(s.replace("\x00", "\uFFFD"))
    elif s is None:
        return s
    else:
        raise TypeError("Can not escape {0} of type {1}".format(s, type(s)))
