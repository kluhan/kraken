from datetime import datetime
from typing import Union

from dateutil import parser


def hacky_datetime_parser(t: Union[datetime, int, str]) -> datetime:
    """Takes a single argument :attr:`t` and tries to parse it to a :class:`datetime` object. If
    :attr:`t` is already a :class:`datetime` object, it is returned as it is. If :attr:`t`
    is an :class:`int`, it is assumed to be a unix timestamp and parsed accordingly.
    If :attr:`t` is a :class:`string`, it is parsed using the :func:`dateutil.parser.parse` function.
    Raises an exception if the :attr:`t` does not match any of the above formats..

    Parameters
    ----------
    t : Union[datetime, int, str]
        The object to be parsed to a :class:`datetime` object.

    Returns
    -------
    datetime
        The parsed :class:`datetime` object.

    Raises
    ------
    TypeError
        If the :attr:`t` does not match any of the above formats.
    """
    if t is None or isinstance(t, datetime):
        pass
    elif isinstance(t, int):
        t = datetime.fromtimestamp(t)
    elif isinstance(t, str):
        t = parser.parse(t)
    else:
        raise TypeError("Can not parse {0} of type {1} to datetime".format(t, type(t)))
    return t
