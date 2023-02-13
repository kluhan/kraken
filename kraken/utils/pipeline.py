import copy

from types import NoneType
from itertools import chain


# TODO: Improve performance
# TODO: Add doc for NoneType
# Really crude approach and there must be a better way combine both object
def combine_dicts_by_addition(i: dict | NoneType, j: dict | NoneType):
    """Combines two dicts like the built-in function .update, but it does not
    replace any values, instead it combines them by calling __add__.
    Args:
        i (dict): First dict
        j (dict): Second dict

    Returns:
        dict: Combined dict
    """

    # Handle None values
    if i is None and j is None:
        return None
    elif i is None:
        return copy.copy(j)
    elif j is None:
        return copy.copy(i)

    # Initialize new dict
    combined_dict = {}
    # Iterate over all accruing keys
    for key in set(chain.from_iterable(d.keys() for d in [i, j])):

        # Combine values if key is in both dicts
        if key in i.keys() and key in j.keys():
            # Check if i and j are dicts
            if isinstance(i[key], dict) and isinstance(j[key], dict):
                # Combine by recursion
                combined_dict[key] = combine_dicts_by_addition(i[key], j[key])
            # Check if i and j are None
            elif i[key] is None and j[key] is None:
                pass
            # Check if i or j None
            elif i[key] is None or j[key] is None:
                combined_dict[key] = i[key] or j[key]
            else:
                # Combine by __add__
                combined_dict[key] = i[key] + j[key]
        # Copy value if key exits in just one dicts
        elif key in i.keys():
            combined_dict[key] = i[key]
        else:
            combined_dict[key] = j[key]
    return combined_dict
