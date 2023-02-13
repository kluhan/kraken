def increment_nested_dict(d, path="", update={}):
    """
    A function to traverse a nested dictionary and increment each field in a MongoEngine document.

    :param d: The nested dictionary to be added to the document.
    :param path: A string representing the current path within the nested dictionary.
    :param update: A dictionary representing the raw update query.
    :return: A dictionary representing the raw update query.
    """
    for k, v in d.items():
        key = mongodb_key_sanitizer(f"{path}__{k}" if path else k)

        # if v is bool and true -> increase k by 1
        if isinstance(v, bool) and v:
            update[f"inc__{key}"] = 1
        # if v is int or float -> increase k by v
        elif isinstance(v, int) or isinstance(v, float):
            update[f"inc__{key}"] = v
        # handle dict by recursion
        elif isinstance(v, dict):
            update = increment_nested_dict(v, key, update)
        # if v is none -> do nothing
        elif v is None:
            pass
        # raise TypeError if v is none of the above types
        else:
            raise TypeError(
                f"dict with value: {v} was supplied. Only dict, bool, int and float are supported!"
            )

    return update


def mongodb_key_sanitizer(s: str):
    """Function to sanitize a string to be used as a MongoDB key."""

    # Replace all . by : in s as MongoDB cant handle field names containing .
    s = s.replace(".", ":")
    # Remove all null characters in s as MongoDB cant handle field names containing null characters
    s = s.replace("\0", "")
    # If s starts with $ characters remove it as MongoDB dont allows field names staring with a $ characters
    s = s.lstrip("$")

    return s
