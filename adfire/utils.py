from types import SimpleNamespace


def dict_to_namespace(obj):
    """
    Recursively convert a dictionary (and any nested dictionaries/lists)
    to a SimpleNamespace.
    """
    if isinstance(obj, dict):
        return SimpleNamespace(**{k: dict_to_namespace(v) for k, v in obj.items()})
    elif isinstance(obj, list):
        return [dict_to_namespace(item) for item in obj]
    else:
        return obj