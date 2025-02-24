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


def namespace_to_dict(namespace):
    if type(namespace) is dict:
        res = {}
        for k, v in namespace.items():
            res[k] = namespace_to_dict(v)
        return res
    elif type(namespace) is list:
        return [namespace_to_dict(item) for item in namespace]
    elif type(namespace) is SimpleNamespace:
        return namespace_to_dict(vars(namespace))
    else:
        return namespace