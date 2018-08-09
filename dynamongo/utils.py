import copy
from schematics.undefined import Undefined


__all__ = ['is_empty', 'non_empty_values', 'merge_deep', 'is_subclass', 'key_proto']


def is_empty(value):
    """Determine if a value is empty.

    A value is considered empty if it is ``None`` or empty string ``""``
    """
    if value is None:
        return True

    if isinstance(value, str):
        return len(value) == 0

    return False


def non_empty_values(d):
    """Return a dict with empty values removed recursively"""
    clean = {}
    for k, v in d.items():
        if isinstance(v, dict):
            v = non_empty_values(v)
        if not is_empty(v):
            clean[k] = v
    return clean


def merge_deep(destination, source):
    """Merge dict objects recursively"""
    destination = copy.deepcopy(destination)
    for k, v in source.items():
        if k in destination and isinstance(v, dict):
            destination[k] = merge_deep(destination[k], v)
        else:
            destination[k] = v
    return destination


def is_subclass(value, class_):
    """Check if value is a sub class of class_"""
    try:
        return issubclass(value, class_)
    except TypeError:
        return False


def key_proto(attr):
    """Return associated DynamoDB attribute type

    :type attr: :py:class:`dynamongo.attributes.Attribute`
    """
    map_ = {str: 'S', (int, float): 'N'}
    allowed = (str, int, float)

    for primitive, proto in map_.items():
        if issubclass(attr.primitive_type, primitive):
            return proto

    raise ValueError(
        "Primary key attribute primitive_type should be one of {}".format(allowed))

