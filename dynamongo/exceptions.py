"""Exceptions"""
import schematics.exceptions as se


class ValidationError(se.ValidationError):
    """Exception raised when invalid data is encountered."""


class ConditionalCheckFailedException(Exception):
    """
    Raised when saving a Model instance would overwrite something
    in the database and we've forbidden that
    """


class ExpressionError(Exception):
    """raised if some expression rules are violated"""
    def __init__(self, msg, expression):
        msg = 'Invalid expression {}. {}'.format(expression, msg)
        super().__init__(msg)


class SchemaError(Exception):
    """SchemaError exception is raised when a schema consistency check fails.

    Common consistency failure includes:

        - lacks of ``__table__`` or ``__hash_key__`` definitions
        - lack of corresponding field definitions for the primary keys
        - When an invalid field type is used in :py:class:`~dynamongo.fields.DictField` \
                or :py:class:`~dynamongo.fields.ListField`
    """
    def __init__(self, msg='', name=None, value=None):
        if name is not None:
            msg = 'Invalid schema entry {}: {}. {}'.format(name, value, msg)
        super().__init__(msg)


class FindOnMultipleKeysError(AttributeError):
    """Used internally"""
