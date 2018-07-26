"""Field classes for various types of data."""

import string
import schematics.types as types
from schematics.undefined import Undefined
from .utils import merge_deep, is_empty
from .exceptions import SchemaError, ValidationError
from .conditions import PrimitiveCondition, OP
from .updates import SetUpdate, RemoveUpdate, ListExtendUpdate, AddUpdate


__all__ = [
    'Field',

    'IntField',
    'FloatField',
    'BooleanField',

    'StringField',
    'UUIDField',
    'EmailField',
    'URLField',
    'IPAddressField',

    'DateTimeField',
    'DateField',
    'TimedeltaField',

    'ListField',
    'DictField'
]


def _e(op):
    """
    Lightweight factory which returns a method that builds an Expression
    consisting of the left-hand and right-hand operands, using `op`.
    """
    def inner(self, rhs):
        return PrimitiveCondition(self, op, rhs)
    return inner


class Field:
    """
    Basic field from which other fields should extend.
    It applies no formatting by default,
    and should only be used in cases where data does not need to be serialized or deserialized.

    Supported primitive conditions are ``==``, ``!=``, ``<``, ``<=``, ``>``, and ``>=``
    """
    native_type = None
    primitive_type = None

    name = None
    required = False

    def set_name(self, name, parent=None):
        """
        Set name

        schema names should start with a alphabetic character
        """

        # todo: This condition is not enforced by dynamoDb, and thus can be removed if need be.
        if name[:1] not in string.ascii_letters:
            raise SchemaError("All schema attribute names must start with an alphabetic character", name, self)

        if parent:
            name = "{}.{}".format(parent.name, name)

        self.name = name

    def _process_empty_value(self, value):
        if is_empty(value):
            if self.required:
                raise ValidationError(
                    "A required value for key `{}` is missing. Field {}".format(self.name, self))
            return None
        return value

    def __str__(self):
        if self.name is not None:
            return self.name
        return super().__str__()

    __lt__ = _e(OP.LT)
    __le__ = _e(OP.LTE)
    __gt__ = _e(OP.GT)
    __ge__ = _e(OP.GTE)
    __eq__ = _e(OP.EQ)
    __ne__ = _e(OP.NE)

    def in_(self, value):
        """
        Creates a condition where the attribute is in the value,

        :param list value: The list of values that the attribute is in.
        """
        return PrimitiveCondition(self, OP.IS_IN, value)

    def contains(self, value):
        """
        Creates a condition where the attribute contains the value.

        :param value: The value the attribute contains.
        """
        return PrimitiveCondition(self, OP.CONTAINS, value)

    def begins_with(self, value):
        """
        Creates a condition where the attribute begins with the value.

        :param value: The value that the attribute begins with.
        """
        return PrimitiveCondition(self, OP.BEGINS_WITH, value)

    def exists(self):
        """Creates a condition where the attribute exists."""
        return PrimitiveCondition(self, OP.EXISTS)

    def not_exists(self):
        """Creates a condition where the attribute does not exist."""
        return PrimitiveCondition(self, OP.NOT_EXISTS)

    def between(self, low, high):
        """
        Creates a condition where the attribute is greater than or equal to the low value
        and less than or equal to the high value.

        :param low: The value that the attribute is greater than or equal to.
        :param high: The value that the attribute is less than or equal to.
        """
        return PrimitiveCondition(self, OP.BETWEEN, (low, high))

    # updates
    def set(self, value):
        """Set field to the given value if it does not exist otherwise update"""
        return SetUpdate(self, value)

    def set_if_not_exists(self, value):
        """Set field to the given value if it does not exist otherwise do nothing"""
        return SetUpdate(self, value, if_not_exists=True)

    def remove(self):
        """Remove field"""
        return RemoveUpdate(self)

    # abstract methods
    @property
    def default(self):
        """Get the default value"""
        raise NotImplementedError

    def to_primitive(self, value, context=None):
        """Convert internal data to a value safe to store in DynamoDB."""
        raise NotImplementedError

    def to_native(self, value, context=None):
        """Convert untrusted data to a richer Python construct."""
        raise NotImplementedError


class SchematicFixer:
    """Mixin to override Schematic type"""
    @property
    def default(self):
        value = super().default
        # schematics uses `Undefined` to represent None values
        if value == Undefined:
            return None
        return value

    def to_primitive(self, value, context=None):
        """Validate & Convert internal data to a value ready for DynamoDB."""
        value = self._process_empty_value(value)
        if value is None:
            return None

        value = super().validate(value)
        return super().to_primitive(value)

    def to_native(self, value, context=None):
        """Convert untrusted data to a richer Python construct."""
        if value is None:
            return value
        return super().to_native(value)


class NumericField(Field):
    """Base class for numeric fields"""
    def add(self, value):
        return AddUpdate(self, value)

    def subtract(self, value):
        return self.add(value * -1)

    def increment(self):
        return self.add(1)

    def decrement(self):
        return self.add(-1)


class IntField(SchematicFixer, types.IntType, NumericField):
    """
    A field that validates input as an Integer

    See `Schematics IntType <https://schematics.readthedocs.io/en/latest/api/types.html#schematics.types.base.IntType>`_
    """


class FloatField(SchematicFixer, types.FloatType, NumericField):
    """
    A field that validates input as a Float

    See `Schematics FloatType <https://schematics.readthedocs.io/en/latest/api/types.html#schematics.types.base.FloatType>`_
    """


class BooleanField(SchematicFixer, types.BooleanType, Field):
    """
    A boolean field

    See `Schematics BooleanType <https://schematics.readthedocs.io/en/latest/api/types.html#schematics.types.base.BooleanType>`_
    """


class StringField(SchematicFixer, types.StringType, Field):
    """
    A Unicode string field.

    See `Schematics StringType <https://schematics.readthedocs.io/en/latest/api/types.html#schematics.types.base.StringType>`_
    """


class EmailField(SchematicFixer, types.EmailType, Field):
    """
    A field that validates input as an E-Mail-Address

    See `Schematics EmailType <https://schematics.readthedocs.io/en/latest/api/types.html#schematics.types.net.EmailType>`_
    """


class URLField(SchematicFixer, types.URLType, Field):
    """
    A field that validates the input as a URL.

    See `Schematics URLType <https://schematics.readthedocs.io/en/latest/api/types.html#schematics.types.net.URLType>`_
    """


class UUIDField(SchematicFixer, types.UUIDType, Field):
    """
    A field that stores a valid UUID value.

    See `Schematics UUIDType <https://schematics.readthedocs.io/en/latest/api/types.html#schematics.types.base.UUIDType>`_
    """


class IPAddressField(SchematicFixer, types.IPAddressType, Field):
    """
    A field that stores a valid IPv4 or IPv6 address.

    See `Schematics IPAddressType <https://schematics.readthedocs.io/en/latest/api/types.html#schematics.types.net.IPAddressType>`_
    """


class DateTimeField(SchematicFixer, types.DateTimeType, Field):
    """
    A field that holds a combined date and time value.

    See `Schematics DateTimeType <https://schematics.readthedocs.io/en/latest/api/types.html#schematics.types.base.DateTimeType>`_
    """


class DateField(SchematicFixer, types.DateType, Field):
    """
    A field that stores and validates date values.

    See `Schematics DateType <https://schematics.readthedocs.io/en/latest/api/types.html#schematics.types.base.DateType>`_
    """


class TimedeltaField(SchematicFixer, types.TimedeltaType, Field):
    """
    A field that stores and validates timedelta value

    See `Schematics TimedeltaType <https://schematics.readthedocs.io/en/latest/api/types.html#schematics.types.base.TimedeltaType>`_
    """


class ListField(SchematicFixer, types.ListType, Field):
    """
    A field for storing a list of items,
    all of which must conform to the type specified by the ``field`` parameter.

    See `Schematics ListType <https://schematics.readthedocs.io/en/latest/api/types.html#schematics.types.compound.ListType>`_

    Note: This field cannot be set to ``None``
    """

    def __init__(self, field, default=list(), **kwargs):
        kwargs['default'] = default

        # This field can never be None. An empty list is allowed
        # Reason: append & prepend ops expect a list
        kwargs['required'] = True

        super().__init__(field, **kwargs)

    def append(self, *values):
        """Append one or more values at the end of the list"""
        return ListExtendUpdate(self, values, append=True)

    def prepend(self, *values):
        """Prepend one or more values at the start of the list"""
        return ListExtendUpdate(self, values, append=False)


class DictField(Field):
    """
    A field that stores dict values.

    Accepts named parameters which must be instances of :py:class:`Field`
    """
    primitive_type = dict
    native_type = dict

    def __init__(self, **fields):
        for key, value in fields.items():
            if not isinstance(value, Field):
                raise ValueError('All entries must be instances of :class: `Field`')

        self.fields = fields
        self.required = True  # A value must always be set. {} is valid

    def set_name(self, name, parent=None):
        super().set_name(name, parent)
        for key, field in self.fields.items():
            field.set_name(key, self)

    def _generate(self, func, value):
        if value is None:
            value = dict()

        generated = {}
        for key, field in self.fields.items():
            generated[key] = getattr(field, func)(value.get(key, None))

        return merge_deep(value, generated)

    @property
    def default(self):
        return {key: field.default for key, field in self.fields.items()}

    def to_native(self, value, context=None):
        return self._generate('to_native', value)

    def to_primitive(self, value, context=None):
        value = self._generate('to_primitive', value)

        clean = {}
        for k, v in value.items():
            if not is_empty(v):
                clean[k] = v

        return self._process_empty_value(clean)
