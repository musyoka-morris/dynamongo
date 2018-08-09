import voluptuous.validators as v
from voluptuous.error import Invalid
import uuid
import re
import datetime
import arrow
import collections
from .conditions import PrimitiveCondition, OP
from .updates import SetUpdate, RemoveUpdate, ListExtendUpdate, AddUpdate
from .utils import is_subclass, is_empty


__all__ = [
    'Attribute',
    'List', 'Dict', 'Boolean',
    'Number', 'Integer', 'Float',
    'String', 'Email', 'URL', 'UUID',
    'IPAddress', '_IPV4', 'IPv6',
    'DateTime', 'Date', 'Timedelta'
]


def _cast(type_, value):
    if type_ is None or isinstance(value, type_):
        return value
    return type_(value)


def _maybe_to_instance(attribute):
    if is_subclass(attribute, Attribute):
        return attribute()
    return attribute


def _assert_non_string(value):
    if isinstance(value, str):
        raise ValueError('value cannot be a string')


def _e(op):
    """
    Lightweight factory which returns a method that builds an Expression
    consisting of the left-hand and right-hand operands, using `op`.
    """
    def inner(self, rhs):
        return PrimitiveCondition(self, op, rhs)
    return inner


class Attribute:
    """
    Basic field from which other fields should extend.
    It applies no formatting by default,
    and should only be used in cases where data does not need to be serialized or deserialized.

    Supported primitive conditions are ``==``, ``!=``, ``<``, ``<=``, ``>``, and ``>=``
    """
    primitive_type = None
    python_type = None

    def __init__(self, required=False, validator=None, default=None, hash_key=False, range_key=False):
        """

        :param bool required:
        :param callable validator:
        :param default:
        """
        self._parent = None
        self._name = None
        self._default = default
        self.required = required or hash_key or range_key
        self.validator = validator

        if hash_key and range_key:
            raise ValueError("An attribute can't be both hash_key and range_key")

        self.hash_key = hash_key
        self.range_key = range_key

    def _add_validator(self, *args):
        validators = [self.validator] + list(args)
        validators = [x for x in validators if x is not None]
        size = len(validators)

        if size == 0:
            self.validator = None
        elif size == 1:
            self.validator = validators[0]
        else:
            self.validator = v.And(*validators)

    def __str__(self):
        if not is_empty(self.name):
            return self.name
        return super().__str__()

    def __repr__(self):
        if not is_empty(self.name):
            return '{}({})'.format(self.__class__.__name__, self.name)
        return super().__repr__()

    @property
    def name(self):
        name, p = self._name, self._parent
        if isinstance(p, Attribute):
            name = "{}.{}".format(p.name, name)
        return name

    def __bind__(self, name, parent):
        # The name set at class instantiation can NEVER be overwritten
        if is_empty(self._name):
            self._name = name
        self._parent = parent

    @property
    def default(self):
        """Get the default value"""
        if callable(self._default):
            return self._default()
        return self._default

    def validate(self, value):
        """Handle data validation."""
        # Handle empty values
        if is_empty(value):
            if self.required:
                raise ValueError('Value must be non null')
            return None

        # load the value first
        pt = self.python_type
        if pt and not isinstance(value, pt):
            value = self.load(value)

        # validate the value in python_type
        if self.validator:
            try:
                value = self.validator(value)
            except Invalid as e:
                raise ValueError(e.msg)

        # allow further validation in subclass
        return self._validate(value)

    def load(self, value):
        """Convert untrusted data to a richer Python construct."""
        if value is None:
            return value

        return self._load(value)

    def dump(self, value):
        """Convert internal data to a value safe to store in DynamoDB."""
        value = self.validate(value)
        if value is None:
            return value

        return self._dump(value)

    def _validate(self, value):
        return value

    def _load(self, value):
        return _cast(self.python_type, value)

    def _dump(self, value):
        return _cast(self.primitive_type, value)

    ###################
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

    ########################
    # conditions
    __lt__ = _e(OP.LT)
    __le__ = _e(OP.LTE)
    __gt__ = _e(OP.GT)
    __ge__ = _e(OP.GTE)
    __eq__ = _e(OP.EQ)
    __ne__ = _e(OP.NE)

    # in_ = _e(OP.IS_IN)
    # contains = _e(OP.CONTAINS)
    # begins_with = _e(OP.BEGINS_WITH)

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


class Number(Attribute):
    """Base class for numeric fields"""
    primitive_type = float
    python_type = float

    def __init__(self, min_value=None, max_value=None, **kwargs):
        super().__init__(**kwargs)
        self._add_validator(v.Range(min=min_value, max=max_value))

    def add(self, value):
        return AddUpdate(self, value)

    def subtract(self, value):
        return self.add(value * -1)

    def increment(self):
        return self.add(1)

    def decrement(self):
        return self.add(-1)


class Integer(Number):
    """A field that validates input as an Integer"""
    primitive_type = int
    python_type = int


class Float(Number):
    """A field that validates input as a Float"""


class Boolean(Attribute):
    """A boolean field"""
    primitive_type = bool
    python_type = bool

    def __init__(self, false=('False', 'false', '0', '', False, 0), **kwargs):
        super().__init__(**kwargs)
        self.false = false

    def _convert(self, value):
        return value not in self.false

    _load = _convert
    _dump = _convert


class _StringType(Attribute):
    primitive_type = str
    python_type = str


class String(_StringType):
    """A Unicode string field."""
    def __init__(self, min_length=None, max_length=None, regex=None, **kwargs):
        super().__init__(**kwargs)
        self._add_validator(
            v.Length(min=min_length, max=max_length),
            v.Match(regex) if regex else None
        )


class Email(_StringType):
    """A field that validates input as an E-Mail-Address"""
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self._add_validator(v.Email())


class URL(_StringType):
    """A field that validates the input as a URL."""
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self._add_validator(v.Url())


class UUID(_StringType):
    """A field that stores a valid UUID value."""
    python_type = uuid.UUID

    def __init__(self, keep_dashes=True, **kwargs):
        super().__init__(**kwargs)
        self.keep_dashes = keep_dashes

    def _dump(self, value):
        if self.keep_dashes:
            return str(value)
        return value.hex


# IP address patterns
_HEX = '0-9A-F'
_IPV4_OCTET = '( 25[0-5] | 2[0-4][0-9] | [0-1]?[0-9]{1,2} )'
_IPV4 = r'( ((%(oct)s\.){3} %(oct)s) )' % {'oct': _IPV4_OCTET}

_IPV6_H16 = '[%s]{1,4}' % _HEX
_IPV6_L32 = '(%(h16)s:%(h16)s|%(ipv4)s)' % {'h16': _IPV6_H16, 'ipv4': _IPV4}
_IPV6 = r"""(
                                    (%(h16)s:){6}%(l32)s  |
                                ::  (%(h16)s:){5}%(l32)s  |
    (               %(h16)s )?  ::  (%(h16)s:){4}%(l32)s  |
    ( (%(h16)s:){,1}%(h16)s )?  ::  (%(h16)s:){3}%(l32)s  |
    ( (%(h16)s:){,2}%(h16)s )?  ::  (%(h16)s:){2}%(l32)s  |
    ( (%(h16)s:){,3}%(h16)s )?  ::  (%(h16)s:){1}%(l32)s  |
    ( (%(h16)s:){,4}%(h16)s )?  ::               %(l32)s  |
    ( (%(h16)s:){,5}%(h16)s )?  ::               %(h16)s  |
    ( (%(h16)s:){,6}%(h16)s )?  :: )""" % {'h16': _IPV6_H16,
                                           'l32': _IPV6_L32}


def _compile(*args):
    regex = "|".join(args)
    return re.compile('^{}$'.format(regex), re.I + re.X)


class IPAddress(_StringType):
    """A field that stores a valid IPv4 or IPv6 address."""
    VERSION = None
    REGEX = _compile(_IPV4, _IPV6)

    def _validate(self, value):
        if not self.REGEX.match(value):
            raise ValueError('Invalid IP{} address'.format(self.VERSION or ''))


class IPv4(IPAddress):
    """A field that stores a valid IPv4 address."""
    VERSION = 'v4'
    REGEX = _compile(_IPV4)


class IPv6(IPAddress):
    """A field that stores a valid IPv6 address."""
    VERSION = 'v6'
    REGEX = _compile(_IPV6)


class _CompoundAttribute(Attribute):
    @property
    def default(self):
        value = super().default
        return self.python_type() if value is None else value


class List(_CompoundAttribute):
    """
    A field for storing a list of items,
    all of which must conform to the type specified by the ``field`` parameter.

    Note: This field cannot be set to ``None``
    """
    primitive_type = list
    python_type = list

    def __init__(self, attribute, min_size=None, max_size=None, **kwargs):
        super().__init__(**kwargs)
        self._add_validator(v.Length(min=min_size, max=max_size))

        attribute = _maybe_to_instance(attribute)
        if not isinstance(attribute, Attribute):
            raise ValueError('attribute must be an instance of :class:`Attribute`')

        self.attribute = attribute

    def _validate(self, value):
        _assert_non_string(value)
        return [self.attribute.validate(x) for x in value]

    def _load(self, value):
        _assert_non_string(value)
        return [self.attribute.load(x) for x in value]

    def _dump(self, value):
        _assert_non_string(value)
        return [self.attribute.dump(x) for x in value]

    def append(self, *values):
        """Append one or more values at the end of the list"""
        return ListExtendUpdate(self, values, append=True)

    def prepend(self, *values):
        """Prepend one or more values at the start of the list"""
        return ListExtendUpdate(self, values, append=False)


class Dict(_CompoundAttribute):
    """
    A field that stores dict values.

    Accepts named parameters which must be instances of :py:class:`Field`
    """
    primitive_type = dict
    python_type = dict

    EXTRA_ALLOW = 1
    EXTRA_DROP = 2
    EXTRA_RAISE = 3

    def __init__(self, attributes, extra=EXTRA_ALLOW, **kwargs):
        super().__init__(**kwargs)
        self.extra = extra

        attributes = _maybe_to_instance(attributes)
        if not isinstance(attributes, (dict, Attribute)):
            raise ValueError('attributes must be an instance of :class:`Attribute` or `dict`')

        if isinstance(attributes, dict):
            for attr in attributes.values():
                if not isinstance(attr, Attribute):
                    raise ValueError('all attributes values must be an instance of :class:`Attribute`')

        self.attributes = attributes

    def __bind__(self, name, parent):
        super().__bind__(name, parent)
        if isinstance(self.attributes, dict):
            for n, attr in self.attributes.items():
                attr.__bind__(n, self)

    def _run(self, value, method):
        if isinstance(self.attributes, Attribute):
            func = getattr(self.attributes, method)
            return {k: func(x) for k, x in value.items()}

        v_keys, m_keys = value.keys(), self.attributes.keys()
        diff = set(v_keys) - set(m_keys)

        if self.extra == self.EXTRA_RAISE and len(diff):
            raise ValueError('extra keys not allowed: {}'.format(diff))

        attributes = self.attributes
        if self.extra == self.EXTRA_ALLOW:
            attributes = {**attributes, **{k: Attribute() for k in diff}}

        return {k: getattr(attr, method)(value.get(k)) for k, attr in attributes.items()}

    def _validate(self, value):
        return self._run(value, 'validate')

    def _load(self, value):
        return self._run(value, 'load')

    def _dump(self, value):
        return self._run(value, 'dump')


# date & time attributes
class DateTime(Attribute):
    """A field that holds a combined date and time value."""
    primitive_type = str
    python_type = datetime.datetime

    def __init__(self, timezone=None, formats=None, **kwargs):
        super().__init__(**kwargs)
        self.timezone = timezone

        if formats is None:
            formats = []
        elif isinstance(formats, str):
            formats = [formats]
        elif isinstance(formats, collections.Sequence):
            formats = list(formats)
        else:
            formats = [formats]

        self.formats = formats

    def _parse(self, value):
        if len(self.formats):
            try:
                value = float(value)
            except ValueError:
                value = arrow.get(value, self.formats)
            except TypeError:
                pass

        dt = arrow.get(value)
        if self.timezone:
            dt = dt.to(self.timezone)
        return dt

    def _load(self, value):
        return self._parse(value).datetime


class Date(DateTime):
    """A field that stores and validates date values."""
    python_type = datetime.date

    def _load(self, value):
        return self._parse(value).date()


class Timedelta(Attribute):
    """A field that stores and validates timedelta value"""
    primitive_type = float
    python_type = datetime.timedelta

    WEEKS = 'weeks'
    DAYS = 'days'
    HOURS = 'hours'
    MINUTES = 'minutes'
    SECONDS = 'seconds'
    MILLISECONDS = 'milliseconds'
    MICROSECONDS = 'microseconds'
    UNITS = [WEEKS, DAYS, HOURS, MINUTES, SECONDS, MILLISECONDS, MICROSECONDS]

    def __init__(self, precision='seconds', **kwargs):
        precision = precision.lower()
        if precision not in self.UNITS:
            raise ValueError("Timedelta.__init__() got an invalid value for parameter 'precision'")
        self.precision = precision
        super().__init__(**kwargs)

    def _load(self, value):
        if isinstance(value, datetime.timedelta):
            return value

        return datetime.timedelta(**{self.precision: float(value)})

    def _dump(self, value):
        base_unit = datetime.timedelta(**{self.precision: 1})
        return int(value.total_seconds() / base_unit.total_seconds())
