"""
.. Note::

    These classes should never be instantiated directly by the user
"""

from abc import ABC, abstractmethod
from boto3.dynamodb.conditions import Key, Attr
from .exceptions import FindOnMultipleKeysError


__all__ = [
    'OP',
    'BaseCondition',
    'PrimitiveCondition',
    'JoinCondition',
    'AndCondition',
    'OrCondition'
]


class OP(ABC):
    EQ = '=='
    NE = '!='
    LTE = '<='
    LT = '<'
    GTE = '>='
    GT = '>'

    IS_IN = 'in'
    CONTAINS = 'contains'
    BEGINS_WITH = 'begins with'
    EXISTS = 'exists'
    NOT_EXISTS = 'not exists'
    BETWEEN = 'between'

    @classmethod
    def name(cls, op):
        for key, value in cls.__dict__.items():
            if value == op:
                return key.lower()

        raise ValueError("Invalid operation '{}'".format(op))

    @classmethod
    def num_args(cls, op):
        if op in [cls.EXISTS, cls.NOT_EXISTS]:
            return 0

        if op == cls.BETWEEN:
            return 2

        return 1


def _assert_expression(other):
    assert isinstance(other, BaseCondition), "Invalid expression"


class BaseCondition(ABC):
    """Base class for all expressions"""
    def __and__(self, other):
        _assert_expression(other)
        return AndCondition(self, other)

    def __or__(self, other):
        _assert_expression(other)
        return OrCondition(self, other)

    @abstractmethod
    def to_boto_key(self):
        raise NotImplementedError

    @abstractmethod
    def to_boto_attr(self):
        raise NotImplementedError


class JoinCondition(BaseCondition, ABC):
    """Base class for joiner expressions"""
    joining_operator = ''

    def __init__(self, left, right):
        self.all = [left, right]

    def __str__(self):
        e = ['({})'.format(exp) for exp in self.all]
        op = ' {} '.format(self.joining_operator)
        return op.join(e)

    @abstractmethod
    def _join(self, a, b):
        raise NotImplementedError

    def _to_boto(self, fn):
        expression = None
        for e in self.all:
            value = getattr(e, fn)()
            if expression is None:
                expression = value
            else:
                expression = self._join(expression, value)

        return expression

    def to_boto_key(self):
        return self._to_boto('to_boto_key')

    def to_boto_attr(self):
        return self._to_boto('to_boto_attr')


class AndCondition(JoinCondition):
    """Initialized by ANDing two expressions i.e, BaseCondition & BaseCondition"""
    joining_operator = '&'

    def _join(self, a, b):
        return a & b

    def __and__(self, other):
        _assert_expression(other)
        self.all.append(other)
        return self


class OrCondition(JoinCondition):
    """Initialized by ORing two expressions i.e, BaseCondition | BaseCondition"""
    joining_operator = '|'

    def _join(self, a, b):
        return a | b

    def __or__(self, other):
        _assert_expression(other)
        self.all.append(other)
        return self


class PrimitiveCondition(BaseCondition):
    """Primitive expression"""
    def __init__(self, attr, op, value=None):
        """
        :param attr: The field attached to this expression
        :type attr: dynamongo.Attribute
        :param op: Comparison operator
        :param value: Value to compare against.
                    The value should be a tuple for expressions that expect more than one input parameter
                    Example is BETWEEN(low, high)
        """
        self.attr = attr
        self.op = op
        self.value = value

    def __str__(self):
        op = self.op
        v = self.value

        if op == OP.BETWEEN:
            return '{} <= {} <= {}'.format(v[0], self.attr, v[1])

        if OP.num_args(op) == 0:
            return '{}({})'.format(op, self.attr)

        return '{} {} {}'.format(self.attr, op, v)

    def to_boto_key(self):
        try:
            return self._to_boto(Key)
        except AttributeError as e:
            if self.op == OP.IS_IN:
                raise FindOnMultipleKeysError(e)
            raise

    def to_boto_attr(self):
        return self._to_boto(Attr)

    def _to_boto(self, class_):
        attr = self.attr
        fn = OP.name(self.op)
        fn = getattr(class_(attr.name), fn)

        args = OP.num_args(self.op)
        if args == 0:
            return fn()
        if args == 1:
            value = attr.dump(self.value)
            return fn(value)

        value = [attr.dump(x) for x in self.value]
        return fn(*value)
