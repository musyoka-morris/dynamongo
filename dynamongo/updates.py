"""
.. Note::

    These classes should never be instantiated directly by the user
"""

from abc import ABC
from collections import OrderedDict
from .utils import is_empty

__all__ = ['UpdateBuilder', 'Update', 'SetUpdate', 'RemoveUpdate', 'AddUpdate', 'ListExtendUpdate']


class UpdateBuilder:
    """Update expression builder"""
    SET = 'SET'
    REMOVE = 'REMOVE'
    ADD = 'ADD'

    def __init__(self, type_, expression, values):
        self.type_ = type_
        self.expression = expression
        self.values = values

    @classmethod
    def create(cls, updates):
        """Prepares update-expression & expression-attribute-values

        :param updates: list of updates to be performed
        :type updates: list[Update]|tuple(Update)
        :return: a tuple (update-expression, expression-attribute-values)
        """
        values = {}
        expressions = OrderedDict([
            (cls.SET, []),
            (cls.ADD, []),
            (cls.REMOVE, [])
        ])

        builders = [update.create() for update in updates]
        for builder in builders:
            if builder is not None:
                expressions[builder.type_].append(builder.expression)
                values.update(builder.values)

        e = []
        for action, options in expressions.items():
            if len(options) > 0:
                e.append('{} {}'.format(action, ', '.join(options)))

        return ' '.join(e), values


class Update(ABC):
    """Base abstract class for update expressions"""
    _index = 1

    def __init__(self, field, value=None):
        self.field = field
        self._value = value

    def create(self):
        raise NotImplementedError

    @property
    def value(self):
        """validated value"""
        field = self.field
        value = self._value

        #########################################
        # DISABLE NESTED ATTRIBUTES
        # TODO: update the model class to be able to handle this and remove the clause
        if '.' in field.name:
            raise NotImplementedError('Support for nested attributes update not implemented yet')

        return field.to_primitive(value)

    def _assert_top_level(self, text):
        name = self.field.name
        if '.' in name:
            raise Exception(
                '{} is only supported for top level attributes only. '
                'Operation can not be performed on nested attribute {}'.format(text, name))

    @staticmethod
    def placeholder():
        """Generate a unique placeholder string"""
        Update._index += 1
        return ':val{}'.format(Update._index)


class RemoveUpdate(Update):
    """Update to remove attributes from the db"""
    def __init__(self, field):
        super().__init__(field, value=None)

    def create(self):
        _ = self.value  # Ensure we are not removing a required field
        return UpdateBuilder(UpdateBuilder.REMOVE, self.field.name, {})


class SetUpdate(Update):
    """Update to set an attribute to the given value"""
    def __init__(self, field, value, if_not_exists=False):
        super().__init__(field, value)
        self.if_not_exists = if_not_exists

    def create(self):
        field = self.field
        value = self.value

        # handle empty values
        if is_empty(value):
            if self.if_not_exists:
                return None
            return RemoveUpdate(field).create()

        ph = self.placeholder()
        rhs = ph
        if self.if_not_exists:
            rhs = 'if_not_exists({}, {})'.format(field.name, ph)

        return UpdateBuilder(UpdateBuilder.SET, '{} = {}'.format(field.name, rhs), {ph: value})


class ListExtendUpdate(Update):
    """Update to append or prepend values to a list"""
    def __init__(self, field, value, append=True):
        super().__init__(field, list(value))
        self.append = append

    def create(self):
        value = self.value

        # handle empty values. we have nothing to append|prepend
        if is_empty(value):
            return None

        name = self.field.name
        ph = self.placeholder()
        template = '{} = list_append({}, {})'
        params = [name, name, ph] if self.append else [name, ph, name]

        return UpdateBuilder(UpdateBuilder.SET, template.format(*params), {ph: value})


class AddUpdate(Update):
    """Update to perform an addition to a numeric value"""
    def create(self):
        self._assert_top_level('numeric addition')

        value = self.value
        if is_empty(value):
            return None

        ph = self.placeholder()
        return UpdateBuilder(UpdateBuilder.ADD, '{} {}'.format(self.field.name, ph), {ph: value})
