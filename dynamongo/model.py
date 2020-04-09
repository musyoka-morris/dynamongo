import logging
import copy
from decimal import Decimal
from botocore.exceptions import ClientError
from inflection import tableize

from .utils import is_empty, merge_deep, non_empty_values
from .connection import Connection
from .fields import Field
from .exceptions import *
from .conditions import *
from .updates import Update, UpdateBuilder, SetUpdate
from .utils import is_subclass

from .attributes import Dict, Attribute

log = logging.getLogger(__name__)

__all__ = ['Model', 'BatchResult']


# Ensures equality expression
STRICT_HASH = 1
STRICT_RANGE = 2
STRICT_BOTH = 3


class _HasAttributes:
    @classmethod
    def get_attributes(cls):
        attributes = {}
        for key, attribute in cls.__dict__.items():
            if isinstance(attribute, Attribute):
                attributes[key] = attribute
            elif is_subclass(attribute, DictAttribute):
                attributes[key] = attribute.create()
        return attributes


class DictAttribute(_HasAttributes):
    __extra__ = Dict.EXTRA_ALLOW

    def __init__(self):
        raise RuntimeError('{} class can not be instantiated'.format(self.__name__))

    @classmethod
    def create(cls):
        return Dict(cls.get_attributes(), extra=cls.__extra__)


class Model(_HasAttributes):
    """
    Base model class with which to define custom models.

    Example usage:

    .. code-block:: python

        from dynamongo import Model
        from dynamongo import IntField, StringField, EmailField

        class User(Model):
            __table__ = 'users'
            __hash_key__ = 'email'

            # fields
            email = EmailField(required=True)
            name = StringField(required=True)
            year_of_birth = IntField(max_value=2018, min_value=1900)

    Each custom model can declare the following class meta data variables:

    **__table__** `(required)`

    The name of table to be associated with this model.
    This is usually prefixed with the table prefix as set in :py:class:`~dynamongo.connection.Connection`.
    i.e, in dynamodb, the table name will appear as ``<table_prefix><table_name>``


    **__hash_key__** `(required)`

    The name of the field to be used as the Hash key for the table.
    **NOTE**: A field for the hash key **MUST** be declared
    and it must be of primitive type ``str|numeric``


    **__range_key__** `(optional)`

    The name of the field to be used as the Range key for the table.
    **NOTE**: This is Optional. However, if declared, a corresponding field **MUST** be declared
    and it must be of primitive type ``str|numeric``


    **__read_units__** `(optional)`

    The number of read units to provision for this table (default ``8``)


    **__write_units__** `(optional)`

    The number of write units to provision for this table (default ``8``)

    See `Amazon’s developer guide <https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/HowItWorks.ProvisionedThroughput.html>`_
    for more information about provisioned throughput Capacity for Reads and Writes
    """

    __table__ = None
    __hash_key__ = None  # required
    __range_key__ = None  # optional

    # The number of read & write units to provision for this table (minimum 5)
    __read_units__ = 8
    __write_units__ = 8

    @classmethod
    def keys_in(cls, values):
        """Convenient method to generate :py:class:`~dynamongo.conditions.CompoundKeyCondition`

        This is useful when working with a model that has a composite primary key
        i.e, both ``hash_key`` and ``range_key``


        Example usage:

        .. code-block:: python

            import datetime
            from dynamongo import Model
            from dynamongo import EmailField, UUIDField, DateTimeField


            class Contacts(Model):
                __table__ = 'user-contacts'
                __hash_key__ = 'user_id'
                __range_key__ = 'email'

                # fields
                user_id = UUIDField(required=True)
                email = EmailField(required=True)
                created_at = DateTimeField(default=datetime.datetime.now)


            # select multiple contacts for different users when you have a
            # list of (user_id, email) tuples
            keys = [('user_id_1', 'john@gmail.com'), ('user_id_2', 'doe@gmail.com')]
            contacts = Contacts.get_many(
                Contacts.keys_in(keys)
            )
        """
        return CompoundKeyCondition(values)

    @classmethod
    def _schema(cls):
        """Return the schema."""
        if getattr(cls, '__schema__', None) is None:
            cls.__schema__ = {}
            for key, field in cls.__dict__.items():
                if isinstance(field, Field):
                    field.set_name(key)
                    cls.__schema__[key] = field
        return cls.__schema__

    @classmethod
    def _primary_keys(cls):
        # Return the primary keys: [hash_key, range_key]
        if not hasattr(cls, '__primary_keys'):
            if cls.__range_key__:
                cls.__primary_keys = [cls.__hash_key__, cls.__range_key__]
            else:
                cls.__primary_keys = [cls.__hash_key__]
        return cls.__primary_keys

    @classmethod
    def _is_key_expression(cls, e):
        """Determine if an expression represents a key attribute."""
        return isinstance(e, PrimitiveCondition) and e.attr.name in cls._primary_keys()

    @classmethod
    def _extract_key_conditions(cls, expression, strict_level=None, all_required=False):
        """ Extract all conditions associated with a primary key

        For a table with composite primary key, the hash and range key conditions
        can only be joined using a ``&`` operator.

        :param expression: An instance of :py:class:`~dynamongo.BaseCondition`

        :param int strict_level: Takes one of ``STRICT_BOTH | STRICT_HASH | STRICT_RANGE``.

            - ``STRICT_BOTH``: All primary key conditions must strictly be ``==`` (equality) checks
            - ``STRICT_HASH``: Conditions on hash key must strictly be ``==`` (equality) checks
            - ``STRICT_RANGE``: Conditions on range key must strictly be ``==`` (equality) checks

        :param boolean all_required: if all required, then there must be a condition for each primary key.
            Meaning that if the table is using a composite primary key,
            conditions on both the hash and range keys need be specified.

        :return: A list of primary key attribute conditions
        :rtype: list of :py:class:`~dynamongo.conditions.PrimitiveCondition`
        """
        if expression is None:
            return None

        expressions = []

        if isinstance(expression, PrimitiveCondition):
            expressions.append(expression)

        if isinstance(expression, AndCondition):
            # primary key attributes can only be bundled using & operator
            expressions = expression.all

        # filter expressions
        expressions = [e for e in expressions if cls._is_key_expression(e)]
        size = len(expressions)
        hash_key = cls.__hash_key__
        range_key = cls.__range_key__

        def _equality_or_raise(e_):
            """Check if the expression is an equality expression"""
            values = dict(
                hash=(hash_key, STRICT_HASH),
                range=(range_key, STRICT_RANGE)
            )

            for text, value in values.items():
                key, level = value
                if strict_level in [STRICT_BOTH, level] and e_.field.op != OP.EQ and e_.field.name == key:
                    raise ExpressionError(
                        'An equality expression was required for the {} key "{}"'.format(text, key),
                        expression
                    )

        # Ensure keys are not repeated
        field_names = [e.attr.name for e in expressions]
        if len(set(field_names)) != size:
            raise ExpressionError("Cannot repeat keys in the same expression", expression)

        # strict equality checks
        for e in expressions:
            _equality_or_raise(e)

        # Ensure all keys are provided
        if all_required and len(cls._primary_keys()) != size:
            msg = 'condition on hash key attribute must be specified'
            if range_key:
                msg = 'Conditions on both hash and range key must be specified joined with & operator'

            raise ExpressionError(msg, expression)

        # We do not have any expressions for the primary key attributes
        if size == 0:
            return None

        # hash_key only
        if size == 1:
            e = expressions[0]

            # range_key cant be used without hash_key
            if e.attr.name == range_key:
                raise ExpressionError(
                    "range_key ({}) expression can't be used without hash_key ({}) expression"
                        .format(range_key, hash_key),
                    expression
                )

        return expressions

    @classmethod
    def _key_map(cls, expression):
        """ convert the expression to a key map

        Note that this method is supposed to generate keys to fetch items  matching an exact primary key.
        Thus, equality ``==`` is the only operators allowed for the primary key attribute conditions.

        Additionally, if the table is using a composite primary key,
        equality conditions on both the hash and range key attributes must be specified and
        joined using a ``&`` operator.

        :param expression: An instance of :py:class:`~dynamongo.conditions.BaseCondition`
        :return: a key map dict
        """
        conditions = cls._extract_key_conditions(expression, strict_level=STRICT_BOTH, all_required=True)

        # Convert to DynamoDB values.
        return {e.attr.name: e.attr.to_primitive(e.value) for e in conditions}

    @classmethod
    def _extract_key_cond(cls, expression, strict_level=None, all_required=False):
        """convert the expression to a Key condition. Usually used in Query & Scan operations

        Check :py:meth:`~Model._extract_key_conditions` for more info
        """
        conditions = cls._extract_key_conditions(expression, strict_level, all_required)

        if conditions is None:
            return None
        if len(conditions) == 1:
            return conditions[0].to_boto_key()
        return AndCondition(*conditions).to_boto_key()

    @classmethod
    def _extract_attr_cond(cls, expression):
        """ convert the expression to an attr condition. Usually used in Query & Scan operations"""
        if expression is None:
            return None

        if isinstance(expression, PrimitiveCondition):
            return None if cls._is_key_expression(expression) else expression.to_boto_attr()

        if isinstance(expression, OrCondition):
            return expression.to_boto_attr()

        if isinstance(expression, AndCondition):
            # omit primary key expressions
            expressions = [e for e in expression.all if not cls._is_key_expression(e)]
            if len(expressions) > 0:
                and_exp = expressions[0]
                for e in expressions[1:]:
                    and_exp &= e
                return and_exp.to_boto_attr()

        # this should never be reached
        raise ExpressionError("", expression)

    @classmethod
    def table_name(cls):
        """Get prefixed table name"""
        return "{}{}".format(Connection.table_prefix(), cls.__table__)

    @classmethod
    def table(cls):
        """Get a dynamoDB Table instance for this model"""
        return Connection.get_table(cls.table_name())

    @classmethod
    def create_table(cls):
        """
        Create a table that'll be used to store instances of cls in AWS dynamoDB.

        This operation should be called before any table read or write operation is undertaken
        """
        schema = cls._schema()

        def _dynamo_db_proto(field):
            """Return associated DynamoDB attribute type"""
            map_ = {str: 'S', (int, float, Decimal): 'N'}
            for k, v in map_.items():
                if issubclass(field.primitive_type, k):
                    return v
            raise SchemaError('Invalid key type: {}'.format(field))

        # prepare key schema & attribute definitions
        keys, attributes = [], []
        for key, type_ in zip(cls._primary_keys(), ['HASH', 'RANGE']):
            proto = _dynamo_db_proto(schema[key])
            keys.append({
                'AttributeName': key,
                'KeyType': type_
            })
            attributes.append({
                'AttributeName': key,
                'AttributeType': proto
            })

        client = Connection().client()
        table_name = cls.table_name()

        # Create table
        client.create_table(
            TableName=table_name,
            KeySchema=keys,
            AttributeDefinitions=attributes,
            ProvisionedThroughput={
                'ReadCapacityUnits': cls.__read_units__,
                'WriteCapacityUnits': cls.__write_units__
            }
        )

        # Wait until the table exists. this can take up to a minute.
        client.get_waiter('table_exists').wait(TableName=table_name)
        log.debug("Created table %s(%s, %s)", cls.table_name(), cls.__hash_key__, cls.__range_key__)

    @classmethod
    def _parse_one_strategy(cls, strategy, allow_condition=True):
        """Parse strategy to extract KeyExpression and

        Returns a tuple (keys, condition):

        ``keys`` is a map of attribute names to attribute values,
        representing the primary key.
        For the primary key, all of the attributes must be provided.
        For example, with a simple primary key,
        only a value for the hash key need be provided.
        For a composite primary key, values for both the hash key and the sort key must be provided.

        ``condition`` is ConditionExpression. The condition(s) an attribute(s) must meet.
        Valid conditions are listed in the `DynamoDB Reference Guide <https://boto3.readthedocs.io/en/latest/reference/customizations/dynamodb.html#ref-dynamodb-conditions>`_.

        :param strategy: strategy can be either of the following:

                - ``dict``: must contain the primary key attributes.
                - ``scalar``: Only valid if the Model does not define a ``range_key``
                - ``tuple``: This is a (``hash_key_value``, ``range_key_value``) tuple. Only valid if the model has a composite primary key
                - instance of ``cls``
                - :py:class:`~dynamongo.conditions.BaseCondition`

        :param bool allow_condition: Whether or not to allow a strategy that has conditions in it.

            When ``allow_condition = False``:

                - raise :py:exc:`~dynamongo.exceptions.ValidationError` if the strategy has conditions
                - returns ``keys`` only

        :return: a (keys, condition) tuple or keys only
        """
        keys, condition = None, None

        if isinstance(strategy, BaseCondition):
            # delete by key and condition
            keys = cls._key_map(strategy)
            condition = cls._extract_attr_cond(strategy)

        elif not isinstance(strategy, cls):
            hash_key = cls.__hash_key__
            range_key = cls.__range_key__

            if isinstance(strategy, dict):
                strategy = cls(**strategy)

            elif range_key and isinstance(strategy, tuple) and len(strategy) == 2:
                strategy = cls(**{
                    hash_key: strategy[0],
                    range_key: strategy[1]
                })

            elif not range_key:
                # assume the item was the hash key.
                # This raises ValidationError if the value cannot be coerced to the hash_key data type
                strategy = cls(**{
                    hash_key: strategy
                })

        if isinstance(strategy, cls):
            keys = strategy._clean_data(cls._primary_keys())

        if keys is None:
            raise ValidationError(
                'Invalid item {}. Each item must be a dict or instance of {}'.format(strategy, cls))

        if not allow_condition:
            if condition:
                raise ValidationError('Strategy with condition not allowed {}'.format(strategy))
            return keys

        return keys, condition

    @classmethod
    def get_one(cls, strategy, consistentRead=True):
        """Retrieve a single item from DynamoDB according to strategy.

        See :ref:`doc_get_one`

        :return: Instance of ``cls`` - The fetched item
        """
        cls._schema()  # init schema

        keys = cls._parse_one_strategy(strategy, False)
        response = cls.table().get_item(Key=keys, ConsistentRead=consistentRead)

        if not response or 'Item' not in response:
            # 'Item not found with keys: {}'.format(keys)
            return None

        return cls._from_db_dict(response["Item"])

    @classmethod
    def get_many(cls, strategy, descending=False, limit=None, consistentRead=True):
        """Retrieve a multiple items from DynamoDB according to strategy.

        Performs either a BatchGet, Query, or Scan depending on strategy

        See :ref:`doc_get_many`

        :param strategy: See :ref:`doc_get_many`
        :param bool descending: Sort order. Items are sorted by the hash key. Items with the same hash key value are sorted by range key
        :param int limit: The maximum number of items to get (not necessarily the number of items returned)
        :return: list of ``cls``
        """
        cls._schema()  # init

        items, expression = [], None
        kc = fc = None

        if isinstance(strategy, CompoundKeyCondition):
            items = strategy.values

        elif isinstance(strategy, list):
            # This can be a list of:
            #   1. dict (primary keys)
            #   2. scalar (hash key)
            #   3. tuple (hash, range) keys
            # only
            items = strategy

        else:
            expression = strategy

        if expression:
            try:
                # todo: handle index tables
                kc = cls._extract_key_cond(expression)
                fc = cls._extract_attr_cond(expression)
            except FindOnMultipleKeysError:
                # We tried hash_key.in_(list)
                #
                # If not range_key & op is EQ for hash_key: GET
                # If not range_key & op is IN for hash_key: BATCH
                # If range_key & op is EQ for hash_key: QUERY
                # Otherwise use SCAN
                hash_key = cls.__hash_key__
                range_key = cls.__range_key__
                if isinstance(expression, PrimitiveCondition) and expression.attr.name == hash_key and not range_key:
                    # This qualifies for a batch request. BATCH
                    items = expression.value
                elif isinstance(expression, BaseCondition):
                    # Force a scan
                    fc = expression.to_boto_attr()
                else:
                    raise

        total_found = 0

        if len(items):
            # do batch here
            keys = [cls._parse_one_strategy(item, False) for item in items]
            resource = Connection.resource()
            table_name = cls.table_name()
            retry_while_no_items = True
            force_break = False

            while len(keys):
                response = resource.batch_get_item(
                    RequestItems={
                        table_name: {
                            'Keys': keys
                        },
                        'ConsistentRead': consistentRead
                    }
                )

                items = response['Responses'].get(table_name, None)
                if items and len(items):
                    retry_while_no_items = True
                    for item in items:
                        yield cls._from_db_dict(item)
                        total_found += 1

                        if limit and limit <= total_found:
                            force_break = True
                            break

                elif retry_while_no_items:
                    # only retry once if no items are returned
                    retry_while_no_items = False

                else:
                    break

                if force_break:
                    break

                unprocessed = response['UnprocessedKeys'].get(table_name, None)
                keys = unprocessed.get('Keys', []) if unprocessed else []

        elif kc is not None or fc is not None:
            # do query or scan here
            kwargs = dict()

            if limit is not None:
                kwargs['Limit'] = limit

            if fc is not None:
                kwargs['FilterExpression'] = fc

            # perform a query
            if kc is not None:
                query_or_scan = 'query'
                kwargs['KeyConditionExpression'] = kc
                kwargs['ScanIndexForward'] = not descending

            # perform a scan
            else:
                # todo: issue a warning here, scans are fucking expensive
                query_or_scan = 'scan'

            func = getattr(cls.table(), query_or_scan)

            # DynamoDB only returns up to 1MB of data per trip, so we need to keep querying or scanning.
            while True:
                response = func(**kwargs)
                for row in response['Items']:
                    yield cls._from_db_dict(row)
                    total_found += 1

                if limit:
                    remaining = limit - total_found
                    if remaining < 1:
                        break
                    kwargs['Limit'] = remaining

                start_key = response.get('LastEvaluatedKey', None)
                if start_key is None:
                    break
                kwargs['ExclusiveStartKey'] = start_key

        else:
            raise ValidationError('Invalid expression {}'.format(expression))

    @classmethod
    def delete_one(cls, strategy):
        """Deletes a single item in a table.
        You can perform a conditional delete operation that deletes the item if it exists,
        or if it has an expected attribute value.

        see :ref:`doc_delete_one`

        :return: The deleted item
        """
        cls._schema()  # init
        keys, condition = cls._parse_one_strategy(strategy)
        return cls._delete_one(keys, condition)

    @classmethod
    def delete_many(cls, strategy):
        """Deletes multiple items in a table.

        see :ref:`doc_delete_many`

        :return: :py:class:`BatchResult`
        """
        cls._schema()  # init

        raw_items, items, with_conditions, expression = [], [], [], None

        if isinstance(strategy, PrimitiveCondition):
            hash_key = cls.__hash_key__
            range_key = cls.__range_key__
            if strategy.op == OP.IS_IN and not range_key and strategy.attr.name == hash_key:
                raw_items = strategy.value
            else:
                expression = strategy

        elif isinstance(strategy, CompoundKeyCondition):
            raw_items = strategy.values

        elif isinstance(strategy, BaseCondition):
            expression = strategy

        elif isinstance(strategy, list):
            with_conditions, items = [], []
            for item in strategy:
                keys, condition = cls._parse_one_strategy(item)

                if condition:
                    with_conditions.append((keys, condition))
                else:
                    items.append(keys)

        if expression:
            items.extend([item._clean_data(cls._primary_keys()) for item in cls.get_many(expression)])

        if len(raw_items):
            items.extend([cls._parse_one_strategy(item, False) for item in raw_items])

        result = BatchResult()

        if len(items):
            with cls.table().batch_writer() as batch:
                for key in items:
                    batch.delete_item(Key=key)
            result.success.extend(items)

        if len(with_conditions):
            for keys, condition in with_conditions:
                item = cls._delete_one(keys, condition)
                if item:
                    result.success.append(item)

        return result

    @classmethod
    def _delete_one(cls, keys, condition):
        """Helper method to delete a single item"""
        params = dict(Key=keys, ReturnValues='ALL_OLD')
        if condition:
            params['ConditionExpression'] = condition

        try:
            response = cls.table().delete_item(**params)
            data = response.get('Attributes', None)
            if data is not None:
                return cls(**data)
        except ClientError as e:
            if e.response['Error']['Code'] != 'ConditionalCheckFailedException':
                raise

        return None

    @classmethod
    def save_one(cls, item, overwrite=True):
        """Creates a new item, or replaces an old item with a new item.
        If an item that has the same primary key as the new item already exists in the specified table,
        the new item completely replaces the existing item
        ``overwrite`` specifies under what circumstances should we overwrite an existing item.

        If ``overwrite = True``, an existing item with the same primary key is replaced by
        the new item unconditionally. This is the default behaviour.

        If ``overwrite = False``, a :py:exc:`~dynamongo.exceptions.ConditionalCheckFailedException` is raised if there is
        an existing item with the same primary key

        If ``overwrite`` is a conditional expression, an existing item with the same primary key
        is replaced by the new item if and only if the condition is met.
        otherwise :py:exc:`~dynamongo.exceptions.ConditionalCheckFailedException` is raised.

        see :ref:`doc_save_one`

        :param item: the item to save. either a ``dict`` or ``cls``
        :param overwrite: This can be a ``bool`` or a condition. it defaults to ``True``
        :raises: :py:exc:`~dynamongo.exceptions.ConditionalCheckFailedException`
        :returns: cls
        """
        schema = cls._schema()
        item = cls._to_self(item)
        data = item._clean_data()

        if not overwrite:
            overwrite = None
            for key in cls._primary_keys():
                e = PrimitiveCondition(schema[key], OP.NE, data[key])
                overwrite = e if overwrite is None else overwrite & e

        params = dict(Item=data, ReturnValues='ALL_OLD')
        if isinstance(overwrite, BaseCondition):
            params['ConditionExpression'] = overwrite.to_boto_attr()

        try:
            cls.table().put_item(**params)
            item._raw_data = copy.deepcopy(data)
            return item
        except ClientError as e:
            if e.response['Error']['Code'] == 'ConditionalCheckFailedException':
                raise ConditionalCheckFailedException(e)
            raise

    @classmethod
    def save_many(cls, items, overwrite=True):
        """Creates or replaces multiple items.
        If an item that has the same primary key as the new item already exists in the specified table,
        the new item completely replaces the existing item
        ``overwrite`` specifies under what circumstances should we overwrite an existing item.

        If ``overwrite = True``, an existing item with the same primary key is replaced by
        the new item unconditionally. This is the default behaviour.

        If ``overwrite = False`` and there is an existing item with the same primary key,
        the item is added on ``BatchResult.fail`` list

        If ``overwrite`` is a conditional expression and an existing item with the same primary key
        does not meet the condition specified, then the item is added on ``BatchResult.fail`` list.

        see :ref:`doc_save_many`

        :param items: a list of items to save. each item can be either a ``dict`` or ``cls``
        :param overwrite: ``bool`` or a condition. it defaults to ``True``
        :returns: :py:class:`BatchResult`
        """

        # do a for loop when we have conditions
        if isinstance(overwrite, BaseCondition) or not overwrite:
            result = BatchResult()

            for item in items:
                try:
                    result.success.append(cls.save_one(item, overwrite))
                except ConditionalCheckFailedException:
                    result.fail.append(item)

            return result

        # do a batch write. It is fast
        models = [cls._to_self(item) for item in items]
        data = [m._clean_data() for m in models]

        with cls.table().batch_writer() as batch:
            for item in data:
                batch.put_item(Item=item)

        return BatchResult(success=models)

    @classmethod
    def _to_self(cls, item, maybe_keys_only=False):
        if isinstance(item, dict):
            return cls(**item)

        if isinstance(item, cls):
            return item

        if maybe_keys_only:
            hash_key = cls.__hash_key__
            range_key = cls.__range_key__

            # todo: handle expression

            if range_key and isinstance(item, tuple) and len(item) == 2:
                return cls(**{
                    hash_key: item[0],
                    range_key: item[1]
                })

            if not range_key:
                # assume the item was the hash key.
                # This raises ValidationError if the value cannot be coerced to the hash_key data type
                return cls(**{
                    hash_key: item
                })

        raise ValidationError(
            'Invalid item {}. Each item must be a dict or instance of {}'.format(item, cls))

    @classmethod
    def update_from_dict(cls, item):
        """Updates an item if and only if it exists in the db

        item primary keys must be provided.

        :param item:
        :type item: dict
        :return: updated item
        """
        schema = cls._schema()

        primary = {key: item.get(key) for key in cls._primary_keys()}

        others = set(item.keys()) - set(primary.keys())
        actions = [SetUpdate(schema[key], item.get(key)) for key in others]

        return cls.update_one(primary, actions)

    @classmethod
    def update_one(cls, strategy, updates):
        """Update all items in the db that satisfy condition

        updates are:  'ADD'|'PUT'|'DELETE'

        :param strategy: Single item selection strategy
        :param updates: list[Update]
        :return: List of updated items
        """
        cls._schema()  # init

        if isinstance(updates, Update):
            updates = [updates]

        if is_empty(updates):
            raise ValidationError('At least one attribute must be updated')

        expression, values = UpdateBuilder.create(updates)
        keys = cls._parse_one_strategy(strategy, False)

        # Do not allow creation of new items. STRICTLY UPDATE ONLY
        from boto3.dynamodb.conditions import Key
        condition = None
        for k, v in keys.items():
            if condition is None:
                condition = Key(k).eq(v)
            else:
                condition &= Key(k).eq(v)

        try:
            response = cls.table().update_item(
                Key=keys,
                UpdateExpression=expression,
                ExpressionAttributeValues=values,
                ConditionExpression=condition,
                ReturnValues='ALL_NEW'
            )
            return cls._from_db_dict(response['Attributes'])
        except ClientError as e:
            if e.response['Error']['Code'] == 'ConditionalCheckFailedException':
                return None
            raise

    @classmethod
    def _from_db_dict(cls, data):
        model = cls(**data)
        model._raw_data = copy.deepcopy(data)
        return model

    def __init__(self, **kwargs):
        schema = self._schema()

        if not self.__table__:
            raise SchemaError("Class does not define __table__")

        if not self.__hash_key__:
            raise SchemaError("Class does not define __hash_key__")

        for key in self._primary_keys():
            if key not in schema:
                raise SchemaError('A field must be defined for the primary key attribute "{}"'.format(key))

        self._raw_data = {}

        # Extract the default value for each field
        defaults = {key: field.default for key, field in schema.items()}

        # Merge the defaults with given values
        data = merge_deep(defaults, kwargs)

        # convert to native objects
        for key, field in schema.items():
            value = field.to_native(data[key])
            setattr(self, key, value)

    def to_dict(self, only=None):
        if only is None:
            only = self._schema().keys()
        return {k: copy.deepcopy(getattr(self, k)) for k in only}

    def _clean_data(self, fields=None):
        # Extra keys are not allowed for the outermost dict. Inner dict can have extra keys
        schema = self._schema()

        if fields is None:
            fields = schema.keys()

        data = {}
        for key in fields:
            field = schema[key]
            value = copy.deepcopy(getattr(self, key))
            data[key] = field.to_primitive(value)

        data = non_empty_values(data)

        # ensure primary keys are provided
        for key in self._primary_keys():
            if key not in data:
                raise ValidationError(
                    'All primary key attributes "{}" must be set to a valid value'.format(self._primary_keys()))

        return data


class BatchResult:
    """Batch result class"""
    def __init__(self, fail=list(), success=list()):
        self.success = success
        self.fail = fail

    @property
    def success_count(self):
        return len(self.success)

    @property
    def fail_count(self):
        return len(self.fail)
