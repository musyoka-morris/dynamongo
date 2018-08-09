from inflection import tableize
from functools import wraps
import collections
import copy
from botocore.exceptions import ClientError

from .utils import *
from .attributes import Dict, Attribute
from .exceptions import SchemaError, ConditionalCheckFailedException, ValidationError
from .connection import Connection
from .conditions import BaseCondition, AndCondition, PrimitiveCondition, OP


__all__ = [
    'BaseModel',
    'Index', 'LocalIndex', 'GlobalIndex',
    'BatchIterator', 'SearchIterator',
    'DictAttribute', 'IMeta'
]


###################################################################################
# mixins
class _HasAttributes:
    @classmethod
    def get_raw_attributes(cls):
        attributes = {}
        for key, attribute in cls.__dict__.items():
            if isinstance(attribute, Attribute):
                attributes[key] = attribute
            elif is_subclass(attribute, DictAttribute):
                attributes[key] = attribute.create()
        return attributes

    @classmethod
    def get_raw_indexes(cls):
        indexes = {}
        for key, value in cls.__dict__.items():
            if isinstance(value, Index):
                indexes[key] = value
        return indexes


def _parse_key(primary_keys, key):
    names = [x.name for x in primary_keys]
    has_range = len(names) == 2

    if isinstance(key, BaseModel):
        d = {name: getattr(key, name) for name in names}

    elif isinstance(key, dict):
        missing = set(names) - set(key.keys())
        if len(missing) > 0:
            raise ValueError('Missing key values for keys {}'.format(missing))
        d = {name: key[name] for name in names}

    elif isinstance(key, BaseCondition):
        if has_range:
            error = ValueError(
                'Both & only hash_key ({}) and range_key ({}) equality conditions must be specified'
                ' and ANDed together. Found {}'.format(*names, key)
            )

            if not isinstance(key, AndCondition) or len(key.all) != 2:
                raise error

            d = {}
            for cond in key.all:
                if not isinstance(cond, PrimitiveCondition):
                    raise error

                name = cond.attr.name
                if cond.op != OP.EQ or name not in names or name in d:
                    raise error

                d[name] = cond.value

        else:
            error = ValueError(
                'Only hash_key ({}) equality condition is allowed.'
                'Found {}'.format(*names, key)
            )

            if not isinstance(key, PrimitiveCondition):
                raise error

            name = key.attr.name
            if key.op != OP.EQ or name not in names:
                raise error

            d = {name: key.value}

    elif has_range and isinstance(key, tuple) and len(key) == 2:
        d = {name: value for name, value in zip(names, key)}

    elif not has_range:
        d = {names[0]: key}

    else:
        raise ValueError('Invalid key {}'.format(key))

    return {
        attr.name: attr.dump(d.get(attr.name))
        for attr in primary_keys
    }


def _key_schema(attrs):
    return [
        {
            'AttributeName': attr.name,
            'KeyType': key_type
        } for attr, key_type in zip(attrs, ['HASH', 'RANGE'])
    ]


#############################################################################
# indexes
class Index:
    PROJECTION_ALL = 'ALL'
    PROJECTION_KEYS_ONLY = 'KEYS_ONLY'

    def __init__(self, name=None, projection=PROJECTION_ALL):
        self.name = name
        self.projection = projection

        self.parent = None
        """:type: Type[BaseModel]"""

        self.hash_key = None
        self.range_key = None

        self._projection =  None

    def __bind_subclass__(self):
        raise NotImplementedError

    def __bind__(self, name, parent):
        """
        :param str name:
        :param Type[BaseModel] parent:
        """
        self.parent = parent
        if is_empty(self.name):
            self.name = name

        # bind subclass
        self.__bind_subclass__()

    def __create_definition__(self):
        # parse projection
        projection = dict()
        if self.projection in [self.PROJECTION_ALL, self.PROJECTION_KEYS_ONLY]:
            projection['ProjectionType'] = self.projection
        else:
            projection['ProjectionType'] = 'INCLUDE'
            projection['NonKeyAttributes'] = self.projection

        return dict(
            IndexName=self.name,
            KeySchema=_key_schema(self.primary_keys()),
            Projection=projection
        )

    def primary_keys(self):
        if self.range_key:
            return [self.hash_key, self.range_key]
        return [self.hash_key]

    def parse_key(self, key):
        return _parse_key(self.primary_keys(), key)

    def _parse_consistent(self, consistent):
        return consistent

    def _parse_key_attr(self, attr):
        if not isinstance(attr, Attribute):
            attr = self.parent.Meta.schema.get(attr)
            if not attr:
                raise ValueError(
                    'Given name ({}) is not a valid attribute name at Index({})'
                    .format(attr, self.name)
                )

        return attr

    def query(self, key, filter=None, projection=None, consistent=False, limit=None, forward=True):
        return QueryIterator(
            key,
            forward,
            cls=self.parent,
            index=self,
            filter=filter,
            projection=projection,
            consistent=self._parse_consistent(consistent),
            limit=limit
        )

    def scan(self, filter=None, projection=None, consistent=False, limit=None, parallel=None):
        return ScanIterator(
            parallel,
            cls=self.parent,
            index=self,
            filter=filter,
            projection=projection,
            consistent=self._parse_consistent(consistent),
            limit=limit
        )


class LocalIndex(Index):
    def __init__(self, range_key, **kwargs):
        super().__init__(**kwargs)
        self.range_key = range_key

    def __bind_subclass__(self):
        self.range_key = self._parse_key_attr(self.range_key)
        self.hash_key = self.parent.Meta.hash_key


class GlobalIndex(Index):
    def __init__(self, hash_key, range_key=None, read_units=None, write_units=None, **kwargs):
        super().__init__(**kwargs)
        self.hash_key = hash_key
        self.range_key = range_key
        self.read_units = read_units
        self.write_units = write_units

    def __bind_subclass__(self):
        meta = self.parent.Meta

        if not self.read_units:
            self.read_units = meta.read_units

        if not self.write_units:
            self.write_units = meta.write_units

        self.hash_key = self._parse_key_attr(self.hash_key)
        if self.range_key:
            self.range_key = self._parse_key_attr(self.range_key)

    def __create_definition__(self):
        definition = super().__create_definition__()
        definition['ProvisionedThroughput'] = dict(
            ReadCapacityUnits=self.read_units,
            WriteCapacityUnits=self.write_units
        )
        return definition

    def _parse_consistent(self, _):
        # You can raise error here if consistent is true
        return False


def _prepare_projection_expression(projection):
    if not projection:
        return None

    if isinstance(projection, (str, Attribute)):
        projection = [projection]

    projection = [x.name if isinstance(x, Attribute) else x for x in projection]
    names = {'#%s' % name: name for name in projection}

    return dict(
        ProjectionExpression=','.join(names.keys()),
        ExpressionAttributeNames=names
    )


#################################################################################
# Result Iterators
class BatchIterator:
    def __init__(self, cls, keys):
        """
        :param Type[BaseModel] cls:
        :param tuple keys:
        """
        self.cls = cls
        self.keys = [cls._parse_key(key) for key in keys]

    def __iter__(self):
        keys, cls = self.keys, self.cls

        resource = cls.Meta.connection.resource()
        table_name = cls.Meta.table_name
        retry_while_no_items = True

        while len(keys):
            response = resource.batch_get_item(
                RequestItems={
                    table_name: {
                        'Keys': keys
                    }
                }
            )

            items = response['Responses'].get(table_name, None)
            if items and len(items):
                retry_while_no_items = True
                for item in items:
                    yield cls._from_db_dict(item)

            elif retry_while_no_items:
                # only retry once if no items are returned
                retry_while_no_items = False

            else:
                break

            unprocessed = response['UnprocessedKeys'].get(table_name, None)
            keys = unprocessed.get('Keys', []) if unprocessed else []

    def all(self):
        return list(self)


class SearchIterator:
    method = None

    def __init__(self, cls=None, index=None, filter=None, projection=None, consistent=False, limit=None):
        """
        :param BaseModel cls:
        :param Index index:
        :param BaseCondition filter:
        :param list projection:
        :param boolean consistent:
        :param int limit:
        """
        self.cls = cls
        self.index = index
        self.filter = filter
        self.projection = projection
        self.consistent = consistent
        self.limit = limit

    def _func(self):
        return getattr(self.cls.Meta.table(), self.method)

    def _params(self):
        params = dict(
            ConsistentRead=self.consistent
        )

        if self.index is not None:
            params['IndexName'] = self.index.name

        if self.limit is not None:
            params['Limit'] = self.limit

        if self.filter is not None:
            params['FilterExpression'] = self.filter.to_boto_attr()

        projection = _prepare_projection_expression(self.projection)
        if projection:
            params['Select'] = 'SPECIFIC_ATTRIBUTES'
            params = {**params, **projection}
        # elif not isinstance(self.index, GlobalIndex):
        #     params['Select'] = 'ALL_ATTRIBUTES'

        return params

    def __iter__(self):
        func, params = self._func(), self._params()
        cls, limit = self.cls, self.limit
        total_found = 0

        # DynamoDB only returns up to 1MB of data per trip, so we need to keep searching
        while True:
            response = func(**params)
            for row in response['Items']:
                yield cls._from_db_dict(row)
                total_found += 1

            if limit:
                remaining = limit - total_found
                if remaining < 1:
                    break
                params['Limit'] = remaining

            start_key = response.get('LastEvaluatedKey', None)
            if start_key is None:
                break
            params['ExclusiveStartKey'] = start_key

    def all(self):
        return list(self)

    def first(self):
        clone = copy.deepcopy(self)
        clone.limit = 1
        items = clone.all()
        if len(items):
            return items[0]
        raise ValueError('No items found')

    def count(self):
        func, params = self._func(), self._params()
        params['Select'] = 'COUNT'
        print(params)
        response = func(**params)
        return response.get('Count', 0)


class QueryIterator(SearchIterator):
    method = 'query'

    def __init__(self, key, forward, **kwargs):
        """
        :param BaseCondition key:
        :param boolean forward:
        """
        super().__init__(**kwargs)
        self.key = key.to_boto_key()
        self.forward = forward

    def _params(self):
        return {
            **super()._params(),
            **dict(
                KeyConditionExpression=self.key,
                ScanIndexForward=self.forward
            )
        }


class ScanIterator(SearchIterator):
    method = 'scan'

    def __init__(self, parallel, **kwargs):
        super().__init__(**kwargs)
        self.parallel = parallel


#######################################################################################
# Helper classes
class DictAttribute(_HasAttributes):
    __extra__ = Dict.EXTRA_ALLOW

    def __init__(self):
        raise RuntimeError('{} class can not be instantiated'.format(self.__name__))

    @classmethod
    def create(cls):
        return Dict(cls.get_raw_attributes(), extra=cls.__extra__)


class _Dummy:
    pass


class IMeta:
    abstract = False
    table_name = None

    read_units = 8
    write_units = 8

    # parameters below here should never be set explicitly
    hash_key = None
    range_key = None

    schema = dict()
    indexes = dict()
    connection = None
    """:type: Connection"""

    @classmethod
    def primary_keys(cls, names_only=False):
        """:rtype: list[:py:class:`Attribute`|str]"""
        keys = [cls.hash_key]
        if cls.range_key:
            keys = [cls.hash_key, cls.range_key]

        if names_only:
            return [attr.name for attr in keys]
        return keys

    @classmethod
    def table(cls):
        """Get a dynamoDB Table instance for this model"""
        return cls.connection.get_table(cls.table_name)


_BOUND_MODELS = set()


def _is_bound(cls):
    return cls in _BOUND_MODELS


def _checks_bound(func):
    @wraps(func)
    def wrapper(cls, *args, **kwargs):
        if not _is_bound(cls):
            raise SchemaError(
                'Model {} not bound yet. Call BaseModel.__bind__ to bind all models'.format(cls)
            )
        return func(cls, *args, **kwargs)
    return wrapper


class BaseModel(_HasAttributes):
    __connection__ = None
    """:type: Connection"""

    class Meta(IMeta):
        abstract = True

    @classmethod
    def _from_db_dict(cls, data):
        return cls(**data)

    def __init__(self, **kwargs):
        schema = self.Meta.schema

        # Extract the default value for each attribute
        defaults = {key: attr.default for key, attr in schema.items()}

        # Merge the defaults with given values
        data = merge_deep(defaults, kwargs)

        # convert to native objects
        for key, attr in schema.items():
            value = attr.load(data[key])
            setattr(self, key, value)

    @classmethod
    def _clean_item(cls, item):
        if isinstance(item, dict):
            item = cls(**item)

        if not isinstance(item, cls):
            raise ValueError('Invalid item. expected a dict or an instance of {}'.format(cls))

        data = non_empty_values({
            key: attr.dump(copy.deepcopy(getattr(item, key)))
            for key, attr in cls.Meta.schema.items()
        })

        # ensure primary keys are provided
        keys = cls.Meta.primary_keys(names_only=True)
        for key in keys:
            if key not in data:
                raise ValidationError(
                    'All primary key attributes "{}" must be set to a valid value'.format(keys))

        return data

    @classmethod
    def _parse_condition(cls, condition):
        if condition is None:
            return None

        if isinstance(condition, BaseCondition):
            return condition.to_boto_attr()

        raise ValueError('Invalid condition {}'.format(condition))

    @classmethod
    def _parse_key(cls, key):
        return _parse_key(cls.Meta.primary_keys(), key)

    @classmethod
    @_checks_bound
    def get(cls, key, consistent=False, projection=None):
        params = dict(
            Key=cls._parse_key(key),
            ConsistentRead=consistent
        )

        projection = _prepare_projection_expression(projection)
        if projection:
            params = {**params, **projection}

        response = cls.Meta.table().get_item(**params)
        if response and 'Item' in response:
            return cls._from_db_dict(response["Item"])

        return None

    @classmethod
    @_checks_bound
    def exists(cls, key, consistent=False):
        item = cls.get(key, consistent=consistent, projection=cls.Meta.hash_key)
        return True if item else False

    @classmethod
    @_checks_bound
    def delete(cls, key, condition=None):
        params = dict(
            Key=cls._parse_key(key),
            ReturnValues='ALL_OLD'
        )

        condition = cls._parse_condition(condition)
        if condition:
            params['ConditionExpression'] = condition

        try:
            response = cls.Meta.table().delete_item(**params)
            data = response.get('Attributes', None)
            if data is not None:
                return cls(**data)
        except ClientError as e:
            if e.response['Error']['Code'] != 'ConditionalCheckFailedException':
                raise

        return None

    @classmethod
    @_checks_bound
    def save(cls, item, condition=None):
        item = cls._clean_item(item)
        params = dict(
            Item=item,
            ReturnValues='ALL_OLD'
        )

        condition = cls._parse_condition(condition)
        if condition:
            params['ConditionExpression'] = condition

        try:
            cls.Meta.table().put_item(**params)
            return cls._from_db_dict(item)
        except ClientError as e:
            if e.response['Error']['Code'] == 'ConditionalCheckFailedException':
                raise ConditionalCheckFailedException(e)
            raise

    @classmethod
    @_checks_bound
    def update(cls, updates, condition=None):
        pass

    @classmethod
    @_checks_bound
    def query(cls, key, filter=None, projection=None, consistent=False, limit=None, forward=True):
        return QueryIterator(
            key,
            forward,
            cls=cls,
            filter=filter,
            projection=projection,
            consistent=consistent,
            limit=limit
        )

    @classmethod
    @_checks_bound
    def scan(cls, filter=None, projection=None, consistent=False, limit=None, parallel=None):
        return ScanIterator(
            parallel,
            cls=cls,
            filter=filter,
            projection=projection,
            consistent=consistent,
            limit=limit
        )

    @classmethod
    @_checks_bound
    def get_many(cls, *keys):
        return BatchIterator(cls, keys)

    @classmethod
    @_checks_bound
    def delete_many(cls, *keys):
        keys = [cls._parse_key(key) for key in keys]
        with cls.Meta.table().batch_writer() as batch:
            for key in keys:
                batch.delete_item(Key=key)

    @classmethod
    @_checks_bound
    def save_many(cls, *items):
        items = [cls._clean_item(item) for item in items]
        with cls.Meta.table().batch_writer() as batch:
            for item in items:
                batch.put_item(Item=item)

        return [cls._from_db_dict(item) for item in items]

    ############################################################################
    @classmethod
    def __bind__(cls, skip_table_setup=False):
        if _is_bound(cls):
            return

        # bind subclasses
        for model in cls.__subclasses__():
            model.__bind__(skip_table_setup)

        # create meta class
        class_ = cls.Meta if 'Meta' in cls.__dict__ else _Dummy

        class Meta(class_, IMeta):
            pass

        if Meta.abstract:
            return

        cls.Meta = Meta

        # connection
        connection = Meta.connection
        if not connection:
            connection = cls.__connection__
            Meta.connection = connection

        if not isinstance(connection, Connection):
            raise ValueError('Invalid connection {}'.format(connection))

        # set table name
        table_name = Meta.table_name
        if is_empty(table_name):
            table_name = tableize(cls.__name__)
        Meta.table_name = Meta.connection.prefixed_table_name(table_name)

        # init attributes
        Meta.schema = cls.get_raw_attributes()
        for name, attr in Meta.schema.items():
            attr.__bind__(name, cls)

        # List of attributes
        attrs = Meta.schema.values()

        # validate hash key
        hash_keys = [x for x in attrs if x.hash_key]
        size = len(hash_keys)
        if size == 1:
            hash_key = hash_keys.pop()
            Meta.hash_key = hash_key
        elif size == 0:
            raise SchemaError('Missing hash_key for model {}'.format(cls))
        else:
            raise SchemaError(
                'Multiple attributes {} declared as hash_key for model {}'
                .format(sorted(hash_keys), cls)
            )

        # validate range key
        range_keys = [x for x in attrs if x.range_key]
        size = len(range_keys)
        if size == 1:
            range_key = range_keys.pop()
            Meta.range_key = range_key
        elif size > 1:
            raise SchemaError(
                'Multiple attributes {} declared as range_key for model {}'
                .format(sorted(range_keys), cls)
            )

        # Init indexes
        Meta.indexes = cls.get_raw_indexes()
        for name, index in Meta.indexes.items():
            index.__bind__(name, cls)

        # validation for collisions in local attributes/indexes
        names = [x.name for x in list(Meta.schema.values()) + list(Meta.indexes.values())]
        duplicates = [item for item, count in collections.Counter(names).items() if count > 1]
        if len(duplicates):
            raise SchemaError(
                'Duplicate attribute or index names {} found in model {}'.format(sorted(set(duplicates)), cls)
            )

        # finalize Meta
        _BOUND_MODELS.add(cls)

        # Create table
        if not skip_table_setup:
            try:
                cls.__create_table__()
            except ClientError as e:
                if e.response['Error']['Code'] != 'ResourceInUseException':
                    raise

                # Table exists. update the table
                cls.__update_table__()

    ########################################################################
    # Table management operations
    @classmethod
    @_checks_bound
    def __create_table__(cls):
        """
        Create a table that'll be used to store instances of cls in AWS dynamoDB.

        This operation should be called before any table read or write operation is undertaken
        """
        meta = cls.Meta

        # prepare attribute definitions
        key_attributes = meta.primary_keys()
        for index in meta.indexes.values():
            key_attributes = key_attributes + index.primary_keys()

        attrs_defined, attributes = [], []
        for attr in key_attributes:
            name = attr.name
            if name not in attrs_defined:
                attributes.append({
                    'AttributeName': name,
                    'AttributeType': key_proto(attr)
                })
                attrs_defined.append(name)

        # Indexes
        li, gi = [], []
        for index in meta.indexes.values():
            definition = index.__create_definition__()
            if isinstance(index, GlobalIndex):
                gi.append(definition)
            else:
                li.append(definition)

        # define parameters
        client = meta.connection.client()
        table_name = meta.table_name
        params = dict(
            TableName=table_name,
            KeySchema=_key_schema(meta.primary_keys()),
            AttributeDefinitions=attributes,
            ProvisionedThroughput={
                'ReadCapacityUnits': meta.read_units,
                'WriteCapacityUnits': meta.write_units
            }
        )

        if len(li):
            params['LocalSecondaryIndexes'] = li
        if len(gi):
            params['GlobalSecondaryIndexes'] = gi

        # Create & Wait until the table exists. this can take up to a minute.
        client.create_table(**params)
        client.get_waiter('table_exists').wait(TableName=table_name)

    @classmethod
    @_checks_bound
    def __update_table__(cls):
        pass

    @classmethod
    @_checks_bound
    def __delete_table__(cls):
        client = cls.Meta.connection.client()
        table_name = cls.Meta.table_name
        client.delete_table(TableName=table_name)
        client.get_waiter('table_not_exists').wait(TableName=table_name)
