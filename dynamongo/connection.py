"""Connection borg"""
import boto3
import os

__all__ = ['Connection']


def _env_config():
    """Read config from the env"""
    return {
        'aws_access_key_id': os.environ.get('AWS_ACCESS_KEY_ID'),
        'aws_secret_access_key': os.environ.get('AWS_SECRET_ACCESS_KEY'),
        'region_name': os.environ.get('AWS_REGION_NAME', 'us-east-2'),
        'table_prefix': os.environ.get('AWS_TABLE_PREFIX')
    }


class Connection(object):
    """Borg that handles access to DynamoDB.

    You should never make any explicit/direct ``boto3.dynamodb`` calls by yourself
    except for table maintenance operations

    Before making any calls, aws credentials must be set by either:

    1. calling :py:meth:`~Connection.set_config`, or
    2. setting environment variables

        - ``AWS_ACCESS_KEY_ID``
        - ``AWS_SECRET_ACCESS_KEY``
        - ``AWS_REGION_NAME``
        - ``AWS_TABLE_PREFIX``
    """
    _connection = None
    _client = None
    _table_cache = {}
    _config = None

    @classmethod
    def set_config(cls, access_key_id=None, secret_access_key=None, region=None, table_prefix=None):
        """Set configuration.

        This is needed only once, globally, per-thread.
        Each of these parameters are optional.
        Provided values overwrite those set using environment variables

        :param access_key_id: AWS access key id
        :param secret_access_key: AWS secret access key
        :param region: AWS region name
        :param table_prefix: The global table namespace to be used for all tables
        """
        config = dict(
            aws_access_key_id=access_key_id,
            aws_secret_access_key=secret_access_key,
            region_name=region,
            table_prefix=table_prefix
        )

        config = {k: v for k, v in config.items() if v is not None}
        old = cls.config()
        cls._config = {**old, **config}

    @classmethod
    def config(cls):
        """
        Get config

        Config can either be set by a call to :py:meth:`~Connection.set_config`
        or environment variables
        """
        if cls._config is None:
            cls._config = dict(**_env_config())
        return cls._config

    @classmethod
    def _config_to_kwargs(cls):
        """Helper method to get all config variables except table prefix"""
        return {k: v for k, v in cls.config().items() if k != 'table_prefix'}

    @classmethod
    def client(cls):
        """Return the DynamoDB client"""
        if cls._client is None:
            cls._client = boto3.client(
                'dynamodb',
                **cls._config_to_kwargs()
            )
        return cls._client

    @classmethod
    def resource(cls):
        """Return the DynamoDB connection"""
        if cls._connection is None:
            cls._connection = boto3.resource(
                'dynamodb',
                **cls._config_to_kwargs()
            )
        return cls._connection

    @classmethod
    def get_table(cls, name):
        """Return DynamoDB Table object"""
        if name not in cls._table_cache:
            cls._table_cache[name] = cls.resource().Table(name)
        return cls._table_cache[name]

    @classmethod
    def table_prefix(cls):
        """Return the ``table_prefix``"""
        prefix = cls.config()['table_prefix']
        return '' if prefix is None else prefix
