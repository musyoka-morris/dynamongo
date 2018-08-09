"""Connection borg"""
import boto3
import os

__all__ = ['Connection']


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
    def __init__(self, access_key_id=None, secret_access_key=None, region=None, table_prefix=None):
        """Set configuration.

        This is needed only once, globally, per-thread.
        Each of these parameters are optional.
        Provided values overwrite those set using environment variables

        :param access_key_id: AWS access key id
        :param secret_access_key: AWS secret access key
        :param region: AWS region name
        :param table_prefix: The global table namespace to be used for all tables
        """
        self.access_key_id = access_key_id
        self.secret_access_key = secret_access_key
        self.region = region
        self.table_prefix = table_prefix if table_prefix else ''

        self._resource = None
        self._client = None
        self._table_cache = {}

    @classmethod
    def from_env(cls):
        """Read config from the env"""
        return cls(
            access_key_id=os.environ.get('AWS_ACCESS_KEY_ID'),
            secret_access_key=os.environ.get('AWS_SECRET_ACCESS_KEY'),
            region=os.environ.get('AWS_REGION_NAME', 'us-east-2'),
            table_prefix=os.environ.get('AWS_TABLE_PREFIX')
        )

    def _boto3_kwargs(self):
        """Helper method to get all config variables except table prefix"""
        return dict(
            aws_access_key_id=self.access_key_id,
            aws_secret_access_key=self.secret_access_key,
            region_name=self.region,
        )

    def client(self):
        """Return the DynamoDB client"""
        if self._client is None:
            self._client = boto3.client(
                'dynamodb',
                **self._boto3_kwargs()
            )
        return self._client

    def resource(self):
        """Return DynamoDB Resource"""
        if self._resource is None:
            self._resource = boto3.resource(
                'dynamodb',
                **self._boto3_kwargs()
            )
        return self._resource

    def get_table(self, name):
        """Return DynamoDB Table object"""
        if name not in self._table_cache:
            self._table_cache[name] = self.resource().Table(name)
        return self._table_cache[name]

    def prefixed_table_name(self, name):
        return "{}{}".format(self.table_prefix, name)
