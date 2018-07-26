.. module:: dynamongo

Quickstart
==========

This guide will walk you through the basics of working with *DynamoDB* and *dynamongo*


Prerequisites
-------------

Before we start, make sure that you have an AWS access key id & AWS secret access key.
If you don't have these keys yet,  you can create them from the AWS Management Console
by `following this documentation <https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/SettingUp.DynamoWebService.html>`_.

.. _doc_connection:

Connection
----------

Before making any calls, dynamongo needs to have access to AWS dynamoDB.
Additionally, it is recommended each repository using this library should have a unique prefix for table names.
AWS connection credentials and the table name prefix can be set in either of two ways:

1. **ENVIRONMENT VARIABLES**
    This is the recommended way of setting dynamongo connection. The env variables are

    - ``AWS_ACCESS_KEY_ID`` : Required
    - ``AWS_SECRET_ACCESS_KEY`` : Required
    - ``AWS_REGION_NAME`` : Optional, defaults to ``us-east-2``
    - ``AWS_TABLE_PREFIX`` : Optional, defaults to ``None``

2. **USING CONNECTION CLASS**

    .. code-block:: python

        from dynamongo import Connection

        Connection.set_config(
            access_key_id='<your aws access key id>',
            secret_access_key='<your aws secret access key>',
            region='<aws region name>',
            table_prefix='<table prefix of your choice>'
        )

    Any values set using this method override environment variables.
    This only need be called once, but it must be called before any attempt to make calls to DynamoDB.

.. Note::
    The ``table_prefix`` is more of a good practice than a feature.
    In DynamoDB, each customer is allocated a single database.
    It is highly recommended to prefix your tables with a name of the form ``application-specific-name``
    to avoid table name collisions with other projects.


.. _doc_declare_model:

Declaring Models
----------------

Lets start with a basic user 'model'

.. code-block:: python

    import datetime
    from dynamongo import Model
    from dynamongo import IntField, StringField, ListField, EmailField, DateTimeField

    class User(Model):
        __table__ = 'users'
        __hash_key__ = 'email'

        email = EmailField(required=True)
        name = StringField(required=True)
        year_of_birth = IntField(max_value=2018, min_value=1900)
        cities_visited = ListField(StringField)
        created_at = DateTimeField(default=datetime.datetime.now)

Every model must declare the following attributes::

    __table__: The name of the table

    __hash_key__: Hash key for the table

and at least one field for the Hash key.
See :py:class:`~dynamongo.model.Model` for detailed documentation on the allowed Model attributes


.. _doc_create_table:

Creating the table
-------------------

Unlike other NoSQL engines like MongoDB, tables must be created and managed explicitly.
At the moment, dynamongo abstracts only the initial table creation.
Other lifecycle management operations may be done directly via Boto3.

To create the table, use :py:meth:`~dynamongo.model.Model.create_table`.
The throughput provisioned for this table is determined by the
attributes ``__read_units__`` & ``__write_units__``. These are optional and they default to ``8``.

.. note::

    Unlike most databases, table creation may take up to 1 minute.

For more information, please see `Amazon’s official documentation <https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/SQLtoNoSQL.CreateTable.html>`_.


.. _doc_save:

Saving data
-----------

.. _doc_save_one:

Saving single item
******************

Saving a single item can be done by calling :py:meth:`~dynamongo.model.Model.save_one` method.
item to be saved is passed as a ``dict`` or an instance of :py:class:`~dynamongo.model.Model`.

By default, if an item that has the same primary key as the new item already exists,
the new item completely replaces the existing item.

You can override this behaviour by passing ``overwrite=False``.
In this case, if an item that has the same primary key as the new item already exists,
a :py:exc:`~dynamongo.exceptions.ConditionalCheckFailedException` exception is raised. Otherwise, the item is saved.

Example using a `dict` object

.. code-block:: python

    john = User.save_one({
        'email': 'johndoe@gmail.com',
        'name': 'John Doe',
        'year_of_birth': 1990,
        'cities_visited': ['Nairobi', 'New York']
    })


Example using a :py:class:`~dynamongo.model.Model` instance

.. code-block:: python

    user = User(
        email='johndoe@gmail.com',
        name='John Doe',
        cities_visited=[]
    )
    user.year_of_birth = 1990
    user.cities_visited = ['Nairobi', 'New York']
    user = User.save_one(user)


.. _doc_save_many:

Saving multiple items
**********************


Multiple items can be saved by calling :py:meth:`~dynamongo.model.Model.save_many` method.
This method takes as input a ``list`` of:

- ``dict`` objects, or
- :py:class:`~dynamongo.model.Model` instances, or
- mixture of both ``dict`` objects and :py:class:`~dynamongo.model.Model` instances

This method returns an :py:class:`~dynamongo.model.BatchResult` instance.

By default, existing items are completely replaced by new items.
passing ``overwrite=False`` changes the default behaviour,
and items which could not be created since an item already exists with the same primary key,
are considered ``failed``.


.. code-block:: python

    user_list = [
        # first user. defined as a dict
        {
            'email': 'johndoe@gmail.com',
            'name': 'John Doe',
            'year_of_birth': 1990,
            'cities_visited': ['Nairobi', 'New York']
        },

        # second user. User instance
        User(
            email='johndoe@gmail.com',
            name='John Doe',
            cities_visited=[]
        )
    ]

    result = User.save_many(user_list, overwrite=False)
    print(result.fail_count)


.. _doc_delete:

Deleting Data
-------------

Just as with saving data, you can delete a single item or many items at once.

.. _doc_delete_one:

Deleting a single item
**********************

Deleting a single item can be done by calling :py:meth:`~dynamongo.model.Model.delete_one` method.
If an item by the given strategy exists, it is deleted and the deleted item is returned.
Otherwise ``None`` is returned.

This method takes in ``strategy`` as input.
``strategy`` can be either of the following:

1. **The primary key value**.

If a model has a `hash_key` only, this is passed in as a scalar.
Otherwise, if the model has both `hash_key` and `range_key`,
the value is passed as a ``(hash_key, range_key)`` tuple.

.. code-block:: python

    user = User.delete_one('johndoe@gmail.com')

2. **Dict object**

The dict should contain all primary key values.
i.e, if the model has both `hash_key` and `range_key`, both should be included in the dict.
Otherwise only a dict with the ``hash_key`` is required.

Non primary key items in the dict are ignored.

.. code-block:: python

    user = User.delete_one({'email': 'johndoe@gmail.com'})


3. **Model instance**

The primary fields attributes must have valid values. Item is deleted by the primary keys.

.. code-block:: python

    user = User.delete_one(User(email='johndoe@gmail.com'))


4. **Key condition**

In its simplest form, if the model does not have a `range_key`,
this should be an equality condition on the hash_key field.

if the model has both `hash_key` and `range_key`,
this should be two equality conditions on both key fields `ANDed` together.

.. code-block:: python

    user = User.delete_one(User.email == 'johndoe@gmail.com')


5. **Key condition + additional checks**

This allows one to delete an item based on the primary key, but with an additional check.

Example #1. Suppose we want to delete a user whose primary key ``email=johndoe@gmail.com``,
but only if the user was born on or before the year ``2000``

.. code-block:: python

    user = User.delete_one(
        (User.email == 'johndoe@gmail.com') & (User.year_of_birth <= 2000)
    )

Example #2. Delete a user whose ``email=johndoe@gmail.com`` if the user has already visited ``Nairobi`` city

.. code-block:: python

    user = User.delete_one(
        (User.email == 'johndoe@gmail.com') & User.cities_visited.contains('Nairobi')
    )

Example #3. This can become even more complex. Delete a user whose ``email=johndoe@gmail.com``
AND the user was born after ``2000`` or the user has already visited ``Nairobi`` city

.. code-block:: python

    user = User.delete_one(
        (User.email == 'johndoe@gmail.com') &
        ((User.year_of_birth > 2000) | User.cities_visited.contains('Nairobi'))
    )

In all cases, equality conditions for the primary keys **must** be present in the condition.
All other conditional checks **must** be `ANDed` to the primary key conditions.
This rule is strictly enforced by both *dynamongo* and *DynamoDB*.
For example, the following strategy would fail:

.. code-block:: python

    # This raises an ExpressionError. The condition is ORed instead of being ANDed
    user = User.delete_one(
        (User.email == 'johndoe@gmail.com') | (User.year_of_birth > 2000))
    )


.. _doc_delete_many:

Deleting multiple items
***********************

Multiple items can be deleted by calling Model.delete_many method.
This method takes in ``strategy`` as input. ``strategy`` can be either of the following:

1. **List**

Each entry in this list must be a valid object that can be passed to the :py:meth:`~dynamongo.model.Model.delete_one` method
as described above.

Examples

.. code-block:: python

    result = User.delete_many([
        'johndoe@gmail.com',
        'email1@gmail.com',
        {'email': 'email2@gmail.com'},
        User(email='email3@gmail.com'),
        User.email == 'email4@gmail.com',
        (User.email == 'email5@gmail.com') & (User.year_of_birth <= 2000)
    ])

2. **Condition**

Here you can pass any valid condition. Suppose we have list of user emails:

.. code-block:: python

     emails = ['johndoe@gmail.com', 'email2@abc.io', 'anotherone@xyz.com']

Example #1. Delete those users unconditionally. It can be achieved in either of the following ways

.. code-block:: python

    # simply passing in the list of emails
    result = User.delete_many(emails)

.. code-block:: python

    # more control. We know exactly what emails is
    result = User.delete_many(User.email.in_(emails))

.. code-block:: python

    # Useful when using composite primary keys
    result = User.delete_many(User.keys_in(emails))


Example #2. Only delete users in the list, but only if the user was born on or before the year ``2000``

.. code-block:: python

    result = User.delete_many(
                (User.email.in_(emails)) &
                (User.year_of_birth > 2000)
            )

Example #3. Delete all users who have ever visited ``Nairobi`` city

.. code-block:: python

    result = User.delete_many(User.cities_visited.contains('Nairobi'))

Example #4. Delete any user who was born before ``1990`` and has never visited ``Nairobi``.
`(we do not need boring people in our system)`

.. code-block:: python

    result = User.delete_many(
                (User.year_of_birth < 1990) &
                (not User.cities_visited.contains('Nairobi'))
            )


.. _doc_get:

Accessing data
--------------

dynamongo supports retrieval of a single item or many items at once.

.. _doc_get_one:

Getting a single item
*********************

Getting a single item can be done by calling :py:meth:`~dynamongo.model.Model.get_one` method.
This method raises :py:exc:`Exception` if an item by the given strategy does not exists.

This method takes in ``strategy`` as input.
``strategy`` can be either of the following:


1. **The primary key value**.

If a model has a `hash_key` only, this is passed in as a scalar.
Otherwise, if the model has both `hash_key` and `range_key`,
the value is passed as a ``(hash_key, range_key)`` tuple.

.. code-block:: python

    user = User.get_one('johndoe@gmail.com')

2. **Dict object**

The dict should contain all primary key values.
i.e, if the model has both `hash_key` and `range_key`, both should be included in the dict.
Otherwise only a dict with the ``hash_key`` is required.

Non primary key items in the dict are ignored.

.. code-block:: python

    user = User.get_one({'email': 'johndoe@gmail.com'})


3. **Model instance**

The primary fields attributes must have valid values. Item is selected by the primary keys.

.. code-block:: python

    user = User.get_one(User(email='johndoe@gmail.com'))


4. **Key condition**

In its simplest form, if the model does not have a `range_key`,
this should be an equality condition on the hash_key field.

if the model has both `hash_key` and `range_key`,
this should be two equality conditions on both key fields `ANDed` together.

.. code-block:: python

    user = User.get_one(User.email == 'johndoe@gmail.com')


.. _doc_get_many:

Getting multiple items
**********************

Multiple items can be fetched by calling :py:meth:`~dynamongo.model.Model.get_many` method.
This method takes in ``strategy`` as input. ``strategy`` can be either of the following:

1. **List**

Each entry in this list must be a valid object that can be passed to the :py:meth:`~dynamongo.model.Model.get_one` method
as described above.

Examples

.. code-block:: python

    users = User.get_many([
        'johndoe@gmail.com',
        'email1@gmail.com',
        {'email': 'email2@gmail.com'},
        User(email='email3@gmail.com'),
        User.email == 'email4@gmail.com'
    ])

2. **Condition**

Here you can pass any valid condition. Suppose we have list of user emails:

.. code-block:: python

     emails = ['johndoe@gmail.com', 'email2@abc.io', 'anotherone@xyz.com']

Example #1. Finding users by their email address, can be achieved in either of the following ways

.. code-block:: python

    # simply passing in the list of emails
    users = User.get_many(emails)

.. code-block:: python

    # more control. We know exactly what emails is
    users = User.get_many(User.email.in_(emails))

.. code-block:: python

    # Useful when using composite primary keys
    users = User.get_many(User.keys_in(emails))


Example #2. Only get users in the list, but only if the user was born on or before the year ``2000``

.. code-block:: python

    users = User.get_many(
                (User.email.in_(emails)) &
                (User.year_of_birth > 2000)
            )

Example #3. Get all users who have ever visited ``Nairobi`` city

.. code-block:: python

    users = User.get_many(User.cities_visited.contains('Nairobi'))

Example #4. Get all user who were born before ``1990`` and have never visited ``Nairobi``.

.. code-block:: python

    users = User.get_many(
                (User.year_of_birth < 1990) &
                (not User.cities_visited.contains('Nairobi'))
            )


