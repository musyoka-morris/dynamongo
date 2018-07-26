********************************************
dynamongo: Pythonic DynamoDB models
********************************************

**dynamongo** is Python ORM/framework-agnostic library for DynamoDB.
It is highly inspired by the PyMongo project.
This documentation attempts to explain everything you need to know to use dynamongo.

.. code-block:: python

    import datetime
    from dynamongo import Model, Connection
    from dynamongo import IntField, StringField, ListField, EmailField, DateTimeField

    # This only need be called once. Alternatively, it can be set using env variables
    Connection.set_config(
        access_key_id='<KEY>',
        secret_access_key='<SECRET>',
        table_prefix='test-'
    )


    class User(Model):
        __table__ = 'users'
        __hash_key__ = 'email'

        email = EmailField(required=True)
        name = StringField(required=True)
        year_of_birth = IntField(max_value=2018, min_value=1900)
        cities_visited = ListField(StringField)
        created_at = DateTimeField(default=datetime.datetime.now)


    # store data to DynamoDB
    john = User.save_one({
        'email': 'johndoe@gmail.com',
        'name': 'John Doe',
        'year_of_birth': 1990,
        'cities_visited': ['Nairobi', 'New York']
    })

    # year_of_birth, cities_visited & created_at are all optional
    jane = User.save_one({
        'email': 'jane@gmail.com',
        'name': 'Jane Doe'
    })

    # Access attribute values
    print(john.name)

    # Fetch data from dynamoDB
    user = User.get_one(User.email == 'johndoe@gmail.com')
    print(user.to_dict())


In short, dynamongo models can be used to easily:

- **validate** input data
- **save** serialized data to DynamoDB
- **read** and deserialize data from DynamoDB
- **delete** items from DynamoDB
- **update** data in DynamoDB


Get It Now
==========

::

    $ pip install dynamongo


Documentation
=============

Full documentation is available at http://dynamongo.readthedocs.io/ .

Requirements
============

- Python >= 3.5
