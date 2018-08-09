"""Examples"""
import datetime
from dynamongo import BaseModel, Connection, GlobalIndex
from dynamongo import Integer, String, List, Email, DateTime, UUID


class MyBaseModel(BaseModel):
    __connection__ = Connection(
        access_key_id='<KEY>',
        secret_access_key='<SECRET>',
        region='<REGION_NAME>',
        table_prefix='test-'
    )

    class Meta:
        abstract = True


class SimpleKey(MyBaseModel):
    number = Integer(min_value=1, hash_key=True)
    uuid = UUID(keep_dashes=False, required=True)
    name = String(required=True)
    email = Email()


class Contact(MyBaseModel):
    user_id = Integer(min_value=1, hash_key=True)
    email = Email(range_key=True)
    created_at = DateTime(default=datetime.datetime.now)


class WithIndex(MyBaseModel):
    id = Integer(min_value=1, hash_key=True)
    email = Email(required=True)
    uuid = UUID(keep_dashes=False)
    name = String()
    by_uuid_and_name = GlobalIndex(uuid, range_key=name, projection=GlobalIndex.PROJECTION_KEYS_ONLY)


class User(MyBaseModel):
    email = Email(required=True, hash_key=True)
    name = String(required=True)
    year_of_birth = Integer(max_value=2018, min_value=1900)
    cities_visited = List(String)
    created_at = DateTime(default=datetime.datetime.now)


MyBaseModel.__bind__()

################################################################################
# save a single user by passing data as a dict
john = User.save({
    'email': 'johndoe@gmail.com',
    'name': 'John Doe',
    'year_of_birth': 1990,
    'cities_visited': ['Nairobi', 'New York']
})

# save a single user by passing data as a User instance
user = User(
    email='johndoe@gmail.com',
    name='John Doe',
    cities_visited=[]
)
user.year_of_birth = 1990
user.cities_visited = ['Nairobi', 'New York']
user = User.save(user)

# Save multiple users at once
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
users = User.save_many(*user_list)

#########################################################################
# delete a single user
User.delete('johndoe@gmail.com')
User.delete({'email': 'johndoe@gmail.com'})
User.delete(User(email='johndoe@gmail.com'))
User.delete(User.email == 'johndoe@gmail.com')
User.delete('johndoe@gmail.com', condition=User.year_of_birth <= 2000)
User.delete(User.email == 'johndoe@gmail.com', condition=User.cities_visited.contains('Nairobi'))
User.delete(
    User.email == 'johndoe@gmail.com',
    condition=(User.year_of_birth > 2000) | User.cities_visited.contains('Nairobi')
)

# delete multiple users
emails = ['johndoe@gmail.com', 'email2@abc.io', 'anotherone@xyz.com']
User.delete_many(*emails)
User.delete_many(
    'email1@gmail.com',
    {'email': 'email2@gmail.com'},
    User(email='email3@gmail.com'),
    User.email == 'email4@gmail.com',
)

################################################################################
# Fetch a single user
User.get('johndoe@gmail.com')
User.get({'email': 'johndoe@gmail.com'})
User.get(User.email == 'johndoe@gmail.com')

# fetch multiple users
User.get_many(*emails)
User.query('johndoe@gmail.com', filter=User.year_of_birth > 2000)
User.scan(User.cities_visited.contains('Nairobi'))

