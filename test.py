"""Examples"""
import datetime
from dynamongo import Model, Connection
from dynamongo import IntField, StringField, ListField, EmailField, DateTimeField

# This only need be called once. Alternatively, it can be set using env variables
Connection.set_config(
    access_key_id='<KEY>',
    secret_access_key='<SECRET>',
    region='<REGION_NAME>',
    table_prefix='test-'
)

#######################################################################################
class User(Model):
    __table__ = 'users'
    __hash_key__ = 'email'

    email = EmailField(required=True)
    name = StringField(required=True)
    year_of_birth = IntField(max_value=2018, min_value=1900)
    cities_visited = ListField(StringField)
    created_at = DateTimeField(default=datetime.datetime.now)


################################################################################
# save a single user by passing data as a dict
john = User.save_one({
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
user = User.save_one(user)

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
result = User.save_many(user_list, overwrite=False)
print(result.fail_count)

#########################################################################
# delete a single user
User.delete_one('johndoe@gmail.com')
User.delete_one({'email': 'johndoe@gmail.com'})
User.delete_one(User(email='johndoe@gmail.com'))
User.delete_one(User.email == 'johndoe@gmail.com')
User.delete_one((User.email == 'johndoe@gmail.com') & (User.year_of_birth <= 2000))
User.delete_one((User.email == 'johndoe@gmail.com') & (User.cities_visited.contains('Nairobi')))
User.delete_one(
    (User.email == 'johndoe@gmail.com') &
    ((User.year_of_birth > 2000) | User.cities_visited.contains('Nairobi'))
)

# delete multiple users
emails = ['johndoe@gmail.com', 'email2@abc.io', 'anotherone@xyz.com']
User.delete_many(emails)
User.delete_many(User.email.in_(emails))
User.delete_many(User.keys_in(emails))
User.delete_many((User.email.in_(emails)) & (User.year_of_birth > 2000))

User.delete_many(User.cities_visited.contains('Nairobi'))
User.delete_many((User.year_of_birth < 1990) & (not User.cities_visited.contains('Nairobi')))
User.delete_many([
    'email1@gmail.com',
    {'email': 'email2@gmail.com'},
    User(email='email3@gmail.com'),
    User.email == 'email4@gmail.com',
    (User.email == 'email5@gmail.com') & (User.year_of_birth <= 2000)
])

################################################################################
# Fetch a single user
User.get_one('johndoe@gmail.com')
User.get_one({'email': 'johndoe@gmail.com'})
User.get_one(User.email == 'johndoe@gmail.com')

# fetch multiple users
User.get_many(emails)
User.get_many(User.email.in_(emails))
User.get_many(User.keys_in(emails))
User.get_many((User.email.in_(emails)) & (User.year_of_birth > 2000))
User.get_many(User.cities_visited.contains('Nairobi'))

