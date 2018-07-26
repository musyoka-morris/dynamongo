# Created by morris (musyokamorris@gmail.com)
#
# Project   : dynamongo
# Date      : 2018/07/22

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


# select multiple contacts for different users when you have a list of (user_id, email) tuples
keys = [('user_id_1', 'john@gmail.com'), ('user_id_2', 'doe@gmail.com')]
contacts = Contacts.get_many(
    Contacts.keys_in(keys)
)

