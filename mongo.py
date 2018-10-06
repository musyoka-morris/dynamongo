# Created by morris (musyokamorris@gmail.com)
#
# Project   : dynamongo
# Date      : 2018/08/09
# import json
# from pymongo import MongoClient
# import datetime
#
#
# def uuid():
#     from uuid import uuid4
#     return uuid4().hex
#
#
# users = MongoClient()['test-database']['test-collection']
#
# result = users.insert_one({
#     '_id': uuid(),
#     'name': 'Another One',
#     'email': 'morris@aims.ac.za',
#     'index': 7,
#     'created_at': datetime.datetime.now()
# })
#
# res = users.find_one({'index': 7})
# l = json.dumps(res)

d = dict(a=2)
del d['abx']