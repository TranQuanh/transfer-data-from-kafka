import config
from pymongo.errors import DuplicateKeyError
collection = config.mongo_conf()
def mongodb(msg_json):
    try:
        collection.insert_one(msg_json) 
        print("load data successfully")
    except DuplicateKeyError:
        print(f"Document with _id {msg_json['_id']} already exists. Skipping.")