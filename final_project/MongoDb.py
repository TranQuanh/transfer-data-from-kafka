import os
from dotenv import load_dotenv
from pymongo import MongoClient
from pymongo.errors import BulkWriteError
class MongoClass:
    def __init__(self):
        mongo_uri = os.getenv('MONGO_URI')
        mongo_db_name = os.getenv('MONGO_DB_NAME')
        mongo_collection = os.getenv('MONGO_COLLECTION')
        
        client = MongoClient(mongo_uri)
        db = client[mongo_db_name]
        collection = db[mongo_collection]
        self.collection =  collection

    def send_message(self,msg_json_list):
        try:
            self.collection.insert_many(msg_json_list, ordered=False)
            print("Data loaded successfully")
        except BulkWriteError as bwe:
            # Hiển thị các lỗi về trùng lặp hoặc lỗi khác nếu có
            for error in bwe.details['writeErrors']:
                print(f"Document with _id {error['op']['_id']} already exists. Skipping.")