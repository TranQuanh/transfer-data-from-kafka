import config
from pymongo.errors import DuplicateKeyError
from pymongo.errors import BulkWriteError
from confluent_kafka import Producer
import time
import json
# mongodb config, create mongo connect
collection = config.mongo_conf()
#producer config, create producer
conf  = config.producer_conf()
producer = Producer(**conf)
topic = 'load-product-view'
#load json into mongo
msg_json_list=[]
MIN_LOAD_MONGO = 500
message_count = 0
def mongodb(msg_json):
    global message_count,msg_json_list
    msg_json_list.append(msg_json)
    message_count+=1
    if(message_count%MIN_LOAD_MONGO==0):
        try:
            collection.insert_many(msg_json_list, ordered=False)
            print("Data loaded successfully")
            msg_json_list = []
        except BulkWriteError as bwe:
            # Hiển thị các lỗi về trùng lặp hoặc lỗi khác nếu có
            for error in bwe.details['writeErrors']:
                print(f"Document with _id {error['op']['_id']} already exists. Skipping.")

    # try:
    #     collection.insert_one(msg_json) 
    #     print("load data successfully")
    # except DuplicateKeyError:
    #     print(f"Document with _id {msg_json['_id']} already exists. Skipping.")

#load json into topic
def acked(err, msg):
    if err is not None:
        # print("Failed to deliver message: %s: %s" % (str(msg), str(err)))
        print("Failed to deliver message: %s" % str(err))
    else:
        # print("Message produced: %s" % (str(msg)))
        pass
check=0
def load_producer(msg_json):
    global check
    # Chuyển đổi đối tượng JSON thành chuỗi
    json_data = json.dumps(msg_json)

    # Mã hóa chuỗi JSON thành bytes
    # msg_bytes = json.encode('utf-8')
    producer.produce(topic, value=json_data, callback=acked)
    ("load data successfully")
    check+=1
    if check %50000:
        producer.flush()
