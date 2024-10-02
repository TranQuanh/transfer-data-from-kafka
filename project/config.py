import socket
from pymongo import MongoClient
from dotenv import load_dotenv
import os

# Tải các biến từ file .env
load_dotenv()

# setup config for consumer
def consumer_conf():
    conf = {
        'bootstrap.servers': os.getenv('BOOTSTRAP_SERVERS'),
        'security.protocol': os.getenv('SECURITY_PROTOCOL'),
        'sasl.mechanism': os.getenv('SASL_MECHANISM'),
        'sasl.username': os.getenv('SASL_USERNAME'),
        'sasl.password': os.getenv('SASL_PASSWORD'),
        'client.id': socket.gethostname(),
        # # 'group.id': 'behaviour', #load mongodb - 1 consumer
        # 'group.id': 'behaviour_topic', #load consumer - 1 consumer
        'group.id': os.getenv('GROUP_ID'),
        'enable.auto.commit': False,
        'auto.offset.reset': os.getenv('AUTO_OFFSET_RESET', 'earliest')
    }
    return conf

# set up config for producer in local kafka docker
def producer_conf():
    conf = {
        'bootstrap.servers': os.getenv('PRODUCER_BOOTSTRAP_SERVERS'),
        'security.protocol': os.getenv('PRODUCER_SECURITY_PROTOCOL'),
        'sasl.mechanism': os.getenv('PRODUCER_SASL_MECHANISM'),
        'sasl.username': os.getenv('PRODUCER_SASL_USERNAME'),
        'sasl.password': os.getenv('PRODUCER_SASL_PASSWORD'),
        'client.id': socket.gethostname(),
        'acks': 'all',
        'enable.idempotence': True,
        'retries': 5,
    }
    return conf

# set up config for local mongodb
def mongo_conf():
    mongo_uri = os.getenv('MONGO_URI')
    mongo_db_name = os.getenv('MONGO_DB_NAME')
    mongo_collection = os.getenv('MONGO_COLLECTION')
    
    client = MongoClient(mongo_uri)
    db = client[mongo_db_name]
    collection = db[mongo_collection]
    return collection