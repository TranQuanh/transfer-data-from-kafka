from confluent_kafka import Producer
from dotenv import load_dotenv
import os
import socket
import json
def acked(err, msg):
        if err is not None:
            # print("Failed to deliver message: %s: %s" % (str(msg), str(err)))
            print("Failed to deliver message: %s" % str(err))
        else:
            # print("Message produced: %s" % (str(msg)))
            ("load data successfully")
class ProducerClass:
    def __init__(self,bootstrap_server,topics):
        self.bootstrap_server = bootstrap_server
        self.topics = topics
        self.topic = topics[0]
        conf = {
        'bootstrap.servers': bootstrap_server,    
        # 'bootstrap.servers': os.getenv('PRODUCER_BOOTSTRAP_SERVERS'),
        'security.protocol': os.getenv('PRODUCER_SECURITY_PROTOCOL'),
        'sasl.mechanism': os.getenv('PRODUCER_SASL_MECHANISM'),
        'sasl.username': os.getenv('PRODUCER_SASL_USERNAME'),
        'sasl.password': os.getenv('PRODUCER_SASL_PASSWORD'),
        'client.id': socket.gethostname(),
        'acks': 'all',
        'enable.idempotence': True,
        'retries': 5,
        }
        self.producer = Producer(**conf)
    def send_message(self,msg_json):
        json_data = json.dumps(msg_json)
        self.producer.produce(self.topic,value = json_data,callback = acked)
        ("load data")
    def commit(self):
        self.producer.flush()