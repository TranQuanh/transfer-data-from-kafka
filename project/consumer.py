import json
import confluent_kafka
from confluent_kafka import Consumer
import socket
from confluent_kafka import KafkaError
import time
import sys
import logging
import config
from common_consumer import mongodb
# Thiết lập logging
logging.basicConfig(filename='kafka_errors.log', level=logging.ERROR,
                    format='%(asctime)s - %(levelname)s - %(message)s')

# stop the program
def test(msg_json):
    print(msg_json)
    time.sleep(20)
def shutdown():
    running = False

# list all partition in topic that kafka is manage
def partition(consumer,topic_name):
    metadata = consumer.list_topics(topic_name)
    # In ra số lượng partition
    if metadata and metadata.topics:
        topic = metadata.topics[topic_name]
        print(f'Topic: {topic.topic}, Number of partitions: {len(topic.partitions)}')

def msg_process(msg,destination):
    try:
        # Chuyển đổi msg.value() (là chuỗi byte) sang chuỗi string
        msg_value = msg.value().decode('utf-8')

        # Chuyển đổi chuỗi string sang đối tượng JSON
        msg_json = json.loads(msg_value)

        if destination in globals():
            func = globals()[destination]
            func(msg_json)
        else:
            print("Không có hàm")
    except json.JSONDecodeError as e:
        # Xử lý lỗi nếu dữ liệu không phải JSON hợp lệ
        print(f"Failed to decode JSON: {e}")

running =True
MIN_COMMIT_COUNT = 500
def consume_loop(consumer,destination):
    try:

        msg_count = 0
        while running:
            msg = consumer.poll(timeout=1.0)
            if msg is None: continue

            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    #End of partition event
                    sys.stderr.write('%% %s [%d] reached end at offset %d\n' %
                                     (msg.topic(), msg.partition(), msg.offset()))
                    break
                elif msg.error():
                    logging.error(f"Kafka Error: {msg.error()}")
            else:
                msg_process(msg,destination)
                print(f"partition hiện tại: {msg.partition()} . offset của message hiện tại:{msg.offset()}")
                msg_count += 1
                print(f"số lượng message:{msg_count}")
                if msg_count % MIN_COMMIT_COUNT == 0:
                    consumer.commit(asynchronous=False)   
                    time.sleep(90)
    finally:
        consumer.close()

def one_consumer_init(topics,conf):
    consumer = Consumer(**conf)
    consumer.subscribe(topics)
    return consumer

######################################
# main code
######################################

# partition(consumer,'product_view')
# conf = config.consumer_conf()
# topics = ['product_view']
# consumer = one_consumer_init(topics,conf)
# consume_loop(consumer,"test")
