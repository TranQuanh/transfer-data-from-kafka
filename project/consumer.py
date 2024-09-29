import json
import confluent_kafka
from confluent_kafka import Consumer
import socket
from confluent_kafka import KafkaError
import time
import sys
import logging
import config
# Thiết lập logging
logging.basicConfig(filename='kafka_errors.log', level=logging.ERROR,
                    format='%(asctime)s - %(levelname)s - %(message)s')

 
def shutdown():
    running = False
    
def partition(consumer,topic_name):
    metadata = consumer.list_topics(topic_name)
    # In ra số lượng partition
    if metadata and metadata.topics:
        topic = metadata.topics[topic_name]
        print(f'Topic: {topic.topic}, Number of partitions: {len(topic.partitions)}')

def msg_process(msg):
    try:
        # Chuyển đổi msg.value() (là chuỗi byte) sang chuỗi string
        msg_value = msg.value().decode('utf-8')

        # Chuyển đổi chuỗi string sang đối tượng JSON
        msg_json = json.loads(msg_value)

        print(msg_json)

        time.sleep(15)
    except json.JSONDecodeError as e:
        # Xử lý lỗi nếu dữ liệu không phải JSON hợp lệ
        print(f"Failed to decode JSON: {e}")

running =True
MIN_COMMIT_COUNT = 500
def consume_loop(consumer):
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
                elif msg.error():
                    logging.error(f"Kafka Error: {msg.error()}")
            else:
                msg_process(msg)
                msg_count += 1
                if msg_count % MIN_COMMIT_COUNT == 0:
                    consumer.commit(asynchronous=False)

        
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
conf = config.conf()
topics = ['product_view']
consumer = one_consumer_init(topics,conf)
consume_loop(consumer)


# consumer.subscribe(['product_view'])
# try:
#     while True:
#         msg = consumer.poll(1.0)  # Poll for new messages (timeout 1 second)

#         if msg is None:
#             continue
#         if msg.error():
#             if msg.error().code() == KafkaError._PARTITION_EOF:
#                 continue
#             else:
#                 print(msg.error())
#                 break

#         # Print the received message
#         json_data = json.loads(msg.value().decode('utf-8'))
#         print(json_data)
#         time.sleep(10)
# except KeyboardInterrupt:
#     pass
# finally:
#     # Close down consumer to commit final offsets.
#     consumer.close()
