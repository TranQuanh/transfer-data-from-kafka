import json
import confluent_kafka
from confluent_kafka import Consumer
import socket
from confluent_kafka import KafkaError
import time
import sys
import logging
import config
from common_producer import mongodb
from common_producer import load_producer
from confluent_kafka import  TopicPartition
import multiprocessing
# Thiết lập logging
logging.basicConfig(filename='kafka_errors.log', level=logging.ERROR,
                    format='%(asctime)s - %(levelname)s - %(message)s')

# stop the program
def test(msg_json):
    print(msg_json)
    time.sleep(20)
def shutdown():
    running = False


def msg_process(msg,destination):
    try:
        # Chuyển đổi msg.value() (là chuỗi byte) sang chuỗi string
        msg_value = msg.value().decode('utf-8')

        # Chuyển đổi chuỗi string sang đối tượng JSON
        msg_json = json.loads(msg_value)

        func = globals()[destination]
        func(msg_json)
    except json.JSONDecodeError as e:
        # Xử lý lỗi nếu dữ liệu không phải JSON hợp lệ
        print(f"Failed to decode JSON: {e}")

running =True
MIN_COMMIT_COUNT = 500
def consume_loop(consumer_id,topics,conf,destination):
    try:
        consumer  = one_consumer_init(topics,conf)
        msg_count = 0
        while running:
            msgs = consumer.consume(10, timeout=10) #milisecond
            # msg = consumer.poll(timeout = 100.0)
            for msg in msgs:
                if msg is None: continue

                if msg.error():
                    if msg.error().code() == KafkaError._PARTITION_EOF:
                        #End of partition event
                        sys.stderr.write('%% %s [%d] reached end at offset %d\n' %
                                        (msg.topic(), msg.partition(), msg.offset()))
                        continue
                    elif msg.error():
                        logging.error(f"Kafka Error: {msg.error()}")

                else:
                    # print(msg.value())
                    msg_process(msg,destination)
                    print(f"consumer hiện tại :{consumer_id} partition hiện tại: {msg.partition()} . offset của message hiện tại:{msg.offset()}")
                    msg_count += 1
                    print(f"số lượng message:{msg_count}")
                    if msg_count % MIN_COMMIT_COUNT == 0:
                        consumer.commit(asynchronous=False)   
    finally:
        consumer.close()
# create 1 consumer consume all partition

# check commit offset in consumer group
def check_offsets(conf, topic,topics):
    consumer = one_consumer_init(topics,conf)
    # Tạo danh sách TopicPartition cho tất cả các partition của topic
    partitions = [TopicPartition(topic, i) for i in range(3)]  # Giả sử topic có 3 partition
    # Lấy committed offset cho tất cả các partition
    committed_offsets = consumer.committed(partitions)

    # In ra committed offset cho từng partition
    for partition, committed_offset in zip(partitions, committed_offsets):
        if committed_offset is not None:
            print(f"Partition {partition.partition}: Offset đã tiêu thụ: {committed_offset.offset}")
        else:
            print(f"Partition {partition.partition}: Chưa có offset nào được tiêu thụ.")

# list all partition in topic that kafka is manage
def partition(id,consumer):
    for i in range(10):
        msg = consumer.poll(1.0)
        time.sleep(2) 
    time.sleep(2) 
    partitions_consumer = consumer.assignment() # Chuyển thành chuỗi để in ra
    print(f"Consumer {id} is managing partitions: {len(partitions_consumer)}")
# create one consumer
def one_consumer_init(topics,conf):
    consumer = Consumer(**conf)
    consumer.subscribe(topics)
    return consumer

#  kiểm tra consumer hiện tại đang chạy partition nào
def check_consumer_manage_partition(id,conf,topics):
    consumer = one_consumer_init(topics,conf)
    partition(id,consumer)

# run multiprocess
def multiprocess(count_consumer,topics,conf,destination):
    if count_consumer >3:
        print("just have 3 broker")
    else:
        print(f"Using {count_consumer} consumer to consume topic in kafka")
        process = []
        # tạo process và start các process
        for i in range(0,count_consumer):
            # p =multiprocessing.Process(target =consume_loop,args =(i,topics,conf,destination))

            # topic = topics[0]
            # p =multiprocessing.Process(target =check_offsets,args =(conf,topic, topics))
            topic = topics[0]
            p = multiprocessing.Process(target= check_consumer_manage_partition,args= (i,conf,topics))
            process.append(p)
            p.start()
        
        #chạy sau p.join() của tất các process thì mới chạy tiếp
        for p in process:
            p.join()

        print("chạy liên lục nên không vào dòng này được đâu ><")
######################################
# main code
######################################

# partition(consumer,'product_view')
# conf = config.consumer_conf()
# topics = ['product_view']
# topic = 'product_view'
# consumer = one_consumer_init(topics,conf)
# check_offsets(consumer,topic)
# time.sleep(1)
# consume_loop(consumer,"test")

