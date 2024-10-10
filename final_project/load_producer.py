from Consumer import ConsumerClass
from Producer import ProducerClass
import time
import multiprocessing
# config consumer
bootstrap_server = '113.160.15.232:9094,113.160.15.232:9194,113.160.15.232:9294'
group_id = 'final'
topics = ['product_view']

#config producer
bootstrap_server = 'localhost:9094'
topics = ['final']
producer = ProducerClass(bootstrap_server,topics)

# push data to producer
MIN_FLUSH_COUNT = 500
producer_msg_count =0
def producer_process(msg_json):
    global producer_msg_count
    producer_msg_count +=1
    producer.send_message(msg_json)
    if(producer_msg_count%MIN_FLUSH_COUNT==0):
        producer.commit()


running = True #the program always run
MIN_COMMIT_COUNT = 500 
def consume_process(consumer_id):
    # create consumer
    consumer = ConsumerClass(bootstrap_server,group_id,topics)
    try:
        msg_count = 0
        while running:
            msgs = consumer.consume_message()
            
            for msg in msgs:
                if msg is None : continue
                else:
                    producer_process(msg)
                    msg_count+=1
                    print(f"số lượng message:{msg_count} của consumer {consumer_id}")
                    if msg_count % MIN_COMMIT_COUNT == 0:
                        consumer.commit()
    except KeyboardInterrupt:
        print(f"Process {consumer_id}: KeyboardInterrupt caught, flushing producer")
        producer.commit()
    finally:
        print(f"Process {consumer_id}: Flushing the rest of the messages in buffer")
        producer.commit()
        print(f"Process {consumer_id}: Over")
        consumer.close()
# check partition in each consumer
def  check_partition_in_consumer(consumer_id):
    consumer = ConsumerClass(bootstrap_server,group_id,topics)
    consumer.len_partition_in_consumer()

# create number of partiton to process parallel
def multiprocess(count_consumer):
    if count_consumer >3:
        print("Just have 3 broker")
    else:
        print(f"Using {count_consumer} consumer to consume topic in kafka")
        process = []
        # create process and start it
        for i in range(0,count_consumer):
            p = multiprocessing.Process(target = consume_process,args = (i,))
            # p = multiprocessing.Process(target = check_partition_in_consumer,args = (i,))
            process.append(p)
            p.start()
        for p in process:
            p.join()

        print("Can't run to this cause it running forever ><")
if __name__ =="__main__":
    time.sleep(5)
    try:
        multiprocess(2)
    except KeyboardInterrupt:
        print('---------------------- Over --------------------')
    producer.commit()