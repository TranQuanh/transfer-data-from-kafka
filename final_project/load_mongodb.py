from Consumer import ConsumerClass
from MongoDb import MongoClass
import time
import multiprocessing
running = True #the program always run
MIN_COMMIT_COUNT = 500 
# config mongo
mongo = MongoClass()
# config consumer
bootstrap_server = '113.160.15.232:9094,113.160.15.232:9194,113.160.15.232:9294'
group_id = 'final'
topics = ['product_view']

msg_json_list = []
message_count = 0
def mongo_process(msg_json):
    global message_count,msg_json_list
    msg_json_list.append(msg_json)
    message_count+=1
    if(message_count%MIN_COMMIT_COUNT):
        mongo.send_message(msg_json_list)
        msg_json_list = []
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
                    mongo_process(msg)
                    msg_count+=1
                    print(f"số lượng message:{msg_count} của consumer {consumer_id}")
                    if msg_count % MIN_COMMIT_COUNT == 0:
                        consumer.commit()
    except KeyboardInterrupt:
        print(f"Process {consumer_id}: KeyboardInterrupt caught")
    finally:
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
    try:
        multiprocess(2)
    except KeyboardInterrupt:
        print('---------------------- Over --------------------')