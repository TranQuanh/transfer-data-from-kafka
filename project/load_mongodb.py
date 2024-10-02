import consumer
import config

# config
#determine which source to load data into
destination = "mongodb"

#config and create consumer
conf = config.consumer_conf()
topics = ['product_view']
one_consumer = consumer.one_consumer_init(topics,conf)

#
try:
    consumer.consume_loop(one_consumer,destination)
except KeyboardInterrupt:
    print("-------------- Over -------------------")
# message = {"1":"haha"}
# consumer.msg_process(message,destination)
