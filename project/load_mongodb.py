import consumer
import config

# config
destination = "mongodb"
conf = config.consumer_conf()
topics = ['product_view']


one_consumer = consumer.one_consumer_init(topics,conf)
consumer.consume_loop(one_consumer,destination)
# message = {"1":"haha"}
# consumer.msg_process(message,destination)
