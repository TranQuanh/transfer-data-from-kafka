import consumer
import config
#determine which source to load data into
destination = "load_producer"

#config consumer and create consumer
conf = config.consumer_conf()
topics = ['product_view']
one_consumer = consumer.one_consumer_init(topics,conf)

try:
    consumer.consume_loop(one_consumer,destination)
except KeyboardInterrupt:
    print("\n-------------------Over -------------------")