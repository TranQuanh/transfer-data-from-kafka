import consumer
import config
if __name__ == "__main__":
    #determine which source to load data into
    destination = "load_producer"

    #config consumer and create consumer
    conf = config.consumer_conf()
    topics = ['product_view']
    one_consumer = consumer.one_consumer_init(topics,conf)

    try:
        consumer.multiprocess(2,topics,conf,destination)
    except KeyboardInterrupt:
        print("-------------- Over -------------------")