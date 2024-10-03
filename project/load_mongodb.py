import consumer
import config


if __name__ == "__main__":
    # config
    #determine which source to load data into
    destination = "mongodb"

    #config and create consumer
    conf = config.consumer_conf()
    topics = ['product_view']
    # one_consumer = consumer.one_consumer_init(topics,conf)

    # try:
    #     consumer.consume_loop(1,topics,conf,destination)
    # except KeyboardInterrupt:
    #     print("-------------- Over -------------------")

    # count_consumer = int(input('Consumer amount you want: '))


    try:
        consumer.multiprocess(2,topics,conf,destination)
    except KeyboardInterrupt:
        print("-------------- Over -------------------")
    # message = {"1":"haha"}
    # consumer.msg_process(message,destination)
