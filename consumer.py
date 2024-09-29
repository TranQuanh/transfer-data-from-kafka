from confluent_kafka import Consumer
import socket
import sys
from confluent_kafka import KafkaError
from confluent_kafka import KafkaException
conf = {
    'bootstrap.servers': 'localhost:9094',
    'security.protocol': 'SASL_PLAINTEXT',
    'sasl.mechanism': 'PLAIN',
    'sasl.username': 'admin',
    'sasl.password': 'Unigap@2024',
    'client.id' : socket.gethostname() ,
    'group.id': 'foo',
    'auto.offset.reset': 'smallest'
}
consumer = Consumer(**conf)
running = True

def basic_consume_loop(consumer, topics):
    try:
        consumer.subscribe(topics)

        while running:
            msg = consumer.poll(timeout=1.0)
            if msg is None: continue

            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    # End of partition event
                    sys.stderr.write('%% %s [%d] reached end at offset %d\n' %
                                     (msg.topic(), msg.partition(), msg.offset()))
                elif msg.error():
                    raise KafkaException(msg.error())
            else:
                print(msg.value())
    finally:
        # Close down consumer to commit final offsets.
        consumer.close()

basic_consume_loop(consumer,['topic'])
# consumer.subscribe(['topic'])
# msg = consumer.poll(timeout=1.0)
# print(msg)