from confluent_kafka import Producer
from confluent_kafka import Consumer
import socket
import time
conf = {
    'bootstrap.servers': 'localhost:9094',
    'security.protocol': 'SASL_PLAINTEXT',
    'sasl.mechanism': 'PLAIN',
    'sasl.username': 'admin',
    'sasl.password': 'Unigap@2024',
    'client.id' : socket.gethostname() 
}
producer = Producer(**conf)

# Gửi message đến topic
topics = ['topic','topic2']
while 1:
    # Produce message
    message = "test"
    for topic in topics:
        producer.produce(topic, key = "123",value=message)
        producer.flush()  # Đảm bảo gửi hết message trước khi thoát
        time.sleep(0.5)
        print("Message sent successfully in "+topic)