import config
import consumer
from confluent_kafka import Consumer
import time
topic ='product_view'
topics = ['product_view']
conf = config.consumer_conf()
consumer1 = Consumer(**conf)
consumer1.subscribe(topics)
consumer2 = Consumer(**conf)
consumer2.subscribe(topics)
# consumer.partition(consumer1,topic)
# consumer.partition(consumer2,topic)

# Dùng poll() để giúp consumer tham gia vào nhóm và nhận partition
for i in range(10):
    msg = consumer1.poll(1.0)
    msg = consumer2.poll(1.0)
    time.sleep(5)  # Chờ thêm một chút để đảm bảo Kafka đã assign partitions

# Kiểm tra partitions được phân công cho consumer1
partitions_consumer1 = consumer1.assignment() # Chuyển thành chuỗi để in ra
print(f"Consumer 1 is managing partitions: {len(partitions_consumer1)}")

# Kiểm tra partitions được phân công cho consumer2
partitions_consumer2 = consumer2.assignment()
print(f"Consumer 2 is managing partitions: {len(partitions_consumer2)}")