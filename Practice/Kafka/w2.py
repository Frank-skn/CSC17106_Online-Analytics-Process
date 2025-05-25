from confluent_kafka import Producer

# Hàm callback khi tin nhắn được gửi thành công hoặc thất bại
def acked(err, msg):
    if err is not None:
        print('Error while producing message: {}'.format(err))
    else:
        print('Message produced: {}'.format(msg))

# Cấu hình Kafka producer
conf = {
    'bootstrap.servers': 'kafka:9092',  # Địa chỉ Kafka broker (dùng 'kafka' thay vì 'localhost' khi chạy trong container Docker)
}

# Tạo producer
producer = Producer(conf)

# Gửi một tin nhắn vào topic 'test-topic'
producer.produce('test-topic', key='key', value='Hello Kafka!', callback=acked)

# Đảm bảo rằng tất cả các tin nhắn đã được gửi trước khi dừng producer
producer.flush()
