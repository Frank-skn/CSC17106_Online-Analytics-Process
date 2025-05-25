##________________________Bài 1_______________________________
#			network_producer.py
from confluent_kafka import Producer
import json
import random
import time

conf = {'bootstrap.servers': 'localhost:9092'}
producer = Producer(conf)

devices = ['server-1', 'router-1', 'switch-1']

def delivery_report(err, msg):
    if err:
        print(f"❌ Delivery failed: {err}")
    else:
        print(f"✅ Delivered to {msg.topic()} [{msg.partition()}]")

while True:
    network_message = {
        'device': random.choice(devices),
        'status': random.choice(['Online', 'Offline']),
        'timestamp': time.time()
    }
    producer.produce(
        topic='network-status',
        value=json.dumps(network_message),
        callback=delivery_report
    )
    print(f"📡 Sent network status: {network_message}")

    producer.poll(0)
    time.sleep(2)
#			network_consumer.py
from confluent_kafka import Consumer, KafkaError
import json

common_conf = {
    'bootstrap.servers': 'localhost:9092',
    'group.id': 'network-consumer-group',
    'auto.offset.reset': 'earliest'
}

consumer = Consumer(common_conf)
consumer.subscribe(['network-status'])

device_status = {}

try:
    while True:
        msg = consumer.poll(1.0)
        if msg is None:
            continue
        if msg.error():
            if msg.error().code() != KafkaError._PARTITION_EOF:
                print(f"Consumer error: {msg.error()}")
            continue

        value = msg.value()
        if not value:
            print(f"Empty message on topic {msg.topic()}")
            continue

        try:
            data = json.loads(value.decode('utf-8'))
            device = data['device']
            status = data['status']
            device_status[device] = status
            print(f"🔌 [{device}] status [{status}]")
        except json.JSONDecodeError:
            print(f"Invalid JSON: {value}")

except KeyboardInterrupt:
    print('Interrupted')

finally:
    consumer.close()



##________________________Bài 2_______________________________
#			app_log_producer.py
from confluent_kafka import Producer
import json
import random
import time

conf = {'bootstrap.servers': 'localhost:9092'}
producer = Producer(conf)

log_levels = ['INFO', 'ERROR', 'DEBUG']
services = ['auth-service', 'payment-service', 'user-service']

def delivery_report(err, msg):
    if err:
        print(f"❌ Delivery failed: {err}")
    else:
        print(f"✅ Delivered to {msg.topic()} [{msg.partition()}]")

while True:
    level = random.choice(log_levels)
    app_log_message = {
        'log_level': level,
        'service_name': random.choice(services),
        'message': "Log message",
        'timestamp': time.time()
    }
    producer.produce(
        topic='app-logs',
        value=json.dumps(app_log_message),
        headers=[('log_level', level)],
        callback=delivery_report
    )
    print(f"🛠 Sent app log: {app_log_message}")

    producer.poll(0)
    time.sleep(2)



#			app_log_consumer.py\
from confluent_kafka import Consumer, KafkaError
import json
import time

#⚙️ Cấu hình Kafka
common_conf = {
    'bootstrap.servers': 'localhost:9092',
    'group.id': 'app-log-consumer-group',
    'auto.offset.reset': 'earliest'
}

consumer = Consumer(common_conf)
consumer.subscribe(['app-logs'])

log_counts = {'INFO': 0, 'ERROR': 0, 'DEBUG': 0}
service_log_counts = {}  # Tạo dict để lưu trữ số lượng log theo từng service
error_log_file = 'error.log'
last_report_time = time.time()

# Biến lưu trữ log ERROR trong 10s
error_log_buffer = []

def process_error_log_buffer():
    if error_log_buffer:
        with open(error_log_file, 'a') as f:
            for log in error_log_buffer:
                f.write(json.dumps(log) + '\n')
        print(f"🚨 {len(error_log_buffer)} ERROR logs saved to {error_log_file}")
        error_log_buffer.clear()  # Xóa buffer sau khi ghi

def update_service_log_count(service_name):
    if service_name not in service_log_counts:
        service_log_counts[service_name] = 0
    service_log_counts[service_name] += 1

try:
    while True:
        msg = consumer.poll(1.0)
        if msg is None:
            continue
        if msg.error():
            if msg.error().code() != KafkaError._PARTITION_EOF:
                print(f"❌ Consumer error: {msg.error()}")
            continue

        value = msg.value()
        if not value:
            print(f"⚠️ Empty message on topic {msg.topic()}")
            continue

        try:
            log = json.loads(value.decode('utf-8'))
            headers = dict(msg.headers() or [])
            level = headers.get('log_level', b'').decode('utf-8')

            # 🚨 Xử lý ERROR ngay lập tức
            if level == 'ERROR':
                error_log_buffer.append(log)
                service_name = log.get("service_name", "Unknown")
                update_service_log_count(service_name)

            # 📈 Cập nhật số lượng log theo level
            if level in log_counts:
                log_counts[level] += 1

        except json.JSONDecodeError:
            print(f"❌ Invalid JSON: {value}")

        # ⏲️ Sau mỗi 10 giây, in thống kê và lưu log ERROR
        if time.time() - last_report_time >= 10:
            print(f"📊 Log counts last 10s: {log_counts}")
            print(f"📊 Service-wise ERROR counts last 10s: {service_log_counts}")
            process_error_log_buffer()  # Ghi tất cả log ERROR vào file
            for k in log_counts:
                log_counts[k] = 0
            service_log_counts.clear()  # Reset service log counts
            last_report_time = time.time()

except KeyboardInterrupt:
    print('⛔ Interrupted')

finally:
    consumer.close()



##________________________Bài 3_______________________________
# producer.py
from confluent_kafka import Producer
import json
import random
import time

conf = {
    'bootstrap.servers': 'localhost:9092'
}

producer = Producer(conf)
events = ['click', 'view', 'purchase']

def delivery_report(err, msg):
    if err is not None:
        print(f"💥 Delivery failed: {err}")
    else:
        print(f"✅ Delivered to {msg.topic()} [{msg.partition()}]")

def choose_partition(event_type):
    if event_type == 'click':
        return 0
    elif event_type == 'view':
        return 1
    elif event_type == 'purchase':
        return 2
    else:
        return random.randint(0, 2)

while True:
    event_type = random.choice(events)
    event = {
        'user_id': f'user_{random.randint(1,100)}',
        'event_type': event_type,
        'item_id': f'item_{random.randint(1,50)}',
        'timestamp': int(time.time() * 1000)
    }
    partition = choose_partition(event_type)
    
    producer.produce(
        topic='user-events',
        key=event_type.encode('utf-8'),
        value=json.dumps(event).encode('utf-8'),
        partition=partition,
        callback=delivery_report
    )
    producer.poll(0)
    time.sleep(1)
# consumer.py
from confluent_kafka import Consumer, KafkaError
import json
import time

# 🛠 Config Kafka
common_conf = {
    'bootstrap.servers': 'localhost:9092',
    'group.id': 'multi-topic-consumer',
    'auto.offset.reset': 'earliest'
}

consumer = Consumer(common_conf)
consumer.subscribe(['network-status', 'app-logs'])

device_status = {}
log_counts = {'INFO': 0, 'ERROR': 0, 'DEBUG': 0}
error_log_file = 'error.log'
last_report_time = time.time()

def handle_network(data):
    device = data['device']
    status = data['status']
    device_status[device] = status
    print(f"🔌 [{device}] status [{status}]")

def handle_log(msg):
    value = msg.value()
    if not value:
        print(f"⚠️ Empty log message on topic {msg.topic()}")
        return

    try:
        log = json.loads(value.decode('utf-8'))
    except json.JSONDecodeError:
        print(f"❌ Invalid JSON: {value}")
        return

    headers = dict(msg.headers() or [])
    level = headers.get('log_level', b'').decode('utf-8')

    if level == 'ERROR':
        with open(error_log_file, 'a') as f:
            f.write(json.dumps(log) + '\n')

    if level in log_counts:
        log_counts[level] += 1

def report_log_counts():
    print(f"📊 Log counts: {log_counts}")
    for k in log_counts:
        log_counts[k] = 0

# 🎯 Main loop
try:
    while True:
        msg = consumer.poll(1.0)
        if msg is None:
            continue
        if msg.error():
            if msg.error().code() != KafkaError._PARTITION_EOF:
                print(f"❌ Consumer error: {msg.error()}")
            continue

        value = msg.value()
        if not value:
            print(f"⚠️ Empty message on topic {msg.topic()}")
            continue

        if msg.topic() == 'network-status':
            try:
                data = json.loads(value.decode('utf-8'))
                handle_network(data)
            except json.JSONDecodeError:
                print(f"❌ Invalid JSON on network-status: {value}")
        elif msg.topic() == 'app-logs':
            handle_log(msg)

        if time.time() - last_report_time >= 10:
            report_log_counts()
            last_report_time = time.time()

except KeyboardInterrupt:
    print('⛔ Interrupted')

finally:
    consumer.close()
