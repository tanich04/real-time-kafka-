import logging
from confluent_kafka import avro
from confluent_kafka.avro import AvroProducer
import time
import random

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Load Avro schema
try:
    value_schema = avro.load("log_schema.avsc")
except Exception as e:
    logging.error(f"Failed to load Avro schema: {e}")
    exit(1)

# Configure Avro Producer with Schema Registry
producer_config = {
    'bootstrap.servers': 'localhost:9092',
    'schema.registry.url': 'http://localhost:8081'
}

producer = AvroProducer(producer_config, default_value_schema=value_schema)

actions = ['login', 'logout', 'purchase', 'view']

try:
    while True:
        log = {
            'timestamp': int(time.time()),
            'user_id': random.randint(1000, 5000),
            'action': random.choice(actions),
            'value': round(random.random() * 100, 2)
        }
        producer.produce(topic='logs_topic', value=log)
        producer.flush()
        logging.info(f"Produced: {log}")
        time.sleep(0.2)
except KeyboardInterrupt:
    logging.info("Producer stopped by user.")
except Exception as e:
    logging.error(f"Error in producer: {e}")
finally:
    producer.flush()