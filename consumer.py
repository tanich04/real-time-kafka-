import logging
import time
import pandas as pd
import psycopg2
from psycopg2.extras import execute_values
from confluent_kafka.avro import AvroConsumer, AvroProducer
from confluent_kafka.avro.serializer import SerializerError
import json

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Kafka Avro Consumer setup
consumer_config = {
    'bootstrap.servers': 'localhost:9092',
    'group.id': 'etl_group',
    'schema.registry.url': 'http://localhost:8081',
    'auto.offset.reset': 'earliest'
}
consumer = AvroConsumer(consumer_config)
consumer.subscribe(['logs_topic'])

# Kafka Avro Producer for the Dead-Letter Queue (DLQ)
dlq_producer_config = {
    'bootstrap.servers': 'localhost:9092',
    'schema.registry.url': 'http://localhost:8081'
}
dlq_producer = AvroProducer(dlq_producer_config)

# PostgreSQL connection
try:
    conn = psycopg2.connect("dbname=etl user=postgres password=postgres host=localhost")
    cursor = conn.cursor()
    logging.info("Successfully connected to PostgreSQL.")
except Exception as e:
    logging.error(f"Could not connect to PostgreSQL: {e}")
    exit(1)

buffer = []
BUFFER_SIZE = 100
dlq_topic = 'user_logs_dlq'

def insert_batch(batch):
    """
    Inserts a batch of records into the PostgreSQL database.
    """
    query = """
        INSERT INTO user_logs (timestamp, user_id, action, value)
        VALUES %s
    """
    try:
        execute_values(cursor, query, batch)
        conn.commit()
        logging.info(f"Successfully inserted {len(batch)} records.")
    except Exception as e:
        conn.rollback()  # Rollback transaction on error
        logging.error(f"Failed to insert batch. Rolling back. Error: {e}")
        # Here, you could send the entire batch to the DLQ for failed DB inserts

# Main consumption loop
logging.info("Starting Kafka consumer loop...")
try:
    while True:
        messages = consumer.poll(timeout=1.0)
        if messages is None:
            continue
        if len(messages) == 0:
            if buffer:
                insert_batch(buffer)
                buffer.clear()
                consumer.commit()
            time.sleep(1)
            continue

        for msg in messages:
            if msg.error():
                if msg.error().code() == -191:  # _PARTITION_EOF
                    continue
                else:
                    logging.error(msg.error())
                    continue
            
            try:
                data = msg.value() # .value() returns a dict for AvroConsumer
                buffer.append((
                    pd.to_datetime(data['timestamp'], unit='s'),
                    data['user_id'],
                    data['action'],
                    data['value']
                ))

                if len(buffer) >= BUFFER_SIZE:
                    insert_batch(buffer)
                    buffer.clear()
                    # Commit offsets after a successful batch insert
                    consumer.commit()
            except SerializerError as e:
                logging.error(f"Failed to deserialize message: {e}")
                dlq_producer.produce(topic=dlq_topic, value=msg.value())
                dlq_producer.flush()
            except Exception as e:
                # Catch other processing errors and send to DLQ
                logging.error(f"Failed to process message from offset {msg.offset()}: {msg.value()}. Error: {e}")
                dlq_producer.produce(topic=dlq_topic, value=msg.value())
                dlq_producer.flush()

except KeyboardInterrupt:
    logging.info("Consumer stopped by user.")
finally:
    # Process any remaining messages in the buffer before shutting down
    if buffer:
        insert_batch(buffer)
        buffer.clear()
        consumer.commit()
    consumer.close()
    conn.close()