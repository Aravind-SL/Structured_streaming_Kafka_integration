from kafka import KafkaProducer
import json
import pandas as pd
import time

# Kafka broker address
kafka_bootstrap_servers = 'localhost:9092'

# Kafka topic to produce data to
kafka_topic = 'order-topic'

file_path = './orders.csv'

# Create Kafka producer
producer = KafkaProducer(
    bootstrap_servers=kafka_bootstrap_servers,
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

def produce_data(kafka_topic, file_path):
    
    df = pd.read_csv(file_path)
    # Produce messages to Kafka topic
    for _, row in df.iterrows():
        message_key = str(row['order_id']).encode('utf-8')  # Convert string to bytes
        message_value = row.to_dict()
        producer.send(kafka_topic, key=message_key, value=message_value)
        time.sleep(0.01)

if __name__ == '__main__':
    produce_data(kafka_topic, file_path)
