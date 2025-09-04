import pandas as pd
from confluent_kafka import SerializingProducer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.serialization import StringSerializer

# Delivery report callback
def delivery_report(err, msg):
    if err:
        print(f"[ERROR] Delivery failed: {err}")
    else:
        print(f"[OK] Produced key={msg.key()} topic={msg.topic()} partition={msg.partition()} offset={msg.offset()}")

# Kafka cluster config
kafka_config = {
    'bootstrap.servers': 'pkc-41p56.asia-south1.gcp.confluent.cloud:9092',
    'security.protocol': 'SASL_SSL',
    'sasl.mechanism': 'PLAIN',
    'sasl.username': 'HJVZZ7WIL5Y6S3XR',
    'sasl.password': 'cfltKKfhdz8J6FLgJT50ASDqT5Uxd5Wbhg3Aeqn3fQl/x+kmA5XGPzGUCD+mqRIA'
}

# Schema Registry config
schema_registry_client = SchemaRegistryClient({
    'url': 'https://psrc-4x6n5v3.us-east-2.aws.confluent.cloud',
    'basic.auth.user.info': '{}:{}'.format('6ZQ272GXY5IHI6CI', 'cfltiX0xme/R7lwp10E7Aa/FY8Fp388CfxL8r3vHBG90g/3ni/UhYQK2Ijwca+yg')
})

# Get latest Avro schema
def get_latest_schema(subject_name):
    schema_response = schema_registry_client.get_latest_version(subject_name)
    return schema_response.schema.schema_str

schema_str = get_latest_schema('ecommerce-orders-value')
avro_serializer = AvroSerializer(schema_registry_client, schema_str)
key_serializer = StringSerializer('utf8')

# Create producer
producer = SerializingProducer({
    'bootstrap.servers': kafka_config['bootstrap.servers'],
    'security.protocol': kafka_config['security.protocol'],
    'sasl.mechanism': kafka_config['sasl.mechanism'],
    'sasl.username': kafka_config['sasl.username'],
    'sasl.password': kafka_config['sasl.password'],
    'key.serializer': key_serializer,
    'value.serializer': avro_serializer
})

# Load dataset
df = pd.read_csv("olist_orders_dataset.csv")
df = df.where(pd.notnull(df), None)  # replace NaN with None

topic_name = "ecommerce-orders"

# Produce messages
for _, row in df.iterrows():
    try:
        key = f"{row['customer_id']}_{row['order_id']}"  # composite key
        producer.produce(
            topic=topic_name,
            key=key,
            value=row.to_dict(),
            on_delivery=delivery_report
        )
        producer.poll(0)
    except Exception as e:
        print(f"[ERROR] while producing order_id={row['order_id']}: {e}")

producer.flush()
print("[DONE] All records produced.")