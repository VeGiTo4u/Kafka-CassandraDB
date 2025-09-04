import uuid
from datetime import datetime
from confluent_kafka import DeserializingConsumer, KafkaError
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer
from confluent_kafka.serialization import StringDeserializer
from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider
from cassandra import ConsistencyLevel
import json 

# Kafka config
kafka_config = {
    'bootstrap.servers': 'pkc-41p56.asia-south1.gcp.confluent.cloud:9092',
    'security.protocol': 'SASL_SSL',
    'sasl.mechanism': 'PLAIN',
    'sasl.username': 'HJVZZ7WIL5Y6S3XR',
    'sasl.password': 'cfltKKfhdz8J6FLgJT50ASDqT5Uxd5Wbhg3Aeqn3fQl/x+kmA5XGPzGUCD+mqRIA',
    'group.id': 'ecommerce-consumer-group',
    'auto.offset.reset': 'earliest'
}

# Schema Registry config
schema_registry_client = SchemaRegistryClient({
    'url': 'https://psrc-4x6n5v3.us-east-2.aws.confluent.cloud',
    'basic.auth.user.info': '{}:{}'.format('6ZQ272GXY5IHI6CI', 'cfltiX0xme/R7lwp10E7Aa/FY8Fp388CfxL8r3vHBG90g/3ni/UhYQK2Ijwca+yg')
})

# Get latest schema
def get_latest_schema(subject_name):
    schema_response = schema_registry_client.get_latest_version(subject_name)
    return schema_response.schema.schema_str

schema_str = get_latest_schema('ecommerce-orders-value')
key_deserializer = StringDeserializer('utf8')
avro_deserializer = AvroDeserializer(schema_registry_client, schema_str)

# Consumer
consumer = DeserializingConsumer({
    **kafka_config,
    'key.deserializer': key_deserializer,
    'value.deserializer': avro_deserializer
})
consumer.subscribe(['ecommerce-orders'])

# Cassandra connection
cloud_config= {
  'secure_connect_bundle': 'secure-connect-ecommerce-db.zip'
}
with open("ecommerce_db-token.json") as f:
    secrets = json.load(f)

CLIENT_ID = secrets["clientId"]
CLIENT_SECRET = secrets["secret"]

auth_provider = PlainTextAuthProvider(CLIENT_ID, CLIENT_SECRET)
cluster = Cluster(cloud=cloud_config, auth_provider=auth_provider)
session = cluster.connect()

insert_query = session.prepare("""
    INSERT INTO ecommerce_keyspace.orders (
        order_id, customer_id, order_status,
        order_purchase_timestamp, order_approved_at,
        order_delivered_carrier_date, order_delivered_customer_date,
        order_estimated_delivery_date, order_hour, order_day_of_week
    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
""")
insert_query.consistency_level = ConsistencyLevel.QUORUM

def parse_ts(ts):
    return datetime.strptime(ts, "%Y-%m-%d %H:%M:%S") if ts else None

# Consume loop
try:
    while True:
        msg = consumer.poll(1.0)
        if msg is None:
            continue
        if msg.error():
            if msg.error().code() != KafkaError._PARTITION_EOF:
                print(f"[ERROR] {msg.error()}")
            continue

        raw_key = msg.key()
        value = msg.value()
        if not raw_key or not value:
            continue

        try:
            customer_id_str, order_id_str = raw_key.split("_", 1)
            customer_id = uuid.UUID(customer_id_str)
            order_id = uuid.UUID(order_id_str)
        except Exception as e:
            print(f"[WARN] Invalid key {raw_key}: {e}")
            continue

        order_purchase_timestamp = parse_ts(value['order_purchase_timestamp'])
        order_approved_at = parse_ts(value['order_approved_at'])
        order_delivered_carrier_date = parse_ts(value['order_delivered_carrier_date'])
        order_delivered_customer_date = parse_ts(value['order_delivered_customer_date'])
        order_estimated_delivery_date = parse_ts(value['order_estimated_delivery_date'])

        order_hour = order_purchase_timestamp.hour if order_purchase_timestamp else None
        order_day_of_week = order_purchase_timestamp.strftime('%A') if order_purchase_timestamp else None

        try:
            session.execute(insert_query, (
                order_id, customer_id, value['order_status'],
                order_purchase_timestamp, order_approved_at,
                order_delivered_carrier_date, order_delivered_customer_date,
                order_estimated_delivery_date, order_hour, order_day_of_week
            ))
            consumer.commit(msg)
            print(f"[OK] Inserted key={raw_key} into Cassandra")
        except Exception as err:
            print(f"[ERROR] Cassandra insert failed for key={raw_key}: {err}")

except KeyboardInterrupt:
    print("[STOP] Consumer interrupted.")

finally:
    consumer.close()
    cluster.shutdown()
    print("[DONE] Consumer and Cassandra closed.")