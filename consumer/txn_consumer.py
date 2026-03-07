from kafka import KafkaConsumer
import json

consumer = KafkaConsumer(
    'high_value_txns_ksql',
    'high_value_txns_streams',
    bootstrap_servers='kafka:9092',
    value_deserializer=lambda m: json.loads(m.decode('utf-8')),
    key_deserializer=lambda k: k.decode('utf-8') if k else None,
    group_id='txn-consumer',
    auto_offset_reset='earliest'
)

for msg in consumer:
    print(f"[{msg.topic}] key={msg.key}, value={msg.value}")