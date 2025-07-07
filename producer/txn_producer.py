from kafka import KafkaProducer
import json
import time
import random

producer = KafkaProducer(
    bootstrap_servers='kafka:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8'),
    key_serializer=lambda k: k.encode('utf-8'),
    api_version=(2,8,0)
)

accounts = ['A123', 'B456', 'C789']
tiers = ['STANDARD', 'PREMIUM']

while True:
    account_id = random.choice(accounts)
    tier = random.choice(tiers)
    key = f"{account_id}-{tier}"

    value = {
        'txn_id': f'TXN{random.randint(1000,9999)}',
        'account_id': account_id,
        'amount': random.uniform(1000, 20000),
        'channel': random.choice(['ATM', 'POS', 'ONLINE'])
    }

    headers = [
        ('event_type', b'DEBIT'),
        ('jurisdiction', b'UK')
    ]

    producer.send('transactions', key=key, value=value, headers=headers)
    print(f"Produced: key={key}, value={value}, headers={headers}")
    time.sleep(1)