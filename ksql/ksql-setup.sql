CREATE STREAM IF NOT EXISTS transactions (
  txn_id STRING,
  account_id STRING,
  amount DOUBLE,
  channel STRING
) WITH (
  KAFKA_TOPIC='transactions',
  VALUE_FORMAT='JSON',
  PARTITIONS=1
);

CREATE STREAM IF NOT EXISTS high_value_txns_ksql
WITH (KAFKA_TOPIC='high_value_txns_ksql', VALUE_FORMAT='JSON') AS
SELECT * FROM transactions WHERE amount > 10000;