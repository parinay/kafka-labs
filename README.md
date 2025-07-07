# Kafka Transactions Filtering: Kafka Streams vs ksqlDB

This project demonstrates and compares two powerful stream processing approaches for filtering high-value debit transactions from Kafka messages using:
- **Kafka Streams (Java)** with header-based filtering.
- **ksqlDB** with SQL-like declarative queries.

The solution includes a Kafka producer, two consumers, a Kafka Streams application, a ksqlDB instance, and a monitoring stack (Prometheus + Grafana).

---

## 📁 Directory Structure

```
.
├── LICENSE
├── README.md                # This documentation file
├── consumer/                # Python consumer reading from filtered topics
│   ├── Dockerfile
│   └── txn_consumer.py
├── docker-compose.yml       # Main setup file to launch all services
├── grafana/
│   └── provisioning/
│       └── datasources/
│           └── datasource.yml  # Grafana data source configuration
├── ksql/
│   └── ksql-setup.sql       # ksqlDB DDL to define stream and filtering logic
├── producer/                # Python Kafka producer
│   ├── Dockerfile
│   └── txn_producer.py
├── prometheus/
│   └── prometheus.yml       # Prometheus configuration for monitoring
└── streams-app/             # Kafka Streams filtering application in Java
    ├── Dockerfile
    ├── FilterHighValueTxns.java
    ├── build/               # Build output folders (if any)
    ├── build.gradle         # Gradle build file
    └── src/
        └── main/
            └── java/
                └── FilterHighValueTxns.java
```

---

## ✅ Prerequisites

- Docker and Docker Compose installed.
- Python 3.9+ (for local code changes)
- Java 11+ (for Kafka Streams if building locally)
- Gradle (if building the Java app manually)

---

## 🚀 How to Run

```bash
# Clone the repo and enter the folder
git clone <your-repo-url>
cd kafka-streams-vs-ksqldb

# Start all containers
docker-compose up --build
```

It will:
- Start Kafka, ksqlDB, and the producer/consumer/streams containers.
- Execute the `ksql-setup.sql` to create streams and filtering query.

---

## 💻 Application Logic

- The `producer/txn_producer.py` publishes random transactions to the `transactions` topic.
- The `streams-app` consumes from `transactions`, filters high-value `DEBIT` transactions via headers + payload, and publishes to `high_value_txns_streams`.
- The `ksql-setup.sql` defines a stream `TRANSACTIONS` and filters `amount > 10000 AND channel = 'ATM'` to publish to `high_value_txns_ksql`.
- The `consumer/txn_consumer.py` reads from both filtered topics and prints messages.

---

## 📊 Monitoring

Prometheus and Grafana are set up to monitor the Kafka containers (extendable to app metrics).

- Access Grafana at: `http://localhost:3000` (if Grafana container is added)
- Prometheus pulls metrics from Kafka (extend using exporters)

---

## 🔍 Comparison: Kafka Streams vs ksqlDB

| Feature                      | Kafka Streams                                | ksqlDB                                           |
|-----------------------------|----------------------------------------------|--------------------------------------------------|
| Language                    | Java                                          | SQL-like (ksql)                                  |
| Filtering Logic             | Programmatic (via headers + payload)         | Declarative (SQL-based on payload only)          |
| Header Access               | Full access to headers                       | Not available                                    |
| Performance                 | High (compiled logic, per-record)            | Medium (interpreted queries)                     |
| Ease of Development         | Complex (code + compile + deploy)            | Easy (write SQL, run via CLI or REST)            |
| Maintenance Overhead       | Higher                                        | Lower                                            |
| Fine-Grained Control        | High                                          | Limited to supported SQL syntax                  |
| Ecosystem Integration       | Tight Java integration                       | Fast prototyping and ad-hoc querying             |

---

## 🔧 Customization

- Update `ksql/ksql-setup.sql` to change stream definitions.
- Update Python producer to generate real-world data.
- Extend Java `FilterHighValueTxns` logic to include more metadata-based filtering.
- Add Prometheus exporters to monitor Java and Python containers.

---

## 🧭 Next Steps

- Add schema registry and use Avro/Protobuf for typed events.
- Build dashboards in Grafana using Prometheus metrics.
- Simulate real-time workloads to compare throughput and latency.
- Enforce Kafka header standards across producers.

---

## 📜 License

This project is under the [MIT License](./LICENSE).

---

Feel free to fork or extend this example for more advanced Kafka streaming demos.