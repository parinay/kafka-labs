FROM python:3.9-slim
WORKDIR /app
COPY txn_producer.py .
RUN pip install kafka-python
CMD ["python", "txn_producer.py"]