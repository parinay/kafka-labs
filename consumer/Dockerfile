FROM python:3.9-slim
WORKDIR /app
COPY txn_consumer.py .
RUN pip install kafka-python
CMD ["python", "txn_consumer.py"]