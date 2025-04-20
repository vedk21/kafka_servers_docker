import json
import time
import random

from kafka import KafkaProducer


CUSTOMERS_TOPIC = 'customers'
ORDERS_TOPIC = 'orders'
BOOTSTRAP_SERVERS = 'localhost:9091,localhost:9092,localhost:9093'

def setup_producer():
  try:
    producer = KafkaProducer(
      bootstrap_servers=BOOTSTRAP_SERVERS,
      value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )
    return producer
  except Exception as e:
    if e == 'NoBrokersAvailable':
      print('waiting for brokers to become available')
    return 'not-ready'


def create_customer_data():
  customer_id = int(time.time())  # Simple unique ID
  return {
    "customer_id": customer_id,
    "name": f"Customer_{customer_id}",
    "email": f"customer_{customer_id}@example.com",
    'region': str(random.choice(['MH', 'UP', 'GJ'])),
    "registration_date": time.strftime("%Y-%m-%d %H:%M:%S")
  }

def create_order_data(customer_id):
  order_id = int(time.time() * 1000) # More granular unique ID
  return {
    "order_id": order_id,
    "customer_id": customer_id,
    "order_date": time.strftime("%Y-%m-%d %H:%M:%S"),
    "total_amount": round(random.uniform(10, 500), 2),
    "status": random.choice(["PLACED", "SHIPPED", "DELIVERED", "CANCELLED"])
  }


def produce_data(producer):
  try:
    while True:
      customer_data = create_customer_data()
      producer.send(CUSTOMERS_TOPIC, value=customer_data)
      print(f"Produced customer data: {customer_data}")

      orders_per_customer = random.choice([5,9,8])
      for i in range(orders_per_customer):
        order_date = create_order_data(customer_data.get('customer_id'))
        producer.send(ORDERS_TOPIC, value=order_date)
        print(f"Customer {customer_data.get('customer_id')}: Produced order data {i}: {order_date}")
        time.sleep(3) # Produce every 3 seconds
      
      time.sleep(5)  # Produce every 5 seconds
  except KeyboardInterrupt:
    print("Producer stopped by user.")
  finally:
    producer.close()


if __name__ == "__main__":
  print('setting up producer, checking if brokers are available')
  producer = 'not-ready'

  while producer == 'not-ready':
    print('brokers not available yet')
    time.sleep(5)
    producer = setup_producer()

  print('brokers are available and ready to produce messages')
  produce_data(producer)
