from google.cloud import pubsub_v1
from faker import Faker
import json
import random
import time
from datetime import datetime
import uuid

# -----------------------------------------
# CONFIGURATION
# -----------------------------------------

PROJECT_ID = "top-vial-497707-r1"
TOPIC_ID = "orders-topic"

# -----------------------------------------
# INITIALIZE
# -----------------------------------------

publisher = pubsub_v1.PublisherClient()
topic_path = publisher.topic_path(PROJECT_ID, TOPIC_ID)

fake = Faker()

# -----------------------------------------
# SAMPLE DATA
# -----------------------------------------

payment_methods = [
    "UPI",
    "Credit Card",
    "Debit Card",
    "Net Banking",
    "Wallet"
]

product_categories = [
    "Electronics",
    "Fashion",
    "Home",
    "Books",
    "Beauty"
]

order_statuses = [
    "PLACED",
    "CONFIRMED",
    "SHIPPED",
    "DELIVERED"
]

# -----------------------------------------
# GENERATE EVENT
# -----------------------------------------

def generate_order_event():

    order_event = {
        "order_id": str(uuid.uuid4()),
        "customer_id": random.randint(1000, 9999),
        "product_id": random.randint(10000, 99999),
        "category": random.choice(product_categories),
        "price": round(random.uniform(100, 5000), 2),
        "quantity": random.randint(1, 5),
        "payment_method": random.choice(payment_methods),
        "order_status": random.choice(order_statuses),
        "order_timestamp": datetime.utcnow().isoformat(),
        "customer_city": fake.city(),
        "customer_state": fake.state(),
        "customer_country": "India"
    }

    return order_event

# -----------------------------------------
# PUBLISH EVENTS
# -----------------------------------------

def publish_order_event():

    while True:

        event = generate_order_event()

        message_json = json.dumps(event)

        message_bytes = message_json.encode("utf-8")

        future = publisher.publish(
            topic_path,
            data=message_bytes
        )

        print(f"Published message ID: {future.result()}")
        print(message_json)
        print("-" * 80)

        time.sleep(2)

# -----------------------------------------
# MAIN
# -----------------------------------------

if __name__ == "__main__":

    print("Starting real-time order event publisher...")

    publish_order_event()
