import pika
from pymongo import MongoClient
import json

# RabbitMQ connection parameters
RABBITMQ_URL = 'amqp://user:password@localhost/%2F'
RABBITMQ_QUEUE = 'new_stories'

# MongoDB connection parameters
MONGODB_URL = 'mongodb://admin:Passw0rd!@localhost:27017/admin'
MONGODB_DB = 'frontend_backend_db'
MONGODB_COLLECTION = 'stories'

# Connect to MongoDB
client = MongoClient(MONGODB_URL)
db = client[MONGODB_DB]
collection = db[MONGODB_COLLECTION]

# Connect to RabbitMQ
connection = pika.BlockingConnection(pika.URLParameters(RABBITMQ_URL))
channel = connection.channel()

# Declare the queue
channel.queue_declare(queue=RABBITMQ_QUEUE, durable=True)

# Define the callback function
def callback(ch, method, properties, body):
    print("Received:", body)

    # Insert the message into MongoDB
    story = json.loads(body)
    collection.insert_one(story)
    print("Story inserted into MongoDB")

    # Acknowledge the message
    ch.basic_ack(delivery_tag=method.delivery_tag)

# Start consuming messages
channel.basic_consume(queue=RABBITMQ_QUEUE, on_message_callback=callback)
print('Waiting for new stories...')
channel.start_consuming()