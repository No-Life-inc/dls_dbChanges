import pika
from dotenv import load_dotenv
from pymongo import MongoClient
import json

# Load environment variables from .env file
env_vars = load_dotenv(override=True)

# RabbitMQ connection parameters
RABBITMQ_URL = f"amqp://{env_vars['RABBITUSER']}:{env_vars['RABBITPW']}@{env_vars['RABBITURL']}/%2F"
RABBITMQ_QUEUE = 'new_stories'

# MongoDB connection parameters
MONGODB_URL = f"mongodb://{env_vars['MONGOUSER']}:{env_vars['MONGOPW']}{env_vars['MONGOURL']}"
MONGODB_DB = {env_vars['MONGODB']}
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