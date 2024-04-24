import os
import pika
from dotenv import load_dotenv
from pymongo import MongoClient
import json

# Load environment variables from .env file
load_dotenv()

print(os.getenv('RABBITUSER'))
# RabbitMQ connection parameters
RABBITMQ_URL = f"amqp://{os.getenv('RABBITUSER')}:{os.getenv('RABBITPW')}@{os.getenv('RABBITURL')}/%2F"
RABBITMQ_NEW_STORIES_QUEUE = 'new_stories'
RABBITMQ_UPDATE_STORY_QUEUE = 'update_story_info'
RABBITMQ_COMMENTS_QUEUE = 'new_comments'

# MongoDB connection parameters
MONGODB_URL = f"mongodb://{os.getenv('MONGOUSER')}:{os.getenv('MONGOPW')}{os.getenv('MONGOURL')}"
MONGODB_DB = os.getenv('MONGODB')
MONGODB_STORY_COLLECTION = 'stories'
MONGODB_COMMENT_COLLECTION = 'comments'

# Connect to MongoDB
client = MongoClient(MONGODB_URL)
db = client[MONGODB_DB]
story_collection = db[MONGODB_STORY_COLLECTION]
comment_collection = db[MONGODB_COMMENT_COLLECTION]

# Connect to RabbitMQ
connection = pika.BlockingConnection(pika.URLParameters(RABBITMQ_URL))
channel = connection.channel()

# Declare the queues
channel.queue_declare(queue=RABBITMQ_NEW_STORIES_QUEUE, durable=True)
channel.queue_declare(queue=RABBITMQ_COMMENTS_QUEUE, durable=True)
channel.queue_declare(queue=RABBITMQ_UPDATE_STORY_QUEUE, durable=True)


# Define the callback function
def story_callback(ch, method, properties, body):
    print("Received:", body)

    # Insert the message into MongoDB
    story = json.loads(body)
    story_collection.insert_one(story)
    print("Story inserted into MongoDB")

    # Acknowledge the message
    ch.basic_ack(delivery_tag=method.delivery_tag)

# Define the callback function for updating story info
def update_story_info_callback(ch, method, properties, body):
    print("Received story update:", body)
    story_update = json.loads(body)

    # Update the story in MongoDB
    story_collection.update_one(
        {'storyGuid': story_update['storyGuid']}, 
        {'$set': {'storyInfo': story_update['storyInfo']}}
    )
    print("Story info updated in MongoDB")

    # Acknowledge the message
    ch.basic_ack(delivery_tag=method.delivery_tag)


# Define the callback function for comments
def comment_callback(ch, method, properties, body):
    print("Received comment:", body)
    comment = json.loads(body)
    comment_collection.insert_one(comment)
    print("Comment inserted into MongoDB")
    ch.basic_ack(delivery_tag=method.delivery_tag)


# Start consuming messages
channel.basic_consume(queue=RABBITMQ_NEW_STORIES_QUEUE, on_message_callback=story_callback)
channel.basic_consume(queue=RABBITMQ_COMMENTS_QUEUE, on_message_callback=comment_callback)
channel.basic_consume(queue=RABBITMQ_UPDATE_STORY_QUEUE, on_message_callback=update_story_info_callback)
print('Waiting for new published content...')
channel.start_consuming()
