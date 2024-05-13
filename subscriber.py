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
RABBITMQ_UPDATE_COMMENT_QUEUE = 'update_comment_info'
RABBITMQ_NEW_COMMENTS_QUEUE = 'new_comments'
RABBITMQ_DELETE_QUEUE = 'delete_story'
RABBITMQ_DELETE_COMMENT_QUEUE = 'delete_comment'


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
channel.queue_declare(queue=RABBITMQ_NEW_COMMENTS_QUEUE, durable=True)
channel.queue_declare(queue=RABBITMQ_UPDATE_STORY_QUEUE, durable=True)
channel.queue_declare(queue=RABBITMQ_UPDATE_COMMENT_QUEUE, durable=True)
channel.queue_declare(queue=RABBITMQ_DELETE_QUEUE, durable=True)
channel.queue_declare(queue=RABBITMQ_DELETE_COMMENT_QUEUE, durable=True)

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


def update_comment_info_callback(ch, method, properties, body):
    print("Received comment update:", body)
    comment_update = json.loads(body)

    try:
        # Update the comment in MongoDB
        result = story_collection.update_one(
            {'comments': {'$elemMatch': {'commentGuid': comment_update['commentGuid']}}},
            {'$set': {'comments.$.commentInfo': comment_update['commentInfo']}}
        )
        print("Matched documents:", result.matched_count)
        print("Modified documents:", result.modified_count)
        
        if result.matched_count == 0:
            print("No matching document found in MongoDB")

    except Exception as e:
        print("Error updating comment info in MongoDB:", e)

    ch.basic_ack(delivery_tag=method.delivery_tag)

# Define the callback function for comments
def comment_callback(ch, method, properties, body):
    print("Received comment:", body)
    comment = json.loads(body)

    # Extract the first commentInfo object from the array
    comment_info = comment.get('commentInfos')[0] if comment.get('commentInfos') else {}

    # Log the userGuid
    user_guid = comment.get('user', {}).get('userGuid')
    print(f"User GUID from comment: {user_guid}")

    story_collection.update_one(
        {'storyGuid': comment.get('story', {}).get('storyGuid')},
        {'$push': {'comments': {
            'commentGuid': comment.get('commentGuid'),
            'createdAt': comment.get('createdAt'),
            'commentInfo': {
                'bodyText': comment_info.get('bodyText'),
                'createdAt': comment_info.get('createdAt')
            },
            'user': comment.get('user')
        }}}
    )
    print("Story updated with comment in MongoDB")
    ch.basic_ack(delivery_tag=method.delivery_tag)

def delete_story_callback(ch, method, properties, body):
    print("Received delete request:", body)
    story_guid = body.decode().strip('"') # decode the byte string to a normal string

    # Delete the story from MongoDB
    result = story_collection.delete_one({'storyGuid': story_guid})
    print(f"Story {story_guid} deleted from MongoDB, deleted count: {result.deleted_count}")

    # Acknowledge the message
    ch.basic_ack(delivery_tag=method.delivery_tag)

# Define the callback function for deleting comments
def delete_comment_callback(ch, method, properties, body):
    print("Received delete comment request:", body)
    comment_guid = body.decode().strip('"') # decode the byte string to a normal string

    # Delete the comment from MongoDB
    result = story_collection.update_one(
            {'comments.commentGuid': comment_guid}, 
            {'$pull': {'comments': {'commentGuid': comment_guid}}}
        )   
    print(f"Comment {comment_guid} deleted from MongoDB, updated count: {result.modified_count}")


    # Acknowledge the message
    ch.basic_ack(delivery_tag=method.delivery_tag)



# Start consuming messages
channel.basic_consume(queue=RABBITMQ_NEW_STORIES_QUEUE, on_message_callback=story_callback)
channel.basic_consume(queue=RABBITMQ_NEW_COMMENTS_QUEUE, on_message_callback=comment_callback)
channel.basic_consume(queue=RABBITMQ_UPDATE_STORY_QUEUE, on_message_callback=update_story_info_callback)
channel.basic_consume(queue=RABBITMQ_UPDATE_COMMENT_QUEUE, on_message_callback=update_comment_info_callback)
channel.basic_consume(queue=RABBITMQ_DELETE_QUEUE, on_message_callback=delete_story_callback)
channel.basic_consume(queue=RABBITMQ_DELETE_COMMENT_QUEUE, on_message_callback=delete_comment_callback)
print('Waiting for new published content...')
channel.start_consuming()
