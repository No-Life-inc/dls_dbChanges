import amqp from 'amqplib/callback_api.js';
import { MongoClient } from 'mongodb';

// RabbitMQ connection parameters
const RABBITMQ_URL = 'amqp://user:password@localhost';
const RABBITMQ_QUEUE = 'new_stories';

// MongoDB connection parameters
const MONGODB_URL = 'mongodb://admin:Passw0rd!@localhost:27017';
const MONGODB_DB = 'frontend_backend_db';
const MONGODB_COLLECTION = 'stories';

let collection = null;

// Connect to MongoDB
MongoClient.connect(MONGODB_URL, (error2, client) => {
    if (error2) {
      console.error('Failed to connect to MongoDB:', error2);
      throw error2;
    }
  
    const db = client.db(MONGODB_DB);
    const collection = db.collection(MONGODB_COLLECTION);
    console.log('Connected to MongoDB');
  
    // Connect to RabbitMQ
    amqp.connect(RABBITMQ_URL, (error0, connection) => {
      if (error0) {
        console.error('Failed to connect to RabbitMQ:', error0);
        throw error0;
      }
  
      connection.createChannel((error1, channel) => {
        if (error1) {
          console.error('Failed to create a channel:', error1);
          throw error1;
        }
  
        channel.assertQueue(RABBITMQ_QUEUE, { durable: true });
        console.log('Waiting for new stories...');
  
        // Consume messages from the queue
        channel.consume(RABBITMQ_QUEUE, (msg) => {
          console.log('Received:', msg.content.toString());
  
          // Insert the message into MongoDB
          const story = JSON.parse(msg.content.toString());
          collection.insertOne(story, (error3, result) => {
            if (error3) {
              console.error('Failed to insert story into MongoDB:', error3);
              throw error3;
            }
  
            console.log('Story inserted into MongoDB:', result.insertedId);
  
            // Acknowledge the message
            channel.ack(msg);
          });
        }, { noAck: false });
      });
    });
  });