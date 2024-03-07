import { MongoClient } from 'mongodb';

// MongoDB connection parameters
const MONGODB_URL = 'mongodb://admin:Passw0rd!@localhost:27017/admin';
const MONGODB_DB = 'frontend_backend_db';

console.log('Connecting to MongoDB...');
// Connect to MongoDB
MongoClient.connect(MONGODB_URL, (err, client) => {
  if (err) {
    console.error('Failed to connect to MongoDB:', err);
    return;
  }

  const db = client.db(MONGODB_DB);
  console.log('Connected to MongoDB');
});