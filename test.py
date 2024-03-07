from pymongo import MongoClient

# MongoDB connection parameters
MONGODB_URL = 'mongodb://admin:Passw0rd!@localhost:27017/admin'
MONGODB_DB = 'frontend_backend_db'

print('Connecting to MongoDB...')

try:
    # Connect to MongoDB
    client = MongoClient(MONGODB_URL)
    db = client[MONGODB_DB]
    print('Connected to MongoDB')
except Exception as e:
    print('Failed to connect to MongoDB:', e)