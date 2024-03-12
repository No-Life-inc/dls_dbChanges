# dls_frontend_write_rest_api

## Contributors

- Morten Bendeke
- Betül Iskender
- Yelong Hartl-He
- Zack Ottesen

## General Use

This is the subscriber for changes that are in RabbitMQ queues.<br>
It subscribes to queues regardning new or changed entities that need to be persisted in the read-only DB.<br>
given the size of the repo, only subscriber.py are in use at the moment.

## Environment Variables

Create a .env in the root folder.

- MONGOUSER=admin
- MONGOPW=Passw0rd!
- MONGOURL=@localhost:27017/admin
- MONGODB=frontend_backend_db
- RABBITUSER=user
- RABBITPW=password
- RABBITURL=localhost

## How To Run

Make sure the environment variables are set.<br>
Run npm install <dependency> to install dependencies.<br>
Lastly, use the following command:

```bash
python subscriber.py
```

## Dependencies

The Repo needs the following dependencies:
- pika 
- pymongo 
- python-dotenv

Run this line to install them all
```bash
pip install pika pymongo python-dotenv
```