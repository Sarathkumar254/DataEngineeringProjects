import json, pdb
from confluent_kafka.deserializing_consumer import DeserializingConsumer
from confluent_kafka.schema_registry.avro import AvroDeserializer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.serialization import StringDeserializer

from pymongo import MongoClient


# setup connection scehma registry

kafka_config = {
    'bootstrap.servers': 'pkc-l7pr2.ap-south-1.aws.confluent.cloud:9092',
    'sasl.mechanisms': 'PLAIN',
    'security.protocol': 'SASL_SSL',
    'sasl.username': 'WFDAKMLCZKNMEBJE',
    'sasl.password': 'k3Za/moI21uOwRip2O1yk1UBThlQelOP8dMzycgj03nM3PqatTrR3vKGUmcx2JzO'
}

# Create a Schema Registry client
schema_registry_client = SchemaRegistryClient({
  'url': 'https://psrc-3n6x0.southeastasia.azure.confluent.cloud',
  'basic.auth.user.info': '{}:{}'.format('75L5HDVKGBJMETRM', 'qeyaZ2I5Jmq9xclxw/u8/TRkAgj12JUW8b7yDKRAyrw5fHnx90isMQeWWs//iN76')
})

schema_name = "delivery_trip-value"
schema_str = schema_registry_client.get_latest_version(schema_name).schema.schema_str

key_deserializer = StringDeserializer("utf_8")
value_deserializer = AvroDeserializer(schema_registry_client, schema_str)

kafka_config["key.deserializer"] = key_deserializer
kafka_config["value.deserializer"] = value_deserializer
kafka_config["group.id"] = "logistic"

# make a connection to mongo DB

mongodb = MongoClient("mongodb+srv://sarath:00000000@logistic.cx6yq.mongodb.net/?retryWrites=true&w=majority&appName=logistic")

db = mongodb["logistic_db"]
collection = db["logistic_data"]

consumer = DeserializingConsumer(kafka_config)

consumer.subscribe(['delivery_trip'])


try:
    while True:
        msg = consumer.poll(1.0)  # Poll for new messages

        if msg is None:
            continue
        
        if msg.error():
            print(f"Consumer error: {msg.error()}")
            continue
        
        # Decode the message and parse as JSON
        message_value = msg.value()
        print(f"Received Message: {message_value}")
        existing_doc = collection.find_one({'BookingID': message_value['BookingID']})
        # Insert into MongoDB
        if existing_doc:
            print(f"Doccument '{message_value['BookingID']}' is already present in MongoDB")
        else:
            collection.insert_one(message_value)
            print("Data inserted into MongoDB:", message_value)

except KeyboardInterrupt:
    print("Stopping Consumer...")

finally:
    # Close the consumer gracefully
    consumer.commit()
    consumer.close()
    mongodb.close()

