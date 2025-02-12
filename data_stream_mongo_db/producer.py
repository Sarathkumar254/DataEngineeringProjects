import pdb
import pandas as pd
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.serialization import StringSerializer

from confluent_kafka.serializing_producer import SerializingProducer

# read the data from csv

data =  pd.read_csv("C:\\Users\\sarat\\OneDrive\\Desktop\\Data Engineering\\MangoDB\\Class_Assignment\\delivery_trip_truck_data.csv")


# ---------- data analysis ------------------------------

# data.head(1)

# data.info()

# data.columns

# data.describe()


# --------- Kafka configuration -----------------------------


# Define Kafka configuration
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

# key serializer and value (avro) serializer

key_serializer = StringSerializer("utf_8")
value_serializer = AvroSerializer(schema_registry_client, schema_str)


# Producer

kafka_config["key.serializer"] = key_serializer
kafka_config["value.serializer"] = value_serializer
producer = SerializingProducer(kafka_config)

def on_delivery(err, msg):

    if err is not None:
        print(f"Delivery failed for this key {msg.key()} beause of {err}")
    else:
        print(f"Delivery successful for key {msg.key()}, {msg.partition()} and offset value {msg.offset()}")


def process_data(record):
    row = {
        "BookingID":record["BookingID"],
        "GpsProvider":record["GpsProvider"],
        "category": record['Market'],
        "BookingID_Date": record["BookingID_Date"],
        "vehicle_no": record["vehicle_no"],
        "Origin_Location": record["Origin_Location"],
        "Destination_Location": record["Destination_Location"],
        "Org_lat_lon": record["Org_lat_lon"],
        "Des_lat_lon": record["Des_lat_lon"],
        "Data_Ping_time": record["Data_Ping_time"],
        "Planned_ETA": record["Planned_ETA"],
        "Current_Location": record["Current_Location"],
        "DestinationLocation": record["DestinationLocation"],
        "actual_eta": record["actual_eta"],
        "Curr_lat": record["Curr_lat"],
        "Curr_lon": record["Curr_lon"],
        "ontime": record["ontime"],
        "delay": record["delay"],
        "OriginLocation_Code": record["OriginLocation_Code"],
        "DestinationLocation_Code": record["DestinationLocation_Code"],
        "trip_start_date": record["trip_start_date"],
        "trip_end_date": record["trip_end_date"],
        "TRANSPORTATION_DISTANCE_IN_KM": record["TRANSPORTATION_DISTANCE_IN_KM"],
        "vehicleType": record["vehicleType"],
        "Minimum_kms_to_be_covered_in_a_day": record["Minimum_kms_to_be_covered_in_a_day"],
        "Driver_Name": record["Driver_Name"],
        "Driver_MobileNo": record["Driver_MobileNo"],
        "customerID": record["customerID"],
        "customerNameCode": record["customerNameCode"],
        "supplierID": record["supplierID"],
        "supplierNameCode": record["supplierNameCode"],
        "Material_Shipped": record["Material Shipped"]
    }

    producer.produce(topic="delivery_trip", key=row["supplierID"], value=row, on_delivery=on_delivery)
    producer.flush()
    print("data processed successfully")









while True:
    data = data.fillna("null").to_dict(orient="records")
    # pdb.set_trace()
    # for key, value in data:dat
    for record in data:
        process_data(record)
        # pdb.set_trace()