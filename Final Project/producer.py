from google.cloud import storage
from kafka import KafkaProducer
from time import sleep
import json
from json import dumps
from os import environ
from os import listdir
from os.path import isfile, join


bucketName = 'ch18b013mkvr'
bucketFolder = 'YELP_train.csv'
topic_name = 'yelp-topic'
BROKER_IP = "10.162.0.2:9092"
storage_client = storage.Client()
bucket = storage_client.get_bucket(bucketName)
files = bucket.list_blobs(prefix=bucketFolder)

fileList = [file.name for file in files if file.name.endswith('.csv')]
producer = KafkaProducer(bootstrap_servers=BROKER_IP, value_serializer=lambda k: k.encode('utf-8'))

for filetemp in fileList : 
    blob = bucket.blob(filetemp)
    x = blob.download_as_string()
    x = x.decode('utf-8')
    data_str = x.split('\n')

    for each_data in data_str:
        message = each_data.split(',')
        if len(message) == 9:
          mess_send = str(message[5]) + '%' + message[6]
          print(mess_send)
          producer.send(topic_name, value=mess_send)
          sleep(0.1)