from kafka import KafkaConsumer
import requests
import re
import pandas as pd
from json import loads
from datetime import datetime, date
import json
API_ENDPOINT="https://api.powerbi.com/beta/693cbea0-4ef9-4254-8977-76e05cb5f556/datasets/82903a0b-8fcc-4c43-ad28-80eb1fc0686f/rows?experience=power-bi&key=eMrGzxzppulghzM3schFPGwq0l3jg2OKcsSygCrhgbsPKKNVrCLXxOCv0OJwdKbVgK%2F5ISYEpEx8ifx%2F2VAsEA%3D%3D"
print("Kafka Consumer App Inicia")
 
consumer = KafkaConsumer(
        'soccer',
        auto_offset_reset='earliest',
        enable_auto_commit=True,
        group_id='my-group-1',
        value_deserializer=lambda m: loads(m.decode('utf-8')),
        bootstrap_servers=[f'localhost:9092']
    )
consumer.poll()
consumer.seek_to_end()

for data_json in consumer:
    s = data_json.value
    df = pd.DataFrame.from_dict([s])
    data = bytes(df.to_json(orient='records'), 'utf-8')
    req = requests.post(API_ENDPOINT, data)
