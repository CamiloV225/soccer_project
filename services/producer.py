from kafka import KafkaProducer
from json import dumps, loads
import time
import pandas as pd
from services.db import connect_postgres
def query():
    conn = connect_postgres()
    df = pd.read_sql_query("SELECT * FROM player_info", conn)
    return df


def kafka_producer():
    print('--------------Kafka App--------------')
    print("---------Sending Messages----------")
    df = query()
    producer = KafkaProducer(
        value_serializer = lambda m: dumps(m).encode('utf-8'),
        bootstrap_servers = ['localhost:9092'],
    )
    time.sleep(0.5)
    try:
        for index, row in df.iterrows():
            message = row.to_dict()
            print(message)
            print("Message Sent")
            producer.send("soccer", value=message)
            time.sleep(0.01)
    except:
        print("No hay mas mensajes")


    


       
