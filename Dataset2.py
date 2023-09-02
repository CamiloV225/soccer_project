import psycopg2
import pandas as pd
import json


def connect_postgres():
         
    connection = psycopg2.connect(
        database="Soccer",
        user="postgres",
        password="Ronny1212",
        host="localhost",
        port=5432
    )
    return connection
    print("Database connection ok")

def create_table():
    connection = connect_postgres()
    cursor = connection.cursor()
    cursor.execute("""CREATE TABLE IF NOT EXISTS playervalue (
        rk integer primary key,
        Name varchar(255),
        Age integer,
        Value varchar(255),
        Team varchar(255),
        Nationality varchar(255),
        Position varchar(255)
        )""")
    
    copy = """COPY playervalue(
        Rk,
        Name,
        Age,
        Value,
        Team,
        Nationality,
        Position
        )
    FROM 'C:/Prevalidador_Requisitos_Saldo_a_favor_Renta_v3.1.0-20/transfermarkt_player_data.csv' DELIMITER ',' CSV HEADER
    """
    cursor.execute(copy)
    connection.commit()

    cursor.close()
    connection.close()

def delete_columns():
    connection = connect_postgres()
    cursor = connection.cursor()
    cursor.execute("""ALTER TABLE playerstats DROP COLUMN pastoatt, 
                   DROP pascmp, 
                   DROP gcapasslive, 
                   DROP gcapassdead, 
                   DROP scapasslive, 
                   DROP scapassdead, 
                   DROP pascrs, 
                   DROP born, 
                   DROP gsot, 
                   DROP gcadrib, 
                   DROP scadrib, 
                   DROP tklwon, 
                   DROP tklw, 
                   DROP tb, 
                   DROP recov, 
                   DROP shofk, 
                   DROP blkpass, 
                   DROP pkwon,
                   DROP paslive,
                   DROP pasdead,
                   DROP ckout,
                   DROP ckstr,
                   DROP paslow,
                   DROP pashigh,
                   DROP paswhead


                   ;""")

if __name__ == "__main__":
    connection = connect_postgres()
    create_table()