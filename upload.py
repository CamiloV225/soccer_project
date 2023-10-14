import pandas as pd
import mysql.connector
import json
from transform import transformstats,transform_value

def connect_mysql():
    with open('db_config.json') as f:
        dbfile = json.load(f)
    
    connection = mysql.connector.connect(
        host=dbfile["host"],
        user=dbfile["user"],
        password=dbfile["password"],
        database=dbfile["database"]
    )
    
    print("Database connection ok")
    return connection

def create_tables():
    conn = connect_mysql()
    cursor = conn.cursor()
    PlayerPosition = f"""CREATE TABLE IF NOT EXISTS player_posicion (
        id INT PRIMARY KEY AUTO_INCREMENT,
        posicion VARCHAR(10)
        )
        """
    cursor.execute(PlayerPosition)
    PlayerValue = f"""CREATE TABLE IF NOT EXISTS player_value (
        id INT PRIMARY KEY AUTO_INCREMENT,
        player VARCHAR(255),
        value BIGINT
        )
        """
    cursor.execute(PlayerValue)
    player_stats = f"""CREATE TABLE IF NOT EXISTS player_stats (
        id INT PRIMARY KEY AUTO_INCREMENT,
        MP INT,
        Starts INT,
        Min INT,
        Goals FLOAT,
        Shots FLOAT,
        SoT FLOAT,
        SoTPorcent FLOAT,
        GSh FLOAT,
        ShoDist FLOAT,
        ShoFK FLOAT,
        ShoPK FLOAT,
        PKatt FLOAT,
        PasTotCmp FLOAT,
        PasTotCmpPorcent FLOAT,
        PasShoCmpPorcent FLOAT,
        PasMedCmpPorcent FLOAT,
        PasLonCmpPorcent FLOAT,
        Assists FLOAT, 
        PPA FLOAT,
        CrsPA FLOAT,
        PasAtt FLOAT,
        PasFK FLOAT,
        PasPress FLOAT,
        Sw FLOAT,
        PasCrs FLOAT,
        CK FLOAT,
        PaswLeft FLOAT,
        PaswRight FLOAT,
        PasInt FLOAT,
        SCA FLOAT,
        GCA FLOAT,
        TklDef3rd FLOAT,
        TklMid3rd FLOAT,
        TklAtt3rd FLOAT,
        TklDri FLOAT,
        TklDriPorcent FLOAT,
        Press FLOAT,
        Blocks FLOAT,
        BlkSh FLOAT,
        BlkShSv FLOAT,
        Intt FLOAT,
        TklplusInt FLOAT, 
        TouDefPen FLOAT,
        TouDef3rd FLOAT,
        TouMid3rd FLOAT,
        TouAtt3rd FLOAT,
        DriSucc FLOAT,
        DriAtt FLOAT,
        DriSuccPorcent FLOAT,
        Carries FLOAT,
        CarTotDist FLOAT,
        CarPrgDist FLOAT,
        CarDis FLOAT,
        Rec FLOAT,
        RecPorcent FLOAT, 
        CrdY FLOAT,
        CrdR FLOAT,
        2CrdY FLOAT,
        Fls FLOAT,
        Fld FLOAT,
        Off FLOAT,
        Crs FLOAT,
        PKcon FLOAT,
        OG FLOAT,
        AerWon FLOAT,
        AerLost FLOAT,
        AerWonPorcent FLOAT
    
        )
        """
    cursor.execute(player_stats)
    player_info = f"""CREATE TABLE IF NOT EXISTS player_info (
        rk INT PRIMARY KEY AUTO_INCREMENT,
        player VARCHAR(255),
        nation VARCHAR(5),
        club VARCHAR(255),
        league VARCHAR(255),
        age INT,
        stats_id INT,
        posicion_id INT,
        value_id INT,
        CONSTRAINT FK_Jugador_info_Value FOREIGN KEY (value_id) REFERENCES player_value (id),
        CONSTRAINT FK_Jugador_info_Stats FOREIGN KEY (stats_id) REFERENCES player_stats (id),
        CONSTRAINT FK_Jugador_info_Posicion FOREIGN KEY (posicion_id) REFERENCES player_posicion (id)
        )
        """
    cursor.execute(player_info)

    conn.commit()
    conn.close()
    insert_data()

    print("Data loaded successfully.")

def insert_data():
    conn = connect_mysql()
    cursor = conn.cursor()
    print('Insertando posiciones')
    datos = [('DF',), ('MF',), ('FW',), ('GK',)]
    consulta = "INSERT INTO player_posicion (posicion) VALUES (%s)"
    cursor.executemany(consulta, datos)
    conn.commit()

    df = transformstats()
    players=df.drop(['Player','Nation','Squad','Comp','Age','Pos'], axis = 1)
    print('Insertando Stats')
    for index, row in players.iterrows():
        query = f"""INSERT INTO player_stats (MP, Starts, Min, Goals, Shots, SoT, SoTPorcent, GSh, ShoDist, ShoFK, ShoPK, PKatt, PasTotCmp, 
                PasTotCmpPorcent, PasShoCmpPorcent, PasMedCmpPorcent, PasLonCmpPorcent, Assists, PPA, CrsPA, PasAtt, PasFK, PasPress, Sw, PasCrs, CK, 
                PaswLeft, PaswRight, PasInt, SCA, GCA, TklDef3rd, TklMid3rd, TklAtt3rd, TklDri, TklDriPorcent, Press, Blocks, BlkSh, BlkShSv, Intt, 
                TklplusInt, TouDefPen, TouDef3rd, TouMid3rd, TouAtt3rd, DriSucc, DriAtt, DriSuccPorcent, Carries, CarTotDist, CarPrgDist, CarDis, Rec, 
                RecPorcent, CrdY, CrdR, 2CrdY, Fls, Fld, Off, Crs, PKcon, OG, AerWon, AerLost, AerWonPorcent) 
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, 
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"""
        cursor.execute(query, (row["MP"], row["Starts"], row["Min"], row["Goals"], row["Shots"], row["SoT"], row["SoT%"], row["G/Sh"], row["ShoDist"], row["ShoFK"], row["ShoPK"], row["PKatt"], row["PasTotCmp"], row["PasTotCmp%"], row["PasShoCmp%"], row["PasMedCmp%"], row["PasLonCmp%"], row["Assists"], row["PPA"], row["CrsPA"], row["PasAtt"], row["PasFK"], row["PasPress"], row["Sw"], row["PasCrs"], row["CK"], row["PaswLeft"], row["PaswRight"], row["PasInt"], row["SCA"], row["GCA"], row["TklDef3rd"], row["TklMid3rd"], row["TklAtt3rd"], row["TklDri"], row["TklDri%"], row["Press"], row["Blocks"], row["BlkSh"], row["BlkShSv"], row["Int"], row["Tkl+Int"], row["TouDefPen"], row["TouDef3rd"], row["TouMid3rd"], row["TouAtt3rd"], row["DriSucc"], row["DriAtt"], row["DriSucc%"], row["Carries"], row["CarTotDist"], row["CarPrgDist"], row["CarDis"], row["Rec"], row["Rec%"], row["CrdY"], row["CrdR"], row["2CrdY"], row["Fls"], row["Fld"], row["Off"], row["Crs"], row["PKcon"], row["OG"], row["AerWon"], row["AerLost"], row["AerWon%"]))
    conn.commit()

    values = transform_value()
    print('Insertando valores')
    for index, row in values.iterrows():
        query = f"INSERT INTO player_value (player, value) VALUES ( %s, %s)"
        cursor.execute(query, (row["Player"], row["Value"]))
    conn.commit()

    print('Insertando Info')
    for index, row in df.iterrows():
        query = f"INSERT INTO player_info ( player, nation, club, league, age,stats_id, posicion_id VALUES ( %s, %s, %s, %s, %s, %s, %s)"
        cursor.execute(query, (row["Player"], row["Nation"], row["Squad"], row["Comp"], row["Age"],row["stats_id"], row["Pos"]))
    conn.commit()



if __name__ == '__main__':
    connect_mysql()
    create_tables()

