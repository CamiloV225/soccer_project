import psycopg2
import json
import logging
import pandas as pd

def connect_postgres():
    with open('/home/camilo/soccer/services/db_config.json') as f:
        dbfile = json.load(f)
    
    connection = psycopg2.connect(
        host=dbfile["host"],
        user=dbfile["user"],
        password=dbfile["password"],
        database=dbfile["database"],
        port=5432
    )
    
    return connection

def create_tables():
    conn = connect_postgres()
    cursor = conn.cursor()
    PlayerPosition = f"""CREATE TABLE IF NOT EXISTS player_posicion (
        id SERIAL PRIMARY KEY,
        posicion VARCHAR(10)
        )
        """
    cursor.execute(PlayerPosition)
    player_stats = f"""CREATE TABLE IF NOT EXISTS player_stats (
        id SERIAL PRIMARY KEY,
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
        SCrdY FLOAT,
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
        rk SERIAL PRIMARY KEY,
        player VARCHAR(255),
        nation VARCHAR(5),
        club VARCHAR(255),
        league VARCHAR(255),
        age INT,
        posicion_id INT,
        CONSTRAINT FK_Jugador_info_Stats FOREIGN KEY (rk) REFERENCES player_stats (id),
        CONSTRAINT FK_Jugador_info_Posicion FOREIGN KEY (posicion_id) REFERENCES player_posicion (id)
        )
        """
    cursor.execute(player_info)
    PlayerValue = f"""CREATE TABLE IF NOT EXISTS player_value (
        id SERIAL PRIMARY KEY,
        player VARCHAR(255),
        value BIGINT,
        predicted_value BIGINT,
        CONSTRAINT FK_Jugador_info_value FOREIGN KEY (id) REFERENCES player_info (rk)
        )
        """
    cursor.execute(PlayerValue)

    #create_index = "CREATE INDEX IF NOT EXISTS idx_player_id ON player_value (player_id)"
    #cursor.execute(create_index)
    
    #CONSTRAINT FK_Jugador_info_value FOREIGN KEY (value_id) REFERENCES player_value (player_id)

    conn.commit()
    conn.close()

    return 

def insert_data(player_stats):
    conn = connect_postgres()
    cursor = conn.cursor()
    create_tables()
    logging.info('---------------------Inserting Positions-------------------')
    datos = [('DF',), ('MF',), ('FW',), ('GK',)]
    consulta = "INSERT INTO player_posicion (posicion) VALUES (%s)"
    cursor.executemany(consulta, datos)
    conn.commit()

    #df = transformstats()
    stats = player_stats.drop(['Player','Value','Nation','Pos','Pos2','Squad','Comp','Age','player_id','predicted_value'], axis = 1)
    stats = stats.set_index('stats_id')
    logging.info('-------------------------Inserting Stats-----------------------')
    for index, row in stats.iterrows():
        query = f"""INSERT INTO player_stats (MP, Starts, Min, Goals, Shots, SoT, SoTPorcent, GSh, ShoDist, ShoFK, ShoPK, PKatt, PasTotCmp, 
                PasTotCmpPorcent, PasShoCmpPorcent, PasMedCmpPorcent, PasLonCmpPorcent, Assists, PPA, CrsPA, PasAtt, PasFK, PasPress, Sw, PasCrs, CK, 
                PaswLeft, PaswRight, PasInt, SCA, GCA, TklDef3rd, TklMid3rd, TklAtt3rd, TklDri, TklDriPorcent, Press, Blocks, BlkSh, BlkShSv, Intt, 
                TklplusInt, TouDefPen, TouDef3rd, TouMid3rd, TouAtt3rd, DriSucc, DriAtt, DriSuccPorcent, Carries, CarTotDist, CarPrgDist, CarDis, Rec, 
                RecPorcent, CrdY, CrdR, SCrdY, Fls, Fld, Off, Crs, PKcon, OG, AerWon, AerLost, AerWonPorcent) 
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, 
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"""
        cursor.execute(query, (row["MP"], row["Starts"], row["Min"], row["Goals"], row["Shots"], row["SoT"], row["SoT%"], row["G/Sh"], row["ShoDist"], row["ShoFK"], row["ShoPK"], row["PKatt"], row["PasTotCmp"], row["PasTotCmp%"], row["PasShoCmp%"], row["PasMedCmp%"], row["PasLonCmp%"], row["Assists"], row["PPA"], row["CrsPA"], row["PasAtt"], row["PasFK"], row["PasPress"], row["Sw"], row["PasCrs"], row["CK"], row["PaswLeft"], row["PaswRight"], row["PasInt"], row["SCA"], row["GCA"], row["TklDef3rd"], row["TklMid3rd"], row["TklAtt3rd"], row["TklDri"], row["TklDri%"], row["Press"], row["Blocks"], row["BlkSh"], row["BlkShSv"], row["Int"], row["Tkl+Int"], row["TouDefPen"], row["TouDef3rd"], row["TouMid3rd"], row["TouAtt3rd"], row["DriSucc"], row["DriAtt"], row["DriSucc%"], row["Carries"], row["CarTotDist"], row["CarPrgDist"], row["CarDis"], row["Rec"], row["Rec%"], row["CrdY"], row["CrdR"], row["2CrdY"], row["Fls"], row["Fld"], row["Off"], row["Crs"], row["PKcon"], row["OG"], row["AerWon"], row["AerLost"], row["AerWon%"]))
    conn.commit()
    
    logging.info('-----------------------Inserting Player´s Info---------------------------')
    info = player_stats.drop(['Value','predicted_value','stats_id','player_id','Pos2','MP','Starts','Min','Goals','Shots','SoT','SoT%','G/Sh','ShoDist','ShoFK','ShoPK','PKatt','PasTotCmp','PasTotCmp%','PasShoCmp%','PasMedCmp%','PasLonCmp%','Assists','PPA','CrsPA','PasAtt','PasFK','PasPress','Sw','PasCrs','CK','PaswLeft','PaswRight','PasInt','SCA','GCA','TklDef3rd','TklMid3rd','TklAtt3rd','TklDri','TklDri%','Press','Blocks','BlkSh','BlkShSv','Int','Tkl+Int','TouDefPen','TouDef3rd','TouMid3rd','TouAtt3rd','DriSucc','DriAtt','DriSucc%','Carries','CarTotDist','CarPrgDist','CarDis','Rec','Rec%','CrdY','CrdR','2CrdY','Fls','Fld','Off','Crs','PKcon','OG','AerWon','AerLost','AerWon%'], axis = 1)
    
    
    info.to_csv('/home/camilo/soccer/data/muestradb.csv')
    for index, row in info.iterrows():
        query = f"INSERT INTO player_info ( player, nation, club, league, age, posicion_id) VALUES ( %s, %s, %s, %s, %s, %s)"
        cursor.execute(query, (row["Player"], row["Nation"], row["Squad"], row["Comp"], row["Age"], row["Pos"]))
    conn.commit()

    logging.info('------------------------Inserting Values---------------------')
    value = player_stats.drop(['stats_id','Nation','Squad','Comp','Age','Pos','Pos2','MP','Starts','Min','Goals','Shots','SoT','SoT%','G/Sh','ShoDist','ShoFK','ShoPK','PKatt','PasTotCmp','PasTotCmp%','PasShoCmp%','PasMedCmp%','PasLonCmp%','Assists','PPA','CrsPA','PasAtt','PasFK','PasPress','Sw','PasCrs','CK','PaswLeft','PaswRight','PasInt','SCA','GCA','TklDef3rd','TklMid3rd','TklAtt3rd','TklDri','TklDri%','Press','Blocks','BlkSh','BlkShSv','Int','Tkl+Int','TouDefPen','TouDef3rd','TouMid3rd','TouAtt3rd','DriSucc','DriAtt','DriSucc%','Carries','CarTotDist','CarPrgDist','CarDis','Rec','Rec%','CrdY','CrdR','2CrdY','Fls','Fld','Off','Crs','PKcon','OG','AerWon','AerLost','AerWon%'], axis = 1)
    for index, row in value.iterrows():
        query = f"INSERT INTO player_value (player, value, predicted_value) VALUES ( %s, %s, %s)"
        cursor.execute(query, (row["Player"], row["Value"], row["predicted_value"]))
    conn.commit()


if __name__ == '__main__':
    create_tables()