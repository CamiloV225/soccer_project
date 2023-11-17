import pandas as pd
import json
import logging
import numpy as np
from services.db import insert_data
from services.producer import kafka_producer
from sklearn.model_selection import train_test_split
from sklearn.metrics import r2_score, mean_squared_error
from sklearn.ensemble import HistGradientBoostingRegressor
import joblib

def connect_db():
    pass
def read_csv1():
    stats = pd.read_csv('/home/camilo/soccer/data/PlayerStats.csv', sep=';', encoding="latin1",index_col=0)
    print(stats)
    return stats.to_json(orient='records')

def read_csv2():
    values = pd.read_csv('/home/camilo/soccer/data/PlayerValue.csv', sep=',',index_col=0)
    print(values)
    return values.to_json(orient='records')

def read_api():
    api = pd.read_csv('/home/camilo/soccer/data/PlayerApi.csv', sep=';',index_col=0)
    print(api)
    return api.to_json(orient='records')

def transform_csv1(**kwargs):
    logging.info("-------------------------Transforming PlayerStats---------------------------")
    ti = kwargs["ti"]
    str_data = ti.xcom_pull(task_ids="read_csv1")
    json_data = json.loads(str_data)
    df = pd.json_normalize(data=json_data)

#Transformaciones del CSV1   
    players=df.drop(['PasTotAtt','PasCmp','GcaPassLive','GcaPassDead','ScaPassLive','ScaPassDead','TB','PasLive','PasDead','PasHigh','PaswHead','PasLow','PasTotDist','PasTotPrgDist','PasShoCmp','PasShoAtt','PasMedCmp','PasMedAtt','PasLonCmp','PasLonAtt','PasAss','Pas3rd','PasProg','PasGround','PaswOther','PaswOther','PasOut','RecProg','CkIn','CkOut','CkStr','TI','PasOff','PasOut','Clr','TklWon','PresSucc','Press%','Born','G/SoT','Recov','PKwon','90s','Err','TklW','CPA','CarProg','GcaDrib','ScaDrib','BlkPass','PasBlocks','ScaSh','ScaFld','ScaDef','GcaSh','GcaFld','GcaDef','TklDriAtt','TklDriPast','Tkl','TouAttPen','TouLive','DriPast','Car3rd','CarMis','RecTarg','DriMegs','Touches','PresDef3rd','PresMid3rd','PresAtt3rd'], axis = 1)
    players['Pos2'] = players['Pos'].apply(lambda x: x[2:] if len(x) > 2 else np.nan)
    players['Pos'] = players['Pos'].apply(lambda x: x[:2] if len(x) > 2 else x)
    posiciones_dict = {'DF': 1, 'MF': 2, 'FW': 3, 'GK': 4}
    players['Pos'] = players['Pos'].replace(posiciones_dict)
  
    return players.to_json(orient='records')

def transform_api(**kwargs):
    logging.info("-------------------------Transforming PlayerApi and PlayerValue---------------------------")

# Obteniendo datos de la API - PlayerApi
    ti = kwargs["ti"]
    str_data = ti.xcom_pull(task_ids="read_api")
    json_data = json.loads(str_data)
    api = pd.json_normalize(data=json_data)

# Obteniendo datos del CSV2 - PlayerValue
    str_data = ti.xcom_pull(task_ids="read_csv2")
    json_data = json.loads(str_data)
    value = pd.json_normalize(data=json_data)

# Se Ordenan los datos para la union
    api = api.drop(['fecha','altura','pie','lugar','posicion','fichado','contrato','club'], axis=1)
    api.rename(columns={'nombre':'Player Name','edad':'Player Age','valor':'Value'}, inplace=True)
    new_order = ['Player Name','Player Age','Value']
    api = api.reindex(columns=new_order)

    value = value.drop(['Nationality','Position','Team'], axis=1)
    value["Value"] = value["Value"].str.replace("€", "").str.replace(".", "").str.replace("m", "000000").str.replace("k", "000")
    value['Value'].replace(' ', np.nan, inplace=True)
    value['Value'].fillna(0, inplace=True)
    value['Value'] = value['Value'].astype('int64')

# Concatena ambos CSV
    combined_df = pd.concat([api, value], ignore_index=True)
    combined_df = combined_df.sort_values(['Player Name', 'Value'], ascending=[True, False]).drop_duplicates('Player Name')
    #combined_df = combined_df.dropna()
    combined_df = combined_df.reset_index(drop=True)
    combined_df = combined_df.sort_index(ascending=True)
    combined_df.to_csv('/home/camilo/soccer/data/values.csv')
    return combined_df.to_json(orient='records')

def merge(**kwargs):
    logging.info("-------------------------Merging CSV files---------------------------")

    ti = kwargs["ti"]
    str_data = ti.xcom_pull(task_ids="transform_csv1")
    json_data = json.loads(str_data)
    player_stats = pd.json_normalize(data=json_data)

    str_data = ti.xcom_pull(task_ids="transform_api")
    json_data = json.loads(str_data)
    player_value = pd.json_normalize(data=json_data)

# Adecuación de columnas y cambios de tipo de dato
    players = pd.merge(player_stats,player_value,how='outer', left_on='Player', right_on='Player Name')
    players['stats_id'] = range(1, len(players) + 1)
    players["player_id"]=players["stats_id"]
    players=players.drop(['Player Name','Player Age'], axis = 1)
    players['Value'].replace(' ', np.nan, inplace=True)
    players['Value'].fillna(0, inplace=True)
    players['Pos2'].fillna('N/A', inplace=True)
    players['Nation'].fillna('N/A', inplace=True)
    players['Age'].fillna(0, inplace=True)
    players.dropna(subset=['Player'], inplace=True)
    #players.to_csv('/home/camilo/soccer/data/NewPlayerInfo1.csv')
    new_order = ['Player','Value','Nation','Pos','Pos2','Squad','Comp','Age','MP','Starts','Min','Goals','Shots','SoT','SoT%','G/Sh','ShoDist','ShoFK','ShoPK','PKatt','PasTotCmp','PasTotCmp%','PasShoCmp%','PasMedCmp%','PasLonCmp%','Assists','PPA','CrsPA','PasAtt','PasFK','PasPress','Sw','PasCrs','CK','PaswLeft','PaswRight','PasInt','SCA','GCA','TklDef3rd','TklMid3rd','TklAtt3rd','TklDri','TklDri%','Press','Blocks','BlkSh','BlkShSv','Int','Tkl+Int','TouDefPen','TouDef3rd','TouMid3rd','TouAtt3rd','DriSucc','DriAtt','DriSucc%','Carries','CarTotDist','CarPrgDist','CarDis','Rec','Rec%','CrdY','CrdR','2CrdY','Fls','Fld','Off','Crs','PKcon','OG','AerWon','AerLost','AerWon%','stats_id','player_id']
    players = players.reindex(columns=new_order)
    players['Age'] = players['Age'].astype('int64')
    players['Pos'] = players['Pos'].astype('int64')
    players['Value'] = players['Value'].astype('int64')
    pd.set_option('display.max_rows', players.shape[0]+1)
    players.to_csv('/home/camilo/soccer/data/NewPlayerInfo.csv')
    print(players.isnull().sum())

    return players.to_json(orient='records')

def predicting_value(**kwargs):
    ti = kwargs["ti"]
    str_data = ti.xcom_pull(task_ids="merge")
    json_data = json.loads(str_data)
    players = pd.json_normalize(data=json_data)
    players = pd.DataFrame(players)

    regressor = joblib.load('/home/camilo/soccer/modelo.pkl')
    players['predicted_value'] =  regressor.predict(players[['Min', 'Starts', 'MP', 'CarPrgDist', 'CarTotDist', 'Rec' ,'Carries','SCA','TouAtt3rd','Shots','PPA']])
    

    players.to_csv('/home/camilo/soccer/data/resultadospred.csv')
    return players.to_json(orient='records')
    

def load(**kwargs):
    logging.info("-------------------------Loading data into Postgres---------------------------")
    ti = kwargs["ti"]
    str_data = ti.xcom_pull(task_ids="predicting_value")
    json_data = json.loads(str_data)
    player_stats = pd.json_normalize(data=json_data)

    #insert_data(player_stats)

    

def kafka(**kwargs):
    logging.info("-------------------------Kafka------------------------")
    kafka_producer()


