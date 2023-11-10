import pandas as pd
import time

def init():
    player_value=transform_api()
    player_stats=transformstats()
    merge(player_value, player_stats) ### MERGED

def transformstats():
    df1 = pd.read_csv('PlayerStats.csv', sep=';',encoding='latin-1', index_col=0)
    players=df1.drop(['PasTotAtt','PasCmp','GcaPassLive','GcaPassDead','ScaPassLive','ScaPassDead','TB','PasLive','PasDead','PasHigh','PaswHead','PasLow','PasTotDist','PasTotPrgDist','PasShoCmp','PasShoAtt','PasMedCmp','PasMedAtt','PasLonCmp','PasLonAtt','PasAss','Pas3rd','PasProg','PasGround','PaswOther','PaswOther','PasOut','RecProg','CkIn','CkOut','CkStr','TI','PasOff','PasOut','Clr','TklWon','PresSucc','Press%','Born','G/SoT','Recov','PKwon','90s','Err','TklW','CPA','CarProg','GcaDrib','ScaDrib','BlkPass','PasBlocks','ScaSh','ScaFld','ScaDef','GcaSh','GcaFld','GcaDef','TklDriAtt','TklDriPast','Tkl','TouAttPen','TouLive','DriPast','Car3rd','CarMis','RecTarg','DriMegs','Touches','PresDef3rd','PresMid3rd','PresAtt3rd'], axis = 1)
    players['Pos2'] = players['Pos'].apply(lambda x: x[2:] if len(x) > 2 else None)
    players['Pos'] = players['Pos'].apply(lambda x: x[:2] if len(x) > 2 else x)
    posiciones_dict = {'DF': 1, 'MF': 2, 'FW': 3, 'GK': 4}
    players['Pos'] = players['Pos'].replace(posiciones_dict)

    players['stats_id'] = range(1, len(df1) + 1)
    players.to_csv('NewPlayerStats.csv')
  
    return players

def merge(player_value, player_stats):
    print('-------------------------MERGE--------------------------')
    df=pd.DataFrame(player_stats)
    #print(df)
    df2=pd.DataFrame(player_value)
    #print(df2)
    #comparacion = df["Player"].isin(df2["Player Name"])

    players = pd.merge(df, df2, how='left', left_on='Player', right_on='Player Name')
    players["player_id"]=players["stats_id"]
    result=players.drop(['stats_id','Player Name','Nationality','Team','Player Age','Position','Nation','Pos','Squad','Comp','Age','MP','Starts','Min','Goals','Shots','SoT','SoT%','G/Sh','ShoDist','ShoFK','ShoPK','PKatt','PasTotCmp','PasTotCmp%','PasShoCmp%','PasMedCmp%','PasLonCmp%','Assists','PPA','CrsPA','PasAtt','PasFK','PasPress','Sw','PasCrs','CK','PaswLeft','PaswRight','PasInt','SCA','GCA','TklDef3rd','TklMid3rd','TklAtt3rd','TklDri','TklDri%','Press','Blocks','Int','Tkl+Int','TouDefPen','TouDef3rd','TouMid3rd','TouAtt3rd','DriSucc','DriAtt','DriSucc%','Carries','CarTotDist','CarPrgDist','CarDis','Rec','Rec%','CrdY','CrdR','2CrdY','Fls','Fld','Off','Crs','PKcon','OG','AerWon','AerLost','AerWon%','Pos2','BlkSh','BlkShSv'], axis = 1)
    result["Value"] = result["Value"].str.replace("€", "").str.replace(".", "").str.replace("m", "000000").str.replace("k", "000")
    result = result.drop_duplicates(subset=['Player'])
    ids_to_drop = [1062, 1761]
    result = result.drop(result[result['player_id'].isin(ids_to_drop)].index)
    
    result['Value'].fillna(0, inplace=True)
    result['Value'] = result['Value'].astype('int64')
    result.to_csv('combinacion1.csv')
    return result

def transform_api():
    #print("------------------------------------API-----------------------------------")
    api = pd.read_csv('Players.csv', sep=';', index_col=0)
    df = pd.read_csv('PlayerValue.csv', sep=',', index_col=0)
    
    api= api.drop(['fecha','altura','pie','fichado','contrato','lugar'], axis=1)
    api.rename(columns={'nombre':'Player Name','edad':'Player Age','valor':'Value','posicion':'Position','club':'Team'}, inplace=True)
    new_order = ['Player Name','Player Age','Value','Team','Position']
    api = api.reindex(columns=new_order)
    combined_df = pd.concat([api, df])

    combined_df = combined_df.sort_values(['Player Name', 'Value'], ascending=[True, False]).drop_duplicates('Player Name')


    #print(combined_df)
    combined_df.to_csv('mrged.csv')
    return combined_df


if __name__ == '__main__':
    player_value=transform_api()
    player_stats=transformstats()
    merge(player_value, player_stats) ### MERGED
