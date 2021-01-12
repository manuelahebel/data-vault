"""
This script contains all code for inserts into the Data Vault model.
Created by all group members; contributions equal.
"""

import pandas as pd
import hashlib
from datetime import datetime
import sqlite3
import ProjektgruppeB_Module_Data_Preparation as DataPrep

###hash_function
def hash_function (text):
    text = text.replace(" ", "")
    text = text.upper()
    text = text.encode("utf-8")
    key = hashlib.md5(text).hexdigest()
    return key

####Function used for inserting into tables
def fill_tables (db_table, df, rs = "1", link_no_hash = 0, sat_to_hash = 0):
    """
    db_table database table
    df = table to be inserted
    """
    ldts = str(datetime.now())[:-7]
    edts = '9999-12-31 00:00:00'
    db = sqlite3.connect(
    "/Users/Jochen/Desktop/Kurse/alfatraining/Data Engineering/Projekt_DataVault_GruppeB.db")
    cursor = db.cursor()
    
    ###get column names from database table
    cursor.execute(f'PRAGMA table_info({db_table})')
    data = cursor.fetchall()
    column_list = []
    for d in data:
        column_list.append(f"{d[1]}")
    
    ###what kind of table are dealing with?
    if len(column_list) == 4:
        table_type = "hub"
    elif len(column_list) < 8:
        table_type = "link"
    else:
        table_type = "sat"
    
    ###Define what to hash
    if table_type in ["hub", "link"]:
        to_hashHK = df
    if table_type == "sat":
        to_hashHK = df.iloc[:,sat_to_hash]
        to_hashFK = df.iloc[:,0]
        to_hashHD = df.iloc[:,1:]
    
    for i in range(len(df)):
        ###Hashing
        hk_string = "".join([*to_hashHK.iloc[i],rs])
        if table_type == "sat":
            hk_string += ldts
        hk = hash_function(hk_string)
        
        if table_type == "sat":
            fk = hash_function("".join([*to_hashFK.iloc[i],rs]))
            hd = hash_function("".join([*to_hashHD.iloc[i].map(str)]))

        if table_type == "link":
            values = []
            for j in range(len(to_hashHK.iloc[i])-link_no_hash):
                hk_string = "".join([to_hashHK.iloc[i,j],rs])
                values.append(hash_function(hk_string))
        
        ###Definition values
        if table_type == "hub":
            values = [hk,df["id"][i], ldts, rs]
        elif table_type == "link":
            values.insert(0, hk)
            if link_no_hash >0:
                values.append(df.iloc[i,-link_no_hash])
            values.append(ldts)
            values.append(rs)
        else:
            values = [hk, fk, ldts, edts, rs, hd]
            for j in range(1,len(df.columns)):
                values.append(df.iloc[i,j])
        
        ###Insertion
        column_string = ", ".join(column_list)
        value_string = ", ".join('"{0}"'.format(v) for v in values)
        sqlquery = f'INSERT INTO {db_table} ({column_string}) VALUES ({value_string})'
        try:
            cursor.execute(sqlquery)
        except:
            print("line",i,"was a duplicate")
            pass
        db.commit()
        print(i)
    db.close()

###Connect to databases
db = sqlite3.connect(
    "/Users/Jochen/Desktop/Kurse/alfatraining/Data Engineering/Projekt_DataVault_GruppeB.db")

cursor = db.cursor()

cursor.execute('ATTACH DATABASE "database.sqlite" AS Source')


###Fill the hubs
Hub_League = pd.read_sql_query("""SELECT id FROM Source.League;""", db).astype(str)
Hub_League.head()
Hub_Player = pd.read_sql_query("""SELECT id FROM Source.Player;""", db).astype(str)
Hub_Player.head()
Hub_Team = pd.read_sql_query("""SELECT id FROM Source.Team;""", db).astype(str)
Hub_Team.head()

fill_tables(db_table = "Hub_League", df = Hub_League)
fill_tables(db_table = "Hub_Player", df = Hub_Player)
fill_tables(db_table = "Hub_Team", df = Hub_Team)

###Fill satellites of hubs
Sat_Team = pd.read_sql_query("""SELECT Source.Team.id, Source.Team.team_long_name, Source.Team.team_short_name, 'soccer', 'm', 1 FROM Hub_Team
	LEFT JOIN Source.Team ON Hub_Team.bk = Source.Team.id;""", db)
Sat_Team["id"] = Sat_Team["id"].astype(str)
Sat_Team.head()

fill_tables(db_table = "Sat_Team", df = Sat_Team)

Sat_Team_Soccer = pd.read_sql_query("""SELECT DISTINCT Source.Team.id, 
Source.Team_Attributes.team_fifa_api_id,
Source.Team_Attributes.team_api_id,
Source.Team_Attributes.date,
Source.Team_Attributes.buildUpPlaySpeed,
Source.Team_Attributes.buildUpPlaySpeedClass,
Source.Team_Attributes.buildUpPlayDribbling,
Source.Team_Attributes.buildUpPlayDribblingClass,
Source.Team_Attributes.buildUpPlayPassing,
Source.Team_Attributes.buildUpPlayPassingClass,
Source.Team_Attributes.buildUpPlayPositioningClass,
Source.Team_Attributes.chanceCreationPassing,
Source.Team_Attributes.chanceCreationPassingClass,
Source.Team_Attributes.chanceCreationCrossing,
Source.Team_Attributes.chanceCreationCrossingClass,
Source.Team_Attributes.chanceCreationShooting,
Source.Team_Attributes.chanceCreationShootingClass,
Source.Team_Attributes.chanceCreationPositioningClass,
Source.Team_Attributes.defencePressure,
Source.Team_Attributes.defencePressureClass,
Source.Team_Attributes.defenceAggression,
Source.Team_Attributes.defenceAggressionClass,
Source.Team_Attributes.defenceTeamWidth,
Source.Team_Attributes.defenceTeamWidthClass,
Source.Team_Attributes.defenceDefenderLineClass
FROM Source.Team_Attributes
LEFT JOIN Source.Team ON Source.Team_Attributes.team_fifa_api_id = Source.Team.team_fifa_api_id
AND Source.Team_Attributes.team_api_id = Source.Team.team_api_id;""", db)

Sat_Team_Soccer["id"] = Sat_Team_Soccer["id"].astype(str)
Sat_Team_Soccer["date"] = Sat_Team_Soccer["date"].astype(str)

fill_tables(db_table = "Sat_Team_Soccer", df = Sat_Team_Soccer, sat_to_hash = [0, 3])

Sat_League = pd.read_sql_query("""SELECT Source.League.id, Source.League.name, Source.Country.name AS country FROM Hub_League
	LEFT JOIN Source.League ON Source.League.id = Hub_League.bk
		LEFT JOIN Source.Country ON Source.Country.id = Source.League.country_id;""", db)
Sat_League["id2"] = Sat_League["id"].astype(str)
Sat_League.drop("id",axis = 1, inplace = True)
Sat_League.rename({"id2":"id"}, axis = 1, inplace = True)
Sat_League = Sat_League[["id","name","country"]]
Sat_League.head()

fill_tables(db_table = "Sat_League", df = Sat_League)

Sat_Player = pd.read_sql_query("""SELECT Source.Player.id, Source.Player.player_name, Source.Player.birthday, Source.Player.height, Source.Player.weight
FROM Source.Player;""", db)
Sat_Player["id"] = Sat_Player["id"].astype(str)
Sat_Player.head()

fill_tables(db_table = "Sat_Player", df = Sat_Player)

Sat_Player_Soccer = pd.read_sql_query("""SELECT DISTINCT Source.Player.id, 
Source.Player_Attributes.player_api_id,
Source.Player_Attributes.player_fifa_api_id,
Source.Player_Attributes.date,
Source.Player_Attributes.overall_rating,
Source.Player_Attributes.potential,
Source.Player_Attributes.preferred_foot,
Source.Player_Attributes.attacking_work_rate,
Source.Player_Attributes.defensive_work_rate,
Source.Player_Attributes.crossing,
Source.Player_Attributes.finishing,
Source.Player_Attributes.heading_accuracy,
Source.Player_Attributes.short_passing,
Source.Player_Attributes.volleys,
Source.Player_Attributes.dribbling,
Source.Player_Attributes.curve,
Source.Player_Attributes.free_kick_accuracy,
Source.Player_Attributes.long_passing,
Source.Player_Attributes.ball_control,
Source.Player_Attributes.acceleration,
Source.Player_Attributes.sprint_speed,
Source.Player_Attributes.agility,
Source.Player_Attributes.reactions,
Source.Player_Attributes.balance,
Source.Player_Attributes.shot_power,
Source.Player_Attributes.stamina,
Source.Player_Attributes.strength,
Source.Player_Attributes.long_shots,
Source.Player_Attributes.aggression,
Source.Player_Attributes.interceptions,
Source.Player_Attributes.vision,
Source.Player_Attributes.penalties,
Source.Player_Attributes.marking,
Source.Player_Attributes.standing_tackle,
Source.Player_Attributes.sliding_tackle,
Source.Player_Attributes.gk_diving,
Source.Player_Attributes.gk_handling,
Source.Player_Attributes.gk_kicking,
Source.Player_Attributes.gk_positioning,
Source.Player_Attributes.gk_reflexes

FROM Source.Player_Attributes
LEFT JOIN Source.Player ON Source.Player_Attributes.player_fifa_api_id = Source.Player.player_fifa_api_id
AND Source.Player_Attributes.player_api_id = Source.Player.player_api_id;""", db)

Sat_Player_Soccer["id"] = Sat_Player_Soccer["id"].astype(str)
Sat_Player_Soccer["date"] = Sat_Player_Soccer["date"].astype(str)

fill_tables(db_table = "Sat_Player_Soccer", df = Sat_Player_Soccer, sat_to_hash = [0, 3])

###Fill first link and its satellites
Link_Match = pd.read_sql_query("""SELECT DISTINCT Source.Team_1.id AS home_hk, Source.Team_2.id AS away_hk, Source.Match.league_id, Source.Match.date FROM Source.Match
LEFT JOIN Source.Team AS Team_1 ON Team_1.team_api_id = Source.Match.home_team_api_id
LEFT JOIN Source.Team AS Team_2 ON Team_2.team_api_id = Source.Match.away_team_api_id""", db)
Link_Match.head()
Link_Match = Link_Match.astype(str)

fill_tables(db_table = "Link_Match", df = Link_Match, link_no_hash = 1)

Sat_Link_Match = pd.read_sql_query("""SELECT DISTINCT Source.Team_1.id AS home_hk, 
Source.Team_2.id AS away_hk, 
Source.Match.league_id, 
Source.Match.date,
Source.Match.id,
Source.Country.name,
Source.Match.season,
Source.Match.stage,
Source.Match.home_team_goal,
Source.Match.away_team_goal,
Source.Match.B365H,
Source.Match.B365D,
Source.Match.B365A,
Source.Match.BWH ,
Source.Match.BWD,
Source.Match.BWA,
Source.Match.IWH,
Source.Match.IWD,
Source.Match.IWA,
Source.Match.LBH,
Source.Match.LBD,
Source.Match.LBA,
Source.Match.PSH,
Source.Match.PSD,
Source.Match.PSA,
Source.Match.WHH,
Source.Match.WHD,
Source.Match.WHA,
Source.Match.SJH,
Source.Match.SJD,
Source.Match.SJA,
Source.Match.VCH,
Source.Match.VCD,
Source.Match.VCA,
Source.Match.GBH,
Source.Match.GBD,
Source.Match.GBA,
Source.Match.BSH,
Source.Match.BSD,
Source.Match.BSA

FROM Source.Match
LEFT JOIN Source.Team AS Team_1 ON Team_1.team_api_id = Source.Match.home_team_api_id
LEFT JOIN Source.Team AS Team_2 ON Team_2.team_api_id = Source.Match.away_team_api_id
LEFT JOIN Source.Country ON Source.Match.country_id = Source.Country.id;""", db)

temp_Sat_Link_Match = Sat_Link_Match["home_hk"].astype(str)+Sat_Link_Match["away_hk"].astype(str)+\
                        Sat_Link_Match["league_id"].astype(str)+Sat_Link_Match["date"].astype(str)
Sat_Link_Match.drop(["home_hk", "away_hk", "league_id", "date"],axis = 1, inplace = True)
Sat_Link_Match = pd.concat([temp_Sat_Link_Match,Sat_Link_Match], axis = 1).reset_index(drop = True)

fill_tables(db_table = "Sat_Link_Match", df = Sat_Link_Match)

Sat_Link_Match_Soccer = pd.read_sql_query("""SELECT DISTINCT 
Source.Team_1.id AS home_hk, 
Source.Team_2.id AS away_hk, 
Source.Match.league_id, 
Source.Match.date,
Source.Match.id, 
Source.Match.match_api_id,
Source.Match.goal,
Source.Match.shoton,
Source.Match.shotoff,
Source.Match.foulcommit,
Source.Match.card,
Source.Match.cross,
Source.Match.corner,
Source.Match.possession 

FROM Source.Match
LEFT JOIN Source.Team AS Team_1 ON Team_1.team_api_id = Source.Match.home_team_api_id
LEFT JOIN Source.Team AS Team_2 ON Team_2.team_api_id = Source.Match.away_team_api_id;""", db)

temp_Sat_Link_Match_Soccer = Sat_Link_Match_Soccer["home_hk"].astype(str)+Sat_Link_Match_Soccer["away_hk"].astype(str)+\
                        Sat_Link_Match_Soccer["league_id"].astype(str)+Sat_Link_Match_Soccer["date"].astype(str)
Sat_Link_Match_Soccer.drop(["home_hk", "away_hk", "league_id", "date"],axis = 1, inplace = True)
Sat_Link_Match_Soccer = pd.concat([temp_Sat_Link_Match_Soccer,Sat_Link_Match_Soccer], axis = 1).reset_index(drop = True)

fill_tables(db_table = "Sat_Link_Match_Soccer", df = Sat_Link_Match_Soccer)

###Fill second link and its satellites
###here the helper functions from the loaded module are used
Query_Link_Match_Player = pd.read_sql_query("""SELECT DISTINCT Source.Team_1.id AS home_hk, 
Source.Team_2.id AS away_hk, 
Source.Match.league_id ,
Source.Match.date,
Source.Match.home_player_1 ,
Source.Match.home_player_2 ,
Source.Match.home_player_3 ,
Source.Match.home_player_4 ,
Source.Match.home_player_5 ,
Source.Match.home_player_6 ,
Source.Match.home_player_7 ,
Source.Match.home_player_8 ,
Source.Match.home_player_9 ,
Source.Match.home_player_10 ,
Source.Match.home_player_11 ,
Source.Match.away_player_1 ,
Source.Match.away_player_2 ,
Source.Match.away_player_3 ,
Source.Match.away_player_4 ,
Source.Match.away_player_5 ,
Source.Match.away_player_6 ,
Source.Match.away_player_7 ,
Source.Match.away_player_8 ,
Source.Match.away_player_9 ,
Source.Match.away_player_10 ,
Source.Match.away_player_11
FROM Source.Match 
LEFT JOIN Source.Team AS Team_1 ON Team_1.team_api_id = Source.Match.home_team_api_id
LEFT JOIN Source.Team AS Team_2 ON Team_2.team_api_id = Source.Match.away_team_api_id
WHERE home_player_X1 IS NOT NULL AND goal IS NOT NULL LIMIT 500;""", db)

Link_Match_Player = DataPrep.all_match_players(Query_Link_Match_Player)
Link_Match_Player.dropna(axis = 0, inplace=True)

temp_Link_Match_Player = Link_Match_Player["home_hk"].astype(str)+Link_Match_Player["away_hk"].astype(str)+\
                        Link_Match_Player["league_id"].astype(str)+Link_Match_Player["date"].astype(str)
Link_Match_Player.drop(["home_hk", "away_hk", "league_id", "date"],axis = 1, inplace = True)
Link_Match_Player = pd.concat([temp_Link_Match_Player,Link_Match_Player], axis = 1).reset_index(drop = True)

Link_Match_Player["player_api_id"] = Link_Match_Player["player_api_id"].astype(int)

temp_player_ids = pd.read_sql_query("""SELECT id, player_api_id FROM Source.Player""", db)

Link_Match_Player['bk_player_id'] = (Link_Match_Player.merge(temp_player_ids, left_on='player_api_id', 
right_on='player_api_id', how='left')['id'])
Link_Match_Player.drop(["player_api_id"],axis = 1, inplace = True)

Link_Match_Player["bk_player_id"] = Link_Match_Player["bk_player_id"].astype(str)

Link_Match_Player = Link_Match_Player.drop_duplicates()

fill_tables(db_table = "Link_Match_Player", df = Link_Match_Player)

df_Match_for_BV = pd.read_sql_query("""SELECT DISTINCT Source.Team_1.id AS home_hk, 
Source.Team_2.id AS away_hk, 
Source.Match.league_id ,
Source.Match.date,
Source.Match.home_player_1 ,
Source.Match.home_player_2 ,
Source.Match.home_player_3 ,
Source.Match.home_player_4 ,
Source.Match.home_player_5 ,
Source.Match.home_player_6 ,
Source.Match.home_player_7 ,
Source.Match.home_player_8 ,
Source.Match.home_player_9 ,
Source.Match.home_player_10 ,
Source.Match.home_player_11 ,
Source.Match.away_player_1 ,
Source.Match.away_player_2 ,
Source.Match.away_player_3 ,
Source.Match.away_player_4 ,
Source.Match.away_player_5 ,
Source.Match.away_player_6 ,
Source.Match.away_player_7 ,
Source.Match.away_player_8 ,
Source.Match.away_player_9 ,
Source.Match.away_player_10 ,
Source.Match.away_player_11 ,
Source.Match.goal ,
Source.Match.shoton ,
Source.Match.shotoff ,
Source.Match.foulcommit ,
Source.Match.card ,
Source.Match.cross ,
Source.Match.corner 
FROM Source.Match 
LEFT JOIN Source.Team AS Team_1 ON Team_1.team_api_id = Source.Match.home_team_api_id
LEFT JOIN Source.Team AS Team_2 ON Team_2.team_api_id = Source.Match.away_team_api_id
WHERE home_player_X1 IS NOT NULL AND goal IS NOT NULL LIMIT 500;""", db)

Sat_Link_Match_Player_Soccer_BV = DataPrep.prep_match_players_BV (df_Match_for_BV)
Sat_Link_Match_Player_Soccer_BV.drop("owngoals",axis = 1, inplace = True)

Sat_Link_Match_Player_Soccer_BV.dropna(axis = 0, inplace=True)

Sat_Link_Match_Player_Soccer_BV["player_api_id"] = Sat_Link_Match_Player_Soccer_BV["player_api_id"].astype(int)

temp_player_ids = pd.read_sql_query("""SELECT id, player_api_id FROM Source.Player""", db)

Sat_Link_Match_Player_Soccer_BV['bk_player_id'] = (Sat_Link_Match_Player_Soccer_BV.merge(temp_player_ids, left_on='player_api_id', 
right_on='player_api_id', how='left')['id'])

Sat_Link_Match_Player_Soccer_BV.dropna(axis = 0, inplace=True)

Sat_Link_Match_Player_Soccer_BV.drop(["player_api_id"],axis = 1, inplace = True)

Sat_Link_Match_Player_Soccer_BV["bk_player_id"] = Sat_Link_Match_Player_Soccer_BV["bk_player_id"].astype(int)

temp_Sat_Link_Match_Player_Soccer_BV = Sat_Link_Match_Player_Soccer_BV["home_hk"].astype(str)+Sat_Link_Match_Player_Soccer_BV["away_hk"].astype(str)+\
                        Sat_Link_Match_Player_Soccer_BV["league_id"].astype(str)+Sat_Link_Match_Player_Soccer_BV["date"].astype(str)+Sat_Link_Match_Player_Soccer_BV["bk_player_id"].astype(str)
Sat_Link_Match_Player_Soccer_BV.drop(["home_hk", "away_hk", "league_id", "date","bk_player_id"],axis = 1, inplace = True)
Sat_Link_Match_Player_Soccer_BV = pd.concat([temp_Sat_Link_Match_Player_Soccer_BV,Sat_Link_Match_Player_Soccer_BV], axis = 1).reset_index(drop = True)

Sat_Link_Match_Player_Soccer_BV = Sat_Link_Match_Player_Soccer_BV.drop_duplicates()

fill_tables(db_table = "Sat_Link_Match_Player_Soccer_BV", df = Sat_Link_Match_Player_Soccer_BV)

df_Match_for_RV = pd.read_sql_query("""SELECT Source.Team_1.id AS home_hk, 
Source.Team_2.id AS away_hk, 
Source.Match.league_id ,
Source.Match.date ,
Source.Match.home_team_api_id ,
Source.Match.away_team_api_id ,
Source.Match.home_player_X1 ,
Source.Match.home_player_X2 ,
Source.Match.home_player_X3 ,
Source.Match.home_player_X4 ,
Source.Match.home_player_X5 ,
Source.Match.home_player_X6 ,
Source.Match.home_player_X7 ,
Source.Match.home_player_X8 ,
Source.Match.home_player_X9 ,
Source.Match.home_player_X10 ,
Source.Match.home_player_X11 ,
Source.Match.away_player_X1 ,
Source.Match.away_player_X2 ,
Source.Match.away_player_X3 ,
Source.Match.away_player_X4 ,
Source.Match.away_player_X5 ,
Source.Match.away_player_X6 ,
Source.Match.away_player_X7 ,
Source.Match.away_player_X8 ,
Source.Match.away_player_X9 ,
Source.Match.away_player_X10 ,
Source.Match.away_player_X11 ,
Source.Match.home_player_Y1 ,
Source.Match.home_player_Y2 ,
Source.Match.home_player_Y3 ,
Source.Match.home_player_Y4 ,
Source.Match.home_player_Y5 ,
Source.Match.home_player_Y6 ,
Source.Match.home_player_Y7 ,
Source.Match.home_player_Y8 ,
Source.Match.home_player_Y9 ,
Source.Match.home_player_Y10 ,
Source.Match.home_player_Y11 ,
Source.Match.away_player_Y1 ,
Source.Match.away_player_Y2 ,
Source.Match.away_player_Y3 ,
Source.Match.away_player_Y4 ,
Source.Match.away_player_Y5 ,
Source.Match.away_player_Y6 ,
Source.Match.away_player_Y7 ,
Source.Match.away_player_Y8 ,
Source.Match.away_player_Y9 ,
Source.Match.away_player_Y10 ,
Source.Match.away_player_Y11 ,
Source.Match.home_player_1 ,
Source.Match.home_player_2 ,
Source.Match.home_player_3 ,
Source.Match.home_player_4 ,
Source.Match.home_player_5 ,
Source.Match.home_player_6 ,
Source.Match.home_player_7 ,
Source.Match.home_player_8 ,
Source.Match.home_player_9 ,
Source.Match.home_player_10 ,
Source.Match.home_player_11 ,
Source.Match.away_player_1 ,
Source.Match.away_player_2 ,
Source.Match.away_player_3 ,
Source.Match.away_player_4 ,
Source.Match.away_player_5 ,
Source.Match.away_player_6 ,
Source.Match.away_player_7 ,
Source.Match.away_player_8 ,
Source.Match.away_player_9 ,
Source.Match.away_player_10 ,
Source.Match.away_player_11 
FROM Source.Match
LEFT JOIN Source.Team AS Team_1 ON Team_1.team_api_id = Source.Match.home_team_api_id
LEFT JOIN Source.Team AS Team_2 ON Team_2.team_api_id = Source.Match.away_team_api_id 
WHERE home_player_X1 IS NOT NULL AND goal IS NOT NULL LIMIT 500;""", db)

Sat_Link_Match_Player_Soccer = DataPrep.prep_match_players_RV (df_Match_for_RV)

Sat_Link_Match_Player_Soccer.dropna(axis = 0, inplace=True)

Sat_Link_Match_Player_Soccer["player_api_id"] = Sat_Link_Match_Player_Soccer["player_api_id"].astype(int)

temp_player_ids = pd.read_sql_query("""SELECT id, player_api_id FROM Source.Player""", db)

Sat_Link_Match_Player_Soccer['bk_player_id'] = (Sat_Link_Match_Player_Soccer.merge(temp_player_ids, left_on='player_api_id', 
right_on='player_api_id', how='left')['id'])

Sat_Link_Match_Player_Soccer.dropna(axis = 0, inplace=True)

Sat_Link_Match_Player_Soccer.drop(["player_api_id"],axis = 1, inplace = True)

Sat_Link_Match_Player_Soccer["bk_player_id"] = Sat_Link_Match_Player_Soccer["bk_player_id"].astype(int)

temp_Sat_Link_Match_Player_Soccer = Sat_Link_Match_Player_Soccer["home_hk"].astype(str)+Sat_Link_Match_Player_Soccer["away_hk"].astype(str)+\
                        Sat_Link_Match_Player_Soccer["league_id"].astype(str)+Sat_Link_Match_Player_Soccer["date"].astype(str)+Sat_Link_Match_Player_Soccer["bk_player_id"].astype(str)
Sat_Link_Match_Player_Soccer.drop(["home_hk", "away_hk", "league_id", "date","bk_player_id"],axis = 1, inplace = True)
Sat_Link_Match_Player_Soccer = pd.concat([temp_Sat_Link_Match_Player_Soccer,Sat_Link_Match_Player_Soccer], axis = 1).reset_index(drop = True)

Sat_Link_Match_Player_Soccer = Sat_Link_Match_Player_Soccer.drop_duplicates()

fill_tables(db_table = "Sat_Link_Match_Player_Soccer", df = Sat_Link_Match_Player_Soccer)


###Close connections
cursor.execute("DETACH DATABASE 'Source';")
db.close()



db = sqlite3.connect(
    "/Users/Jochen/Desktop/Kurse/alfatraining/Data Engineering/Datenbank_Testprojekt_DataVault.db")
cursor = db.cursor()
cursor.execute("SELECT * FROM Hub_Customer")
print(cursor.fetchall())
db.close()