"""
This script contains all code to build the database tables
Created by all group members; contributions equal.
"""

import sqlite3

db = sqlite3.connect(
    "/Users/Jochen/Desktop/Kurse/alfatraining/Data Engineering/Projekt_DataVault_GruppeB.db")
cursor = db.cursor()

sqlquery = """
DROP TABLE IF EXISTS Sat_League;
DROP TABLE IF EXISTS Sat_Team;
DROP TABLE IF EXISTS Sat_Team_Soccer;
DROP TABLE IF EXISTS Sat_Link_Match;
DROP TABLE IF EXISTS Sat_Link_Match_Soccer;
DROP TABLE IF EXISTS Sat_Link_Match_Player_Soccer_BV;
DROP TABLE IF EXISTS Sat_Link_Match_Player_Soccer;
DROP TABLE IF EXISTS Sat_Player;
DROP TABLE IF EXISTS Sat_Player_Soccer;
DROP TABLE IF EXISTS Sat_Player_Soccer_BV;
DROP TABLE IF EXISTS Link_Match;
DROP TABLE IF EXISTS Link_Match_Player;
DROP TABLE IF EXISTS Hub_Team;
DROP TABLE IF EXISTS Hub_Player;
DROP TABLE IF EXISTS Hub_League;
DROP TABLE IF EXISTS Record_Source;

CREATE TABLE Record_Source(
source_id INTEGER PRIMARY KEY AUTOINCREMENT ,  
source_name varchar(30),
comment varchar(100) DEFAULT NULL
);

CREATE TABLE Hub_Team(
hk VARCHAR(32) PRIMARY KEY,
bk VARCHAR(30),
ldts DateTime DEFAULT CURRENT_TIMESTAMP,
rs INTEGER DEFAULT 1,
Foreign Key(rs) references Record_Source(source_id)
);

CREATE TABLE Hub_League(
hk VARCHAR(32) PRIMARY KEY,
bk INTEGER,
ldts DateTime DEFAULT CURRENT_TIMESTAMP,
rs INTEGER DEFAULT 1,
Foreign Key(rs) references Record_Source(source_id)
);

CREATE TABLE Hub_Player(
hk VARCHAR(32) PRIMARY KEY,
bk VARCHAR(30),
ldts DateTime DEFAULT CURRENT_TIMESTAMP,
rs INTEGER DEFAULT 1,
Foreign Key(rs) references Record_Source(source_id)
);

CREATE TABLE Link_Match (
hk VARCHAR(32) PRIMARY KEY, 
home_team_id VARCHAR(32),
away_team_id VARCHAR(32),
league_id VARCHAR(32),
date_of_match DATE,
ldts DATETIME DEFAULT CURRENT_TIMESTAMP,
rs INTEGER DEFAULT 1,
Foreign Key(home_team_id) references Hub_Team(hk),
Foreign Key(away_team_id) references Hub_Team(hk),
Foreign Key(league_id) references Hub_League(hk)
);

CREATE TABLE Link_Match_Player (
hk VARCHAR(32) PRIMARY KEY,
match_id VARCHAR(32),
player_id VARCHAR(32),
ldts DATETIME DEFAULT CURRENT_TIMESTAMP,
rs INTEGER DEFAULT 1,
Foreign Key(match_id) references Link_Match(hk),
Foreign Key(player_id) references Hub_Player(hk)
);

CREATE TABLE Sat_League(
hk VARCHAR(32) PRIMARY KEY,
league_id VARCHAR(32),
ldts DATETIME DEFAULT CURRENT_TIMESTAMP,
edts DATETIME DEFAULT '9999-12-31',
rs INTEGER DEFAULT 1,
hd VARCHAR(32),
league_name VARCHAR(40),
country_name VARCHAR(60),
Foreign Key(league_id) references Hub_League(hk),
Foreign Key(rs) references Record_Source(source_id)
);

CREATE TABLE Sat_Team(
hk VARCHAR(32) PRIMARY KEY,
team_id VARCHAR(32),
ldts DATETIME DEFAULT CURRENT_TIMESTAMP,
edts DATETIME DEFAULT '9999-12-31',
rs INTEGER DEFAULT 1,
hd VARCHAR(32),
long_name VARCHAR(35),
short_name VARCHAR(5),
sport_type VARCHAR(35) DEFAULT "soccer",
gender VARCHAR(1) DEFAULT "m",
club_team INTEGER DEFAULT 1,
Foreign Key(team_id) references Hub_Team(hk),
Foreign Key(rs) references Record_Source(source_id)
);

CREATE TABLE Sat_Team_Soccer(
hk VARCHAR(32) PRIMARY KEY,
team_id VARCHAR(32),
ldts DATETIME DEFAULT CURRENT_TIMESTAMP,
edts DATETIME DEFAULT '9999-12-31',
rs INTEGER DEFAULT 1,
hd VARCHAR(32),
team_fifa_api_id INTEGER,
team_api_id INTEGER,
date_of_record DATE,
buildUpPlaySpeed INTEGER,
buildUpPlaySpeedClass VARCHAR(25),
buildUpPlayDribbling REAL,
buildUpPlayDribblingClass VARCHAR(25),
buildUpPlayPassing INTEGER,
buildUpPlayPassingClass VARCHAR(25),
buildUpPlayPositioningClass VARCHAR(25),
chanceCreationPassing INTEGER,
chanceCreationPassingClass VARCHAR(25),
chanceCreationCrossing INTEGER,
chanceCreationCrossingClass VARCHAR(25),
chanceCreationShooting INTEGER,
chanceCreationShootingClass INTEGER,
chanceCreationPositioningClass INTEGER,
defencePressure INTEGER,
defencePressureClass VARCHAR(25),
defenceAggression INTEGER,
defenceAggressionClass VARCHAR(25),
defenceTeamWidth INTEGER,
defenceTeamWidthClass VARCHAR(25),
defenceDefenderLineClass VARCHAR(25),
Foreign Key(team_id) references Hub_Team(hk),
Foreign Key(rs) references Record_Source(source_id)
);

CREATE TABLE Sat_Link_Match_Soccer(
hk VARCHAR(32) PRIMARY KEY,
match_id VARCHAR(32),
ldts DATETIME DEFAULT CURRENT_TIMESTAMP,
edts DATETIME DEFAULT '9999-12-31',
rs INTEGER DEFAULT 1,
hd VARCHAR(32),
bk_match_id INTEGER,
match_api_id INTEGER,
goal VARCHAR(2000),
shot_on VARCHAR(2000),
shot_off VARCHAR(2000),
foul_committed VARCHAR(2000),
card VARCHAR(2000),
cross_pass VARCHAR(2000),
corner VARCHAR(2000),
possession VARCHAR(2000),
Foreign Key(match_id) references Link_Match(hk),
Foreign Key(rs) references Record_Source(source_id)
);

CREATE TABLE Sat_Link_Match(
hk VARCHAR(32) PRIMARY KEY, 
match_id VARCHAR(32),
ldts DATETIME DEFAULT CURRENT_TIMESTAMP,
edts DATETIME DEFAULT '9999-12-31',
rs INTEGER DEFAULT 1,
hd VARCHAR(32),
bk_match_id INTEGER,
country_name VARCHAR(60),
season VARCHAR(9),
stage INTEGER,
home_team_points INTEGER,
away_team_points INTEGER,
B365H REAL,
B365D REAL,
B365A REAL,
BWH REAL,
BWD REAL,
BWA REAL,
IWH REAL,
IWD REAL,
IWA REAL,
LBH REAL,
LBD REAL,
LBA REAL,
PSH REAL,
PSD REAL,
PSA REAL,
WHH REAL,
WHD REAL,
WHA REAL,
SJH REAL,
SJD REAL,
SJA REAL,
VCH REAL,
VCD REAL,
VCA REAL,
GBH REAL,
GBD REAL,
GBA REAL,
BSH REAL,
BSD REAL,
BSA REAL,
Foreign Key(match_id) references Link_Match(hk),
Foreign Key(rs) references Record_Source(source_id)
);

CREATE TABLE Sat_Link_Match_Player_Soccer_BV(
hk VARCHAR(32) PRIMARY KEY,  
match_player_id VARCHAR(32),
ldts DATETIME DEFAULT CURRENT_TIMESTAMP,
edts DATETIME DEFAULT '9999-12-31',
rs INTEGER DEFAULT 1,
hd VARCHAR(32),
goals INTEGER,
yellow_cards INTEGER, 
red_cards INTEGER ,
shots_on INTEGER,
shots_off INTEGER,
cross_passes INTEGER,
corners INTEGER,
Foreign Key(match_player_id) references Link_Match_Player(hk),
Foreign Key(rs) references Record_Source(source_id)
);

CREATE TABLE Sat_Link_Match_Player_Soccer (
hk VARCHAR(32) PRIMARY KEY, 
match_player_id VARCHAR(32),
ldts DATETIME DEFAULT CURRENT_TIMESTAMP,
edts DATETIME DEFAULT '9999-12-31',
rs INTEGER DEFAULT 1,
hd VARCHAR(32),
home_away VARCHAR(1),
position_x REAL,
position_y REAL,
Foreign Key(match_player_id) references Link_Match_Player(hk),
Foreign Key(rs) references Record_Source(source_id)
);

CREATE TABLE Sat_Player(
hk VARCHAR(32) PRIMARY KEY,
player_id VARCHAR(32),
ldts DATETIME DEFAULT CURRENT_TIMESTAMP,
edts DATETIME DEFAULT '9999-12-31',
rs INTEGER DEFAULT 1,
hd VARCHAR(32),
name VARCHAR(70),
birthday DATE,
height INTEGER,
weight INTEGER,
Foreign Key(player_id) references Hub_Player(hk),
Foreign Key(rs) references Record_Source(source_id)
);

CREATE TABLE Sat_Player_Soccer(
hk VARCHAR(32) PRIMARY KEY,
player_id VARCHAR(32),
ldts DATETIME DEFAULT CURRENT_TIMESTAMP,
edts DATETIME DEFAULT '9999-12-31',
rs INTEGER DEFAULT 1,
hd VARCHAR(32),
player_api_id INTEGER,
player_fifa_api_id INTEGER,
date_of_record DATE,
overall_rating INTEGER,
potential INTEGER,
preferred_foot VARCHAR(5),
attacking_work_rate VARCHAR(6),
defensive_work_rate VARCHAR(6),
crossing INTEGER,
finishing INTEGER,
heading_accuracy INTEGER,
short_passing INTEGER,
volleys INTEGER,
dribbling INTEGER,
curve INTEGER,
free_kick_accuracy INTEGER,
long_passing INTEGER,
ball_control INTEGER,
acceleration INTEGER,
sprint_speed INTEGER,
agility INTEGER,
reactions INTEGER,
balance INTEGER,
shot_power INTEGER,
stamina INTEGER,
strength INTEGER,
long_shots INTEGER,
aggression INTEGER,
interceptions INTEGER,
vision INTEGER,
penalties INTEGER,
marking INTEGER,
standing_tackle INTEGER, 
sliding_tackle INTEGER,
gk_diving INTEGER,
gk_handling INTEGER,
gk_kicking INTEGER,
gk_positioning INTEGER,
gk_reflexes INTEGER,
Foreign Key(player_id) references Hub_Player(hk),
Foreign Key(rs) references Record_Source(source_id)
);

CREATE TABLE Sat_Player_Soccer_BV(
hk VARCHAR(32) PRIMARY KEY,
player_id VARCHAR(32),
ldts DATETIME DEFAULT CURRENT_TIMESTAMP,
edts DATETIME DEFAULT '9999-12-31',
rs INTEGER DEFAULT 1,
hd VARCHAR(32),
season VARCHAR(9),
goals_season INTEGER,
yellow_cards_season INTEGER, 
red_cards_season INTEGER ,
shots_on_season INTEGER,
shots_off_season INTEGER,
cross_passes_season INTEGER,
corners_season INTEGER,
Foreign Key(player_id) references Hub_Player(hk),
Foreign Key(rs) references Record_Source(source_id)
);

"""

cursor.executescript(sqlquery)
db.commit()

db.close()