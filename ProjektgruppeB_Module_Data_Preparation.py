"""
This module provides functions that make information from xml cells available and
to restructure data such as player's position.
Created by all group members; contributions equal.
"""

import pandas as pd
from lxml import etree as ET

###For Link_Match_Player
def all_match_players (df): 
    player_list = []
    for m in range(len(df)):
        row = df.iloc[m, :]
        for player in row["home_player_1":"away_player_11"]:
            player_list.append([row["home_hk"],row["away_hk"],row["league_id"],row["date"], player])
    output = pd.DataFrame(player_list, columns=["home_hk", "away_hk", "league_id", "date", "player_api_id"])
    return output

###For player-Business-Vault
def get_goals_cards_per_player(string):
    root = ET.fromstring(string)
    typee = [comment.text for comment in root.xpath('value/comment')]
    player = [player1.text for player1 in root.xpath('value/player1')]
    df = pd.DataFrame(list(zip(typee,player)), columns=["type","player"])
    df = df.groupby(["type",'player']).size().reset_index().rename(columns={0:'count'})
    return df

def get_shots_on_off_crosses_corners_per_player(string):
    root = ET.fromstring(string)
    player = [player1.text for player1 in root.xpath('value/player1')]
    df = pd.DataFrame(player, columns=["player"])
    df = df.groupby('player').size().reset_index().rename(columns={0:'count'})
    return df

### For Team-Business-Vault
def get_first_goal_card_per_team(string):
    root = ET.fromstring(string)
    team = [team.text for team in root.xpath('value/team')]
    minute = [elapsed.text for elapsed in root.xpath('value/elapsed')]
    df = pd.DataFrame(list(zip(team,minute)), columns=["team","minute"])
    df = df.groupby(["team"])["minute"].min().reset_index()
    return df

###Prepare all data from xml per player to insert in business vault
def prep_match_players_BV (df): 
    player_list = []
    for m in range(len(df)):
        row = df.iloc[m, :]
        try:
            row_goal = get_goals_cards_per_player(row.goal)
        except:
            pass
        try:
            row_shoton = get_shots_on_off_crosses_corners_per_player(row.shoton)
        except:
            pass
        try:
            row_shotoff = get_shots_on_off_crosses_corners_per_player(row.shotoff)
        except:
            pass
        try:
            row_card = get_goals_cards_per_player(row.card)
        except:
            pass
        try:
            row_cross = get_shots_on_off_crosses_corners_per_player(row.cross)
        except:
            pass
        try:
            row_corner = get_shots_on_off_crosses_corners_per_player(row.corner)
        except:
            pass
        for player in row["home_player_1":"away_player_11"]:
            goals, owngoals, shotons, shotoffs, crosses, corners, y_card, r_card = [0,0,0,0,0,0,0,0]
            try:
                if ((str(player) == row_goal['player']) & ("n" == row_goal['type'])).any():
                    goals = row_goal.loc[(str(player) == row_goal['player']) & ("n" == row_goal['type']), 'count'].item()
            except:
                pass
            try:
                if ((str(player) == row_goal['player']) & ("o" == row_goal['type'])).any():
                    owngoals = row_goal.loc[(str(player) == row_goal['player']) & ("o" == row_goal['type']), 'count'].item()
            except:
                pass
            try: 
                if ((str(player) == row_shoton['player'])).any():
                    shotons = row_shoton.loc[(str(player) == row_shoton['player']), 'count'].item()
            except:
                pass
            try:
                if ((str(player) == row_shotoff['player'])).any():
                    shotoffs = row_shotoff.loc[(str(player) == row_shotoff['player']), 'count'].item()
            except:
                pass
            try:
                if ((str(player) == row_card['player']) & ("y" == row_card['type'])).any():
                    y_card = row_card.loc[(str(player) == row_card['player']) & ("y" == row_card['type']), 'count'].item()
            except:
                pass
            try:
                if ((str(player) == row_card['player']) & ("r" == row_card['type'])).any():
                    r_card = row_card.loc[(str(player) == row_card['player']) & ("r" == row_card['type']), 'count'].item()
            except:
                pass
            try:
                if ((str(player) == row_cross['player'])).any():
                    crosses = row_cross.loc[(str(player) == row_cross['player']), 'count'].item()
            except:
                pass
            try:
                if ((str(player) == row_corner['player'])).any():
                    corners = row_corner.loc[(str(player) == row_corner['player']), 'count'].item()
            except:
                pass
            player_list.append([row["home_hk"],row["away_hk"],row["league_id"],row["date"], player, goals, owngoals, shotons, shotoffs, y_card, r_card, crosses,
                           corners])
    output = pd.DataFrame(player_list, columns=["home_hk", "away_hk", "league_id", "date", "player_api_id","goals", "y_card", "r_card", "owngoals", 
                                                    "shotons", "shotoffs", 
                                                     "crosses", "corners"])
    return output

###prepare data related to player's position etc. to place in raw data vault
def prep_match_players_RV (df): 
    player_list = []
    for m in range(len(df)):
        row = df.iloc[m, :]
        i = 6
        j = 28
        for player in row["home_player_1":"away_player_11"]:
            try:
                position_x = row.iloc[i]
                position_y = row.iloc[j]
            except:
                pass
            if i in range(6,17):
                home_away = "home"
            else:
                home_away = "away"
            i += 1
            j += 1
            player_list.append([row["home_hk"],row["away_hk"],row["league_id"],row["date"], player, home_away, position_x, position_y])
    output = pd.DataFrame(player_list, columns=["home_hk", "away_hk", "league_id", "date", "player_api_id", "home_away",
                                                    "position_x", "position_y"])
    return output

###prepare data (first goal per team) to put in business data vault
def prep_match_first_goals (df):
    match_list = []
    for m in range(len(df)):
        row = df.iloc[m, :]
        try:
            row_goal = get_first_goal_card_per_team(row.goal)
        except:
            pass
        home_min, away_min = [99999, 99999] 
        try:
            home_min = row_goal.loc[(str(row["home_team_api_id"]) == row_goal["team"]), "minute"].item()
        except:
            pass
        try:
            away_min = row_goal.loc[(str(row["away_team_api_id"]) == row_goal["team"]), "minute"].item()
        except:
            pass
        match_list.append([row["home_team_api_id"], home_min, row["away_team_api_id"],
                               away_min])
    output = pd.DataFrame(match_list, columns=["home_team", "home_min",
                                                "away_team", "away_min"])
    return output