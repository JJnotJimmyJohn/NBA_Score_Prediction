# coding: utf-8

# In[9]:


import logging
from tqdm import tqdm
import sys
sys.path.append('.')
from pathlib import Path
import pandas as pd
from cbastats.DBHelper import MongoDBHelper
import os
from dotenv import load_dotenv
from utils.datarefresher import get_current_season, set_logging_config


# In[3]:


config={}


# In[4]:


current_season_year = int(get_current_season())
config['CURRENT_SEASON'] = f"{current_season_year-1}-{current_season_year}"


# In[29]:


config['COLUMN_NAMES'] = ['season', 'DATE', 'boxscores_url', 'game_id', 'HOME', 'VISITOR',
       'HOME_PTS', 'VISITOR_PTS', 'Pace_home', 'eFG%_home', 'TOV%_home',
       'TS%_home', '3PAr_home', 'FTr_home', 'DRB%_home', 'TRB%_home',
       'AST%_home', 'STL%_home', 'BLK%_home', 'DRtg_home', 'ORB%_home',
       'FT/FGA_home', 'ORtg_home', 'Pace_visitor', 'eFG%_visitor',
       'TOV%_visitor', 'ORB%_visitor', 'FT/FGA_visitor', 'ORtg_visitor',
       'TS%_visitor', '3PAr_visitor', 'FTr_visitor', 'DRB%_visitor',
       'TRB%_visitor', 'AST%_visitor', 'STL%_visitor', 'BLK%_visitor',
       'DRtg_visitor', 'season_nth_game', 'hometeam_nth_homegame',
       'visitorteam_nth_visitorgame', 'TOTAL_PTS', 'HOME_VISITOR_PTS_DIFF',
       'HOME_WIN']
config['NUMERIC_COLS'] = ['VISITOR_PTS','HOME_PTS','Pace_home', 'eFG%_home', 'TOV%_home',
       'ORB%_home', 'FT/FGA_home', 'ORtg_home', 'Pace_visitor', 'eFG%_visitor',
       'TOV%_visitor', 'ORB%_visitor', 'FT/FGA_visitor', 'ORtg_visitor',
       'TS%_home', '3PAr_home', 'FTr_home', 'DRB%_home', 'TRB%_home',
       'AST%_home', 'STL%_home', 'BLK%_home', 'DRtg_home', 'TS%_visitor',
       '3PAr_visitor', 'FTr_visitor', 'DRB%_visitor', 'TRB%_visitor',
       'AST%_visitor', 'STL%_visitor', 'BLK%_visitor', 'DRtg_visitor']
config['DUP_COL'] = ['PLAYER','MP','eFG%','TOV%','USG%','ORB%','ORtg','BPM','Unnamed: 16_level_1']


# In[6]:


DOTENV_PATH="."
env_path = Path(DOTENV_PATH) / '.env'
if not (env_path.exists()):
    print('.env file is missing.')
    sys.exit()
load_dotenv(dotenv_path=env_path,verbose=True)


# In[7]:


needed_envs = ['MONGODB_PWD', 'MONGODB_USERNAME', 'MONGODB_ENDPOINT','LOGGER_NAME']
envs = os.environ
# only checks if user wants to save data to DB
# check if all needed environment variables are present

for needed_env in needed_envs:
    if needed_env not in envs:
        raise Exception(f"Missing environment variable: {needed_env}.\n     Please check if these environment variables are present: {needed_envs}")
    config[needed_env] = envs[needed_env]


# In[8]:


mongodbio = MongoDBHelper()
client = mongodbio.create_connection(
    config['MONGODB_USERNAME'], config['MONGODB_PWD'], config['MONGODB_ENDPOINT'])
nba_db = client['nbaStats']
# coll_nbaGames = nba_db['nbaGames']
# coll_nbaGamesStaging= nba_db['nbaGamesStaging']
coll_nbaBoxScores = nba_db['nbaProcessedBoxScores']


# In[62]:


logger = set_logging_config(config['LOGGER_NAME'],False)


# # Manipulate Data

# In[66]:


def gen_team_feat_PastNGames_Avg(N_GAMES=10):
    logger.info(f"Generating features")
    logger.info(f"Pulling box scores from MongoDB")
    boxscores = mongodbio.select_records(coll_nbaBoxScores,filter={},field={'_id':0})
    boxscores = pd.DataFrame(boxscores)
    logger.info(f"Pulled box scores: {len(boxscores)} games across {boxscores['season'].nunique()} seasons")

    logger.info(f"Summarize home team data")
    # columns need to be summarized for home teams
    col_summ_home = [col for col in list(boxscores.columns) if col.endswith('_home')]
    hteam_games=boxscores[['DATE','HOME','game_id','season']]
    # shift one row down, so that calculating past stats won't cause data leakage
    hteams_boxscores=boxscores.groupby(['season','HOME']).shift(1).dropna(how='any')[col_summ_home]
    # shift games downward by 1 row, so when calculating rolling 10 games, game to predict is not included
    shifted=pd.merge(hteam_games,hteams_boxscores,left_index=True,right_index=True)
    home_summarized = shifted.rename_axis(index='game_index').sort_values(by='DATE').groupby(['season','HOME']).rolling(N_GAMES).mean().dropna(how='any')[col_summ_home].droplevel(level=[0,1])

    logger.info(f"Summarize visitor team data")
    # columns need to be summarized for visitor teams
    col_summ_visitor = [col for col in list(boxscores.columns) if col.endswith('_visitor')]
    vteam_games=boxscores[['DATE','VISITOR','game_id','season']]
    # shift one row down, so that calculating past stats won't cause data leakage
    vteams_boxscores=boxscores.groupby(['season','VISITOR']).shift(1).dropna(how='any')[col_summ_visitor]
    # shift games downward by 1 row, so when calculating rolling 10 games, game to predict is not included
    shifted=pd.merge(vteam_games,vteams_boxscores,left_index=True,right_index=True)
    visitor_summarized = shifted.rename_axis(index='game_index').sort_values(by='DATE').groupby(['season','VISITOR']).rolling(N_GAMES).mean().dropna(how='any')[col_summ_visitor].droplevel(level=[0,1])

    logger.info(f"Join home team, visitor team data")
    processed_dataset=pd.concat([boxscores.rename_axis('game_index')[['DATE','VISITOR', 'VISITOR_PTS', 'HOME', 'HOME_PTS', 'boxscores_url','game_id']],
              home_summarized,
              visitor_summarized],join='inner',axis=1)
    processed_dataset['TOTAL_PTS'] = processed_dataset['VISITOR_PTS']+processed_dataset['HOME_PTS']
    processed_dataset['HOME_VISITOR_PTS_DIFF'] = processed_dataset['HOME_PTS']-processed_dataset['VISITOR_PTS']
    processed_dataset['HOME_WIN'] = (processed_dataset['HOME_VISITOR_PTS_DIFF']>0)*1
    
    logger.info(f"Insert data into MongoDB")
    coll_feat = nba_db['nbaTeamFeat_PastNGames_Avg']
    coll_feat.delete_many(filter={})
    coll_feat.insert_many(processed_dataset.to_dict('records'))
    logger.info(f"Data insertion complete")

# In[67]:

if __name__=="__main__":
    gen_team_feat_PastNGames_Avg(N_GAMES=10)



# In[ ]:





# In[ ]:





# In[ ]:




