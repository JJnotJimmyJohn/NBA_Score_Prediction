
# In[1]:


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


# In[2]:


config={}


# In[3]:


current_season_year = int(get_current_season())
config['CURRENT_SEASON'] = f"{current_season_year-1}-{current_season_year}"


# In[4]:


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


# In[5]:


DOTENV_PATH="."
env_path = Path(DOTENV_PATH) / '.env'
if not (env_path.exists()):
    print('.env file is missing.')
    sys.exit()
load_dotenv(dotenv_path=env_path,verbose=True)


# In[6]:


needed_envs = ['MONGODB_PWD', 'MONGODB_USERNAME', 'MONGODB_ENDPOINT','LOGGER_NAME']
envs = os.environ
# only checks if user wants to save data to DB
# check if all needed environment variables are present

for needed_env in needed_envs:
    if needed_env not in envs:
        raise Exception(f"Missing environment variable: {needed_env}.\n     Please check if these environment variables are present: {needed_envs}")
    config[needed_env] = envs[needed_env]
    
# In[]:
logger = set_logging_config(config['LOGGER_NAME'],False)


# %%

# In[7]:

logger.info("Connecting to MongoDB")
mongodbio = MongoDBHelper()
client = mongodbio.create_connection(
    config['MONGODB_USERNAME'], config['MONGODB_PWD'], config['MONGODB_ENDPOINT'])
nba_db = client['nbaStats']
coll_nbaGames = nba_db['nbaGames']
coll_nbaGamesStaging= nba_db['nbaGamesStaging']


# ## Assemble Raw Data

# In[8]:


logger.info("Selecting schedules")
# schedule
schedule = mongodbio.select_records(coll_nbaGames,field={
    'four_factors':0, 'basic_boxscores':0,'advanced_boxscores':0,'_id':0
})
schedule=pd.DataFrame(schedule)
# print(schedule.shape)
schedule.head()


# In[9]:


logger.info("Selecting four factors")
# four factors
fourfactors=mongodbio.select_records(coll_nbaGames,field={'four_factors':1,'_id':0
})

fourfactors_dfs=[]
for ff in fourfactors:
    _ = pd.DataFrame(ff['four_factors'])
    fourfactors_dfs.append(_)
fourfactors = pd.concat(fourfactors_dfs)
fourfactors = fourfactors.reset_index(drop=True)
# print(fourfactors.shape)
fourfactors.head()


# In[10]:


logger.info("Selecting box scores")
# box scores
adv_box_scores = mongodbio.select_records(coll_nbaGames,field={
    'game_id':1,'advanced_boxscores':1,'_id':0
})

team_adv_box_scores_dfs = []
bad_games=[]
for adv_box_score in adv_box_scores:
    game_id = adv_box_score['game_id']
    team_adv_box_score = adv_box_score['advanced_boxscores']
    for team, team_adv_box_score in team_adv_box_score.items():
        team_adv_box_score = pd.DataFrame(team_adv_box_score)
        team_adv_box_score['Team']=team
        team_adv_box_score['game_id']=game_id
        team_adv_box_scores_dfs.append(team_adv_box_score.iloc[-1])

adv_box_scores=pd.concat(team_adv_box_scores_dfs,axis=1).transpose().reset_index(drop=True)

# delete duplicated columns and empty, useless columns
rm_cols = config['DUP_COL']
adv_box_scores = adv_box_scores.drop(columns=rm_cols)
# print(adv_box_scores.shape)
adv_box_scores.head()


# ## Join Data Together

# In[11]:


logger.info("Join data together")
# join with four factors
_ = pd.merge(schedule,fourfactors,left_on=['game_id','HOME'],right_on=['game_id','Team'])
_ = pd.merge(_,fourfactors,left_on=['game_id','VISITOR'],right_on=['game_id','Team'],suffixes=('_home','_visitor'))
# join with box scores
_ = pd.merge(_,adv_box_scores,left_on=['game_id','Team_home'],right_on=['game_id','Team'])
complete_data = pd.merge(_,adv_box_scores,left_on=['game_id','Team_visitor'],right_on=['game_id','Team'],suffixes=('_home','_visitor'))
complete_data = complete_data.drop(columns=['Team_home','Team_visitor'])

# In[14]:


complete_data[config['NUMERIC_COLS']] = complete_data[config['NUMERIC_COLS']].astype('float')


# # Add Aux Columns

# In[15]:


complete_data['season_nth_game'] = complete_data.groupby(['season']).cumcount()+1
complete_data['hometeam_nth_homegame'] = complete_data.groupby(['season','HOME']).cumcount()+1
complete_data['visitorteam_nth_visitorgame'] = complete_data.groupby(['season','VISITOR']).cumcount()+1

complete_data['TOTAL_PTS'] = complete_data['VISITOR_PTS']+complete_data['HOME_PTS']
complete_data['HOME_VISITOR_PTS_DIFF'] = complete_data['HOME_PTS']-complete_data['VISITOR_PTS']
complete_data['HOME_WIN'] = (complete_data['HOME_VISITOR_PTS_DIFF']>0)*1


# In[16]:


cols_diff=set(complete_data.columns).symmetric_difference(set(config['COLUMN_NAMES']))
assert len(cols_diff)==0, f"Difference in columns: {cols_diff}"


# In[17]:


# rearrange column orders
complete_data = complete_data[config['COLUMN_NAMES']]


# In[20]:

logger.info("Inserting records to MongoDB")
coll_nbaGameBoxScores = nba_db['nbaProcessedBoxScores']

coll_nbaGameBoxScores.delete_many(filter={})

coll_nbaGameBoxScores.insert_many(complete_data.to_dict('records'))

logger.info("Script complete")
