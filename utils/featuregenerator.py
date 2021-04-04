"""
This module will populate features for the data
"""

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

#TODO: all these setup code are repetitive, put them into one single file like main.py, then run from there
# In[2]:
config={}

current_season_year = int(get_current_season())
config['CURRENT_SEASON'] = f"{current_season_year-1}-{current_season_year}"

DOTENV_PATH="."
env_path = Path(DOTENV_PATH) / '.env'
if not (env_path.exists()):
    print('.env file is missing.')
    sys.exit()
load_dotenv(dotenv_path=env_path,verbose=True)

needed_envs = ['MONGODB_PWD', 'MONGODB_USERNAME', 'MONGODB_ENDPOINT','LOGGER_NAME']
envs = os.environ
# check if all needed environment variables are present
for needed_env in needed_envs:
    if needed_env not in envs:
        raise Exception(f"Missing environment variable: {needed_env}.\n     Please check if these environment variables are present: {needed_envs}")
    config[needed_env] = envs[needed_env]
logger = set_logging_config(config['LOGGER_NAME'],False)

# %%

