# from cbastats import DBHelper
import os
import sys
from requests.api import head
# make sure the packages can be found
sys.path.append('.')
# import other packages
import pandas as pd
from pathlib import Path
from basketball_reference_scraper.seasons import get_schedule, get_standings
from basketball_reference_scraper.box_scores import get_box_scores
from basketball_reference_scraper.constants import TEAM_TO_TEAM_ABBR
from cbastats.Scraper import Scraper
import datetime
import requests
from bs4 import BeautifulSoup
from tqdm import tqdm
from argparse import ArgumentParser
import logging
# import click

def load_team_enum(file_path):
    try:
        with open(file_path,'r') as f:
            teams = f.readlines()
    except FileNotFoundError as err:
        raise err
    
    team_dict={}
    for team in teams:
        splits = team.split(":")
        team_dict[splits[0].strip().title()] = splits[1].strip()

    return team_dict


def set_logging_config(verbose):
    # get logger
    logger = logging.getLogger('data_refresher')

    # create formatter
    formatter = logging.Formatter('%(asctime)s | %(filename)s | %(funcName)s | %(levelname)s | %(message)s')

    # create console handler
    ch = logging.StreamHandler(sys.stdout)
    # create file handler
    fh = logging.FileHandler("mylog.log",mode='a',encoding='UTF-8')
    # set format
    handlers = [ch, fh]
    for handler in handlers:
        handler.setFormatter(formatter)
        logger.addHandler(handler)
    
    if verbose:
        logger.setLevel(logging.DEBUG)
    else:
        logger.setLevel(logging.INFO)

    return logger

def get_current_season():
    ENCODING = 'UTF-8'
    PARSER = 'html.parser'
    HEADERS = {
    'User-Agent': r'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_10_1) AppleWebKit/537.36 (KHTML, like Gecko) '
    r'Chrome/41.0.2227.1 Safari/537.36'}
    logger = logging.getLogger('data_refresher')
    logger.info('Getting current season')
    page_content=Scraper.get_page_content("https://www.basketball-reference.com/leagues/",
    encoding=ENCODING,parser=PARSER,headers=HEADERS)
    current_season = page_content.find_all('th',attrs={'scope':"row","data-stat":"season"})[0].text
    current_season = int(current_season[:4])+1
    logger.info(f'Current season is {current_season-1}-{current_season}')
    # get_schedule takes later year of the season
    return current_season


def scrape_schedule(*args):
    logger = logging.getLogger('data_refresher')
    logger.info('Getting current season')
    all_schedules=[]
    if len(args)==1:
        year = args[0]
        logger.info(f'1 season to scrape')
        logger.info(f'Scraping {year-1}-{year} season')
        year_schedule = get_schedule(year, playoffs=False)
        year_schedule['season'] = f"{year-1}-{year}"
        all_schedules.append(year_schedule)
    else:
        # range does not include the last argument, therefore +1
        logger.info(f'{args[1]-args[0]+1} season(s) to scrape')
        for year in range(args[0],args[1]+1):
            logger.info(f'Scraping {year-1}-{year} season')
            year_schedule = get_schedule(year, playoffs=False)
            year_schedule['season']= f"{year-1}-{year}"
            all_schedules.append(year_schedule)
            
    all_schedules=pd.concat(all_schedules)
    logger.info('Schedules scraped!')
    return all_schedules

def clean_schedule(all_schedules):
    logger = logging.getLogger('data_refresher')
    logger.info('Cleaning scraped schedule')
    all_schedules.dropna(how='any',inplace=True)
    all_schedules['VISITOR']=all_schedules['VISITOR'].apply(lambda x: x.replace('*','').upper())
    all_schedules['HOME']=all_schedules['HOME'].apply(lambda x: x.replace('*','').upper())
    # logger.debug('unique visitor teams:'+str(all_schedules['VISITOR'].unique()))
    # logger.debug('unique home teams:'+str(all_schedules['HOME'].unique()))
    all_schedules=all_schedules.replace(TEAM_TO_TEAM_ABBR)
    for visitor in all_schedules['VISITOR'].unique():
        try:
            assert len(visitor)==3, f"{visitor}'s team name needs fix"
        except Exception as err:
            logger.critical(f"{visitor}'s team name needs fix")
            raise err
    for home in all_schedules['HOME'].unique():
        try:
            assert len(visitor)==3, f"{home}'s team name needs fix"
        except Exception as err:
            logger.critical(f"{home}'s team name needs fix")
            raise err
    logger.info("Schedule cleaned!")
    return all_schedules

def refresh():
    #####################################################################################
    # parameters

    # take in raw arguments
    parser = ArgumentParser(description="Refresh NBA data in DB or save full data to local (default: %(default)s)")
    parser.add_argument('-v','--verbose',action='store_true', help="indicate the verbosity level (default: %(default)s)")
    parser.add_argument('-wd','--working_dir', default='.', help='path of relative working directory (default: %(default)s)')
    parser.add_argument('-dd','--data_dir', default='data', help='path of data directory (default: %(default)s)')
    parser.add_argument('-ef','--team_enum_fpath', default='data/team_enums.txt', help='path of team enums file (default: %(default)s)')
    parser.add_argument('-sf','--save_to_file', action="store_true", help='indicate whether save data to DB (default: %(default)s)')
    parser.add_argument('-fr','--full_refresh', action="store_true", help="indicate whether perform a full refresh (default: %(default)s)")
    parser.add_argument('-ss','--start_season',type=int,default=2014,help="start season to refresh (default: %(default)s)")
    raw_args = parser.parse_args()

    # set logging level
    logger = set_logging_config(raw_args.verbose)
    logger.info(F"Running '{__file__}'")

    # process raw arguments
    logger.info('Parsing arguments') 


    config={}
    config['WORKING_DIR'] = Path(raw_args.working_dir)
    config['LOCAL_DATA_FOLDER']=Path(raw_args.data_dir)
    config['TEAM_ENUM_FILEPATH'] = Path(raw_args.team_enum_fpath)
    config['SAVE_TO_FILE'] = raw_args.save_to_file
    config['FULL_REFRESH'] = raw_args.full_refresh
    config['START_SEASON'] = raw_args.start_season


    # load these environment variables for DB access
    logger.info('Checking DB access environment variables')
    needed_envs = ['MONGODB_PWD','MONGODB_USERNAME','MONGODB_ENDPOINT']
    envs = os.environ
    # only checks if user wants to save data to DB
    # check if all needed environment variables are present
    if not config['SAVE_TO_FILE']:
        logger.info("Data will be saved to DB")
        for needed_env in needed_envs:
            if needed_env not in envs:
                raise Exception(f"Missing environment variable: {needed_env}.\n \
            Please check if these environment variables are present: {needed_envs}")
            config[needed_env]=envs[needed_env]

    # set up local data folder if data not save to DB
    if config['SAVE_TO_FILE']:
        local_data_folder = Path(config['LOCAL_DATA_FOLDER'])
        if not local_data_folder.exists():
            os.mkdir(local_data_folder)

    config['TEAM_ENUM'] = load_team_enum(config['TEAM_ENUM_FILEPATH'])
    logger.debug(config)



    #####################################################################################
    # workflow starts
    

    # get current season year
    # if current season is 2020-21, current season year will be 2021
    current_season = get_current_season()
    # if full refresh, scrape all year until current year
    if config['FULL_REFRESH']:
        all_schedules = scrape_schedule(config['START_SEASON'],current_season)
    # if not, just scrape current year season
    else:
        all_schedules = scrape_schedule(current_season)
    
    # clean schedule
    
    all_schedules = clean_schedule(all_schedules)


    # then update games according to the DB

if __name__ == '__main__':
    refresh()
    # print(get_current_season())