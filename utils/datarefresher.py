# from cbastats import DBHelper
import os
import sys
import pandas as pd
from pathlib import Path
from basketball_reference_scraper.seasons import get_schedule, get_standings
from basketball_reference_scraper.box_scores import get_box_scores
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

# def set_logging_config(verbose):
#     format_s = '%(asctime)s | %(filename)s | %(funcName)s | %(levelname)s | %(message)s'
#     #TODO: i might want to log function name, file name, log time
#     if verbose:
#         logging.basicConfig(level=logging.DEBUG,format=format_s,handlers=[
#         logging.FileHandler("mylog.log"),
#         logging.StreamHandler(sys.stdout)
#         ])
#         print('Verbose mode activated...')
#     else:
#         logging.basicConfig(level=logging.INFO,format=format_s,handlers=[
#         logging.FileHandler("mylog.log"),
#         logging.StreamHandler(sys.stdout)
#         ])
        # logging.basicConfig(level=logging.INFO,format=f'{asctime}')

def set_logging_config(verbose):
    # get logger
    logger = logging.getLogger('main_logger')

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



def refresh():
    # logging.basicConfig(level=logging.INFO)
    #####################################################################################
    # parameters

    # take in raw arguments
    parser = ArgumentParser(description="Refresh NBA data in DB or save full data to local.")
    parser.add_argument('-v','--verbose',action='store_true', help="indicate the verbosity level")
    parser.add_argument('-wd','--working_dir', default='.', help='path of relative working directory')
    parser.add_argument('-dd','--data_dir', default='data', help='path of data directory')
    parser.add_argument('-ef','--team_enum_fpath', default='data/team_enums.txt', help='path of team enums file')
    parser.add_argument('-sf','--save_to_file', action="store_true", help='indicate whether save data to DB')
    parser.add_argument('-fr','--full_refresh', action="store_true", help="indicate whether perform a full refresh")
    raw_args = parser.parse_args()

    # set logging level
    logger = set_logging_config(raw_args.verbose)
    logger.info(F"Running '{__file__}'")

    # process raw arguments
    logger.info('Parsing arguments') 

    args={}
    args['WORKING_DIR'] = Path(raw_args.working_dir)
    args['LOCAL_DATA_FOLDER']=Path(raw_args.data_dir)
    args['TEAM_ENUM_FILEPATH'] = Path(raw_args.team_enum_fpath)
    args['SAVE_TO_FILE'] = raw_args.save_to_file
    args['FULL_REFRESH'] = raw_args.full_refresh


    # load these environment variables for DB access
    logger.info('Checking DB access environment variables')
    needed_envs = ['MONGODB_PWD','MONGODB_USERNAME','MONGODB_ENDPOINT']
    envs = os.environ
    # only checks if user wants to save data to DB
    # check if all needed environment variables are present
    if not args['SAVE_TO_FILE']:
        logger.info("Data will be saved to DB")
        for needed_env in needed_envs:
            if needed_env not in envs:
                raise Exception(f"Missing environment variable: {needed_env}.\n \
            Please check if these environment variables are present: {needed_envs}")
            args[needed_env]=envs[needed_env]





    #####################################################################################
    

    # set up local data folder if data not save to DB
    if args['SAVE_TO_FILE']:
        local_data_folder = Path(args['LOCAL_DATA_FOLDER'])
        if not local_data_folder.exists():
            os.mkdir(local_data_folder)

    args['TEAM_ENUM'] = load_team_enum(args['TEAM_ENUM_FILEPATH'])
    logger.debug(args)

if __name__ == '__main__':
    refresh()