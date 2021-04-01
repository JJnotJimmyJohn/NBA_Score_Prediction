import sys
sys.path.append('.')
# from cbastats import DBHelper
from requests.api import head
import os
from cbastats.DBHelper import MongoDBHelper
import pandas as pd
from pathlib import Path
from basketball_reference_scraper.seasons import get_schedule, get_standings
from basketball_reference_scraper.box_scores import get_box_scores
from basketball_reference_scraper.constants import TEAM_TO_TEAM_ABBR
from cbastats.Scraper import Scraper
import logging
from argparse import ArgumentParser
from tqdm import tqdm
from bs4 import BeautifulSoup
import requests
import datetime
import requests
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry
# make sure the packages can be found
# import other packages
# import click

LOGGER_NAME = 'data_refresher'
# test git

def load_team_enum(file_path):
    """
    description

    Parameters
    ----------


    Examples
    --------

    Returns
    -------


    """
    try:
        with open(file_path, 'r') as f:
            teams = f.readlines()
    except FileNotFoundError as err:
        raise err

    team_dict = {}
    for team in teams:
        splits = team.split(":")
        team_dict[splits[0].strip().title()] = splits[1].strip()

    return team_dict


def set_logging_config(verbose):
    """
    description

    Parameters
    ----------


    Examples
    --------

    Returns
    -------


    """
    # get logger
    logger = logging.getLogger(LOGGER_NAME)

    # create formatter
    formatter = logging.Formatter(
        '%(asctime)s | %(filename)30s | %(funcName)30s | %(levelname)8s | %(message)s')

    # create console handler
    ch = logging.StreamHandler(sys.stdout)
    # create file handler
    fh = logging.FileHandler("mylog.log", mode='a', encoding='UTF-8')
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
    """
    description

    Parameters
    ----------


    Examples
    --------

    Returns
    -------


    """
    ENCODING = 'UTF-8'
    PARSER = 'html.parser'
    HEADERS = {
        'User-Agent': r'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_10_1) AppleWebKit/537.36 (KHTML, like Gecko) '
        r'Chrome/41.0.2227.1 Safari/537.36'}
    logger = logging.getLogger(LOGGER_NAME)
    logger.info('Getting current season')
    page_content = Scraper.get_page_content("https://www.basketball-reference.com/leagues/",
                                            encoding=ENCODING, parser=PARSER, headers=HEADERS)
    current_season = page_content.find_all(
        'th', attrs={'scope': "row", "data-stat": "season"})[0].text
    current_season = int(current_season[:4])+1
    logger.info(f'Current season is {current_season-1}-{current_season}')
    # get_schedule takes later year of the season
    return current_season


def scrape_schedule(*args):
    """
    description

    Parameters
    ----------


    Examples
    --------

    Returns
    -------


    """
    logger = logging.getLogger(LOGGER_NAME)
    logger.info('Getting current season')
    all_schedules = []
    if len(args) == 1:
        year = args[0]
        logger.info(f'1 season to scrape')
        logger.info(f'Scraping {year-1}-{year} season')
        year_schedule = get_schedule(year, playoffs=False)
        year_schedule['season'] = f"{year-1}-{year}"
        all_schedules.append(year_schedule)
    else:
        # range does not include the last argument, therefore +1
        logger.info(f'{args[1]-args[0]+1} season(s) to scrape')
        for year in range(args[0], args[1]+1):
            logger.info(f'Scraping {year-1}-{year} season')
            year_schedule = get_schedule(year, playoffs=False)
            year_schedule['season'] = f"{year-1}-{year}"
            all_schedules.append(year_schedule)

    all_schedules = pd.concat(all_schedules)
    logger.info('Schedules scraped!')
    return all_schedules


def abbr_team_name(all_schedules):
    """
    description

    Parameters
    ----------


    Examples
    --------

    Returns
    -------


    """
    logger = logging.getLogger(LOGGER_NAME)
    logger.info('Converting long team names into abbrs')
    all_schedules.dropna(how='any', inplace=True)
    all_schedules['VISITOR'] = all_schedules['VISITOR'].apply(
        lambda x: x.replace('*', '').upper())
    all_schedules['HOME'] = all_schedules['HOME'].apply(
        lambda x: x.replace('*', '').upper())
    # logger.debug('unique visitor teams:'+str(all_schedules['VISITOR'].unique()))
    # logger.debug('unique home teams:'+str(all_schedules['HOME'].unique()))
    all_schedules = all_schedules.replace(TEAM_TO_TEAM_ABBR)
    # validate team names were converted
    for visitor in all_schedules['VISITOR'].unique():
        try:
            assert len(visitor) == 3, f"{visitor}'s team name needs fix"
        except Exception as err:
            logger.critical(f"{visitor}'s team name needs fix")
            raise err
    for home in all_schedules['HOME'].unique():
        try:
            assert len(visitor) == 3, f"{home}'s team name needs fix"
        except Exception as err:
            logger.critical(f"{home}'s team name needs fix")
            raise err
    logger.info('Converted long team names into abbrs')
    return all_schedules


def gen_aux_info(all_schedules):
    """
    description

    Parameters
    ----------


    Examples
    --------

    Returns
    -------


    """
    logger = logging.getLogger(LOGGER_NAME)
    # generate game_id and URLs
    logger.info(f"Generating {len(all_schedules)} games' game_id and URL")
    box_scores_urls = []
    gameids = []
    for key, row in all_schedules.iterrows():
        nums_to_join = [str(num) for num in [
            row['DATE'].year, f"{row['DATE'].month:02d}", f"{row['DATE'].day:02d}", 0, row['HOME']]]
        url = "https://www.basketball-reference.com/boxscores/" + \
            ''.join(nums_to_join)+".html"
        gameids.append(''.join(nums_to_join))
        box_scores_urls.append(url)
    all_schedules['boxscores_url'] = box_scores_urls
    all_schedules['game_id'] = gameids
    logger.info(
        f"Merged Schedule with {len(all_schedules)} games' game_id and URL")
    return all_schedules


def process_schedule(all_schedules):
    """
    description

    Parameters
    ----------


    Examples
    --------

    Returns
    -------


    """
    logger = logging.getLogger(LOGGER_NAME)
    logger.info("Start processing schedule")
    all_schedules = abbr_team_name(all_schedules)
    processed_schedule = gen_aux_info(all_schedules)
    logger.info("Schedule processed!")
    return processed_schedule


def get_four_factors(url, game_id,session=None):
    """
    description

    Parameters
    ----------


    Examples
    --------

    Returns
    -------


    """
    session = session or requests.Session()
    response = session.get(url, timeout=5)
    html = response.content
    html = html.decode()
    stat_html = html.replace('<!--', "")
    stat_html = stat_html.replace('-->', "")
    soup = BeautifulSoup(stat_html, 'html.parser')
    table = pd.read_html(
        str(soup.find_all('table', attrs={"id": "four_factors"})[0]))[0]
    table = table.droplevel(0, axis=1)
    table = table.rename({'Unnamed: 0_level_1': 'Team'}, axis=1)
    table['game_id'] = game_id
    return table.to_dict('records')


def requests_retry_session(retries=5,backoff_factor=0.3,status_forcelist=(500, 502, 504),session=None):
    """
    description

    Parameters
    ----------


    Examples
    --------

    Returns
    -------


    """
    session = session or requests.Session()
    retry = Retry(
        total=retries,
        read=retries,
        connect=retries,
        backoff_factor=backoff_factor,
        status_forcelist=status_forcelist,
    )
    adapter = HTTPAdapter(max_retries=retry)
    session.mount('http://', adapter)
    session.mount('https://', adapter)
    return session

def get_updating_tasks(collection,field_to_update:str,fields:dict=None)->list:
    """
    Get the documents that needs update, defined by `field_to_update`. 
    Document with empty field, or doesn't contain the filed will be acquired.
    Limit the fields returned by using `fields`

    Parameters
    ----------
    collection : pymongo collection
    field_to_update : a string indicating which field to check
    fields: a dictionary to indicate which fields to pull from mongodb 

    Examples
    --------

    Returns
    -------
    list of documents from Mongodb

    """
    for x in tqdm(fields):
        pass
    logger = logging.getLogger(LOGGER_NAME)
    logger.info(f"Getting tasks: {field_to_update}")
    tasks = list(collection.find(filter={"$or": [{field_to_update: {"$exists": False}}, {field_to_update: None}]}, projection=fields))
    logger.info(f"{len(tasks)} tasks: {field_to_update}")
    return list(tasks)


def refresh():
    #####################################################################################
    # configurations
    # parameters

    # take in raw arguments
    parser = ArgumentParser(
        description="Refresh NBA data in DB or save full data to local (default: %(default)s)")
    parser.add_argument('-v', '--verbose', action='store_true',
                        help="indicate the verbosity level (default: %(default)s)")
    parser.add_argument('-wd', '--working_dir', default='.',
                        help='path of relative working directory (default: %(default)s)')
    parser.add_argument('-dd', '--data_dir', default='data',
                        help='path of data directory (default: %(default)s)')
    # parser.add_argument('-ef','--team_enum_fpath', default='data/team_enums.txt', help='path of team enums file (default: %(default)s)')
    parser.add_argument('-sf', '--save_to_file', action="store_true",
                        help='indicate whether save data to DB (default: %(default)s)')
    parser.add_argument('-fr', '--full_refresh', action="store_true",
                        help="indicate whether perform a full refresh (default: %(default)s)")
    parser.add_argument('-ss', '--start_season', type=int, default=2014,
                        help="start season to refresh (default: %(default)s)")
    raw_args = parser.parse_args()

    # set logging level
    logger = set_logging_config(raw_args.verbose)
    logger.info(F"Running '{__file__}'")

    # process raw arguments
    logger.info('Parsing arguments')

    config = {}
    config['WORKING_DIR'] = Path(raw_args.working_dir)
    config['LOCAL_DATA_FOLDER'] = Path(raw_args.data_dir)
    # config['TEAM_ENUM_FILEPATH'] = Path(raw_args.team_enum_fpath)
    config['SAVE_TO_FILE'] = raw_args.save_to_file
    config['FULL_REFRESH'] = raw_args.full_refresh
    config['START_SEASON'] = raw_args.start_season

    # load these environment variables for DB access
    # TODO: use dotenv
    logger.info('Checking DB access environment variables')
    needed_envs = ['MONGODB_PWD', 'MONGODB_USERNAME', 'MONGODB_ENDPOINT']
    envs = os.environ
    # only checks if user wants to save data to DB
    # check if all needed environment variables are present
    if not config['SAVE_TO_FILE']:
        logger.info("Data will be saved to DB")
        for needed_env in needed_envs:
            if needed_env not in envs:
                raise Exception(f"Missing environment variable: {needed_env}.\n \
            Please check if these environment variables are present: {needed_envs}")
            config[needed_env] = envs[needed_env]

    # set up local data folder if data not save to DB
    if config['SAVE_TO_FILE']:
        raise NotImplementedError
        # local_data_folder = Path(config['LOCAL_DATA_FOLDER'])
        # if not local_data_folder.exists():
        #     os.mkdir(local_data_folder)

    # config['TEAM_ENUM'] = load_team_enum(config['TEAM_ENUM_FILEPATH'])
    logger.debug(config)
    # configurations
    #####################################################################################
    # workflow starts
    logger.info("Work flow starts")

    # get current season year; if current season is 2020-21, current season year will be 2021
    current_season = get_current_season()

    # if full refresh, scrape all year until current year
    if config['FULL_REFRESH']:
        all_schedules = scrape_schedule(config['START_SEASON'], current_season)
    # if not, just scrape current year season
    else:
        all_schedules = scrape_schedule(current_season)

    # clean schedule
    all_schedules = process_schedule(all_schedules)

    # load into Staging db
    # connect to nbaStats db, production, staging collections
    logger.info(
        f"Connecting to {config['MONGODB_ENDPOINT']} as {config['MONGODB_USERNAME']}")
    mongodbio = MongoDBHelper()
    client = mongodbio.create_connection(
        config['MONGODB_USERNAME'], config['MONGODB_PWD'], config['MONGODB_ENDPOINT'])
    nba_db = client['nbaStats']
    coll_nbaGames = nba_db['nbaGames']
    coll_nbaGamesStaging = nba_db['nbaGamesStaging']
    # TODO: add logger into DBHelper
    logger.info('Start inserting new schedule records')
    mongodbio.insert_new_games(all_schedules.to_dict(
        'records'), coll_nbaGames, coll_nbaGamesStaging, id_col_name='game_id')

    # update score - Don't need it for BBR schedule
    #     result=coll_cbaGames.find_one_and_update(
    #         filter={"$and":[{'GameID_Sina':game['GameID_Sina']},
    #                         {'比分':{"$ne":game['比分']}}
    #                         ]},
    #         update={'$set': game})

    # scrape four factors
    # TODO: use multithread

    session = requests_retry_session()
    ff_tasks = get_updating_tasks(collection=coll_nbaGames,field_to_update='four_factors',fields={'boxscores_url':1,'game_id':1})
    # TODO: may need to set headers etc. for session
    # TODO: repeat updating process until nothing left to do
    # TODO: clean up code
    failure_counter = 0
    success_counter = 0
    for task in tqdm(ff_tasks):
        try:
            logger.debug(
                f"Scraping four factors: {task['boxscores_url']},{task['game_id']}")
            four_factors = get_four_factors(
                task['boxscores_url'], task['game_id'], session)
            coll_nbaGames.find_one_and_update(filter={"_id": task['_id']}, update={
                                              '$set': {'four_factors': four_factors}})
            success_counter += 1
        except Exception as err:
            failure_counter += 1
            logger.debug(
                f"Error encountered: {task['boxscores_url']},{task['game_id']}")
            logger.warning(
                f"Error encountered in getting four factors. Error: {repr(err)}")
            if failure_counter > 10:
                logger.warning(
                    'Too many failures encountered. Skipping this step.')
                break
    logger.info(
        f"{success_counter} four factors updated,{failure_counter} failures encountered.")

    # scrape box scores
    logger.info('Pulling list of games that needs box scores')
    # bs_tasks = get_updating_tasks(collection=coll_nbaGames,field_to_update='four_factors')

    bs_tasks = mongodbio.select_records(coll_nbaGames, filter={"$or": [{'basic_boxscores': {
                                        "$exists": False}}, {'basic_boxscores': None}]}, field={'four_factors': 0, 'boxscores_url': 0,'VISITOR_PTS':0,'HOME_PTS':0})
    logger.info(f"{len(bs_tasks)} games don't have box scores")
    failure_counter = 0
    success_counter = 0
    for task in tqdm(bs_tasks):
        try:
            logger.debug(
                f"Scraping box scores: {task['game_id']}")
            basic_boxscores = get_box_scores(
                task['DATE'], task['HOME'], task['VISITOR'], period='GAME', stat_type='BASIC',session=session)
            basic_boxscores[task['HOME']] = basic_boxscores[task['HOME']].to_dict("records")
            basic_boxscores[task['VISITOR']] = basic_boxscores[task['VISITOR']].to_dict("records")
            coll_nbaGames.find_one_and_update(filter={"_id": task['_id']}, update={
                                              '$set': {'basic_boxscores': basic_boxscores}})
            advanced_boxscores = get_box_scores(
                task['DATE'], task['HOME'], task['VISITOR'], period='GAME', stat_type='ADVANCED',session=session)
            advanced_boxscores[task['HOME']] = advanced_boxscores[task['HOME']].to_dict("records")
            advanced_boxscores[task['VISITOR']] = advanced_boxscores[task['VISITOR']].to_dict("records")
            coll_nbaGames.find_one_and_update(filter={"_id": task['_id']}, update={
                                              '$set': {'advanced_boxscores': advanced_boxscores}})
            success_counter += 1
        except Exception as err:
            failure_counter += 1
            logger.debug(
                f"Error encountered: {task['game_id']}")
            logger.warning(
                f"Error encountered in getting four factors. Error: {repr(err)}")
            if failure_counter > 10:
                logger.warning(
                    'Too many failures encountered. Skipping this step.')
                raise err
    logger.info(
        f"{success_counter} box scores updated,{failure_counter} failures encountered.")

    # after basic data are scraped, start constructing training data (in another script)


if __name__ == '__main__':
    refresh()
    # print(get_current_season())
