# import neptune
import datetime
import time
from pathlib import Path

import numpy as np
import pandas as pd
from fastai.tabular.all import *
from pandas.api.types import (is_categorical_dtype, is_numeric_dtype,
                              is_string_dtype)
from sklearn.ensemble import RandomForestRegressor
from sklearn.model_selection import train_test_split
from sklearn.tree import DecisionTreeRegressor
from tqdm.notebook import tqdm

# import click

# overall objective:
    # easy to run
    # accepts new data or maybe directly take xs,y (easy to change dependent variables and independent variables)
    # run in jupyter notebooks

# parameters
# objective:classification, regression
# models: decision tree, 

# To help you decide between options and arguments, 
# the recommendation is to use arguments exclusively for things 
# like going to subcommands or input filenames / URLs, 
# and have everything else be an option instead.

# @click.command()
# @click.option('--count', default=1, required=True, type=int, help='number of greetings')
# @click.argument('name')
# def hello(count, name):
#     for x in range(count):
#         click.echo('Hello %s!' % name)
