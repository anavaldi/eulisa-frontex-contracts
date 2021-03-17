import logger
import logging
import sys
import pandas as pd
import json
import datetime
import numpy as np
import re
import boto3
import glob

'''COLORED LOGGING'''
BLACK, RED, GREEN, YELLOW, BLUE, MAGENTA, CYAN, WHITE = range(8)

# These are the sequences need to get colored ouput
RESET_SEQ = "\033[0m"
COLOR_SEQ = "\033[1;%dm"
BOLD_SEQ = "\033[1m"

def formatter_message(message, use_color = True):
    if use_color:
        message = message.replace("$RESET", RESET_SEQ).replace("$BOLD", BOLD_SEQ)
    else:
        message = message.replace("$RESET", "").replace("$BOLD", "")
    return message

COLORS = {
    'WARNING': YELLOW,
    'INFO': WHITE,
    'DEBUG': BLUE,
    'CRITICAL': YELLOW,
    'ERROR': RED
}

class ColoredFormatter(logging.Formatter):
    def format(self, record):
        levelname = record.levelname
        if levelname in COLORS:
            levelname_color = COLOR_SEQ % (30 + COLORS[levelname]) + levelname + RESET_SEQ
            record.levelname = levelname_color
        return logging.Formatter.format(self, record)

def create_logger(name='Experiment', **kwargs):
    '''This functions defines the Logger called Experiment, with the relevant colored formatting'''
    if 'level' in kwargs:
        LOGGER_LEVEL = kwargs['level']
    else:
        LOGGER_LEVEL = 'DEBUG'
    if 'filename' in kwargs:
        LOGGER_FILE = kwargs['filename']
    else:
        LOGGER_FILE = None
    fh = logging.StreamHandler(sys.stdout)
    f = ColoredFormatter('[%(name)s] - %(levelname)s - %(asctime)s: %(message)s ', '%H:%M')
    fh.setFormatter(f)
    test = logging.getLogger(name)
    test.propagate = False
    test.setLevel(LOGGER_LEVEL)
    if not len(test.handlers):
        test.addHandler(fh)

    if LOGGER_FILE is not None:
        fh = logging.FileHandler(LOGGER_FILE)
        fh.setFormatter(logging.Formatter('[%(name)s] - %(levelname)s: %(message)s - %(asctime)s ', datefmt='%H:%M'))
        fh.setLevel(LOGGER_LEVEL)
        test.addHandler(fh)

    return test

logger=create_logger()

def write_data_csv(df, wordkey, schema, **config):
	"""
    Write data into raw folder.
    """
	logger.info('Writting dataset into ' + schema + '...')
	df.to_csv(config['ROOT_PATH'] + config['DATA_PATH'] + '/' + schema + '/' + wordkey + '.csv')

def write_data_json(df, wordkey, schema, **config):
	"""
    Write data into raw folder.
    """
	logger.info('Writting dataset into ' + schema + '...')
	df = df.reset_index()
	df.to_json(config['ROOT_PATH'] + config['DATA_PATH'] + '/' + schema + '/' + wordkey + '.json')


def read_data_csv(wordkey, schema, **config):
	"""
    Write data into raw folder.
    """
	logger.info('Reading dataset: ' +  wordkey + ' from ' + schema + '...')
	df = pd.read_csv(config['ROOT_PATH'] + config['DATA_PATH'] + '/' + schema + '/' + wordkey + '.csv')
	return df

def read_data_json(wordkey, schema, **config):
	"""
    Write data into raw folder.
    """
	logger.info('Reading dataset: ' +  wordkey + ' from ' + schema + '...')
	df = pd.read_json(config['ROOT_PATH'] + config['DATA_PATH'] + '/' + schema + '/' + wordkey + '.json')
	return df
