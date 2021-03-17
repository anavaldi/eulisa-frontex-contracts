import json
import re
import pandas as pd
import yaml
import numpy as np


from sklearn.preprocessing import MinMaxScaler


# Open yaml
with open('../config_file.yaml', 'r') as f:
    configs = yaml.load(f)

import sys
sys.path.insert(0, configs['ROOT_PATH'] + configs['UTILS_PATH'])

from utils import *

if __name__== "__main__":
    df_contractors = pd.read_csv('..' + configs['DATA_PATH'] + '/etendering_contractors_' + configs['AGENCY'] + '.csv')

    # Build graph DataFrame
    logger.info("Buiding graph dataset.")
    ids = []
    sources = []
    targets = []
    weights = []

    ids, sources, targets, weights = df_to_graph(df_contractors, ids, sources, targets, weights)
    df_graph = pd.DataFrame([ids, sources, targets, weights]).T
    df_graph.columns = ['id_contract', 'source', 'target', 'weight']
    if configs['AGENCY'] == 'eulisa':
        df_graph = df_clean_graph_eulisa(df_graph)
    elif configs['AGENCY'] == 'frontex':
        df_graph = df_clean_graph_frontext(df_graph)

    df_graph = scale_edge_weights(df_graph)
    logger.info('Writing dataset.')
    df_graph.to_csv('..' + configs['DATA_PATH'] + '/etendering_graph_' + configs['AGENCY'] + '.csv', index=False)
