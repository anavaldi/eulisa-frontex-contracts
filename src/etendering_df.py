import json
import re
import pandas as pd
import yaml
import numpy as np



# Open yaml
with open('../config_file.yaml', 'r') as f:
    configs = yaml.load(f)

import sys
sys.path.insert(0, configs['ROOT_PATH'] + configs['UTILS_PATH'])

from utils import *

if __name__== "__main__":
    # Read raw document
    corpus = read_corpus(configs)

    # Structure the document into a JSON file
    # Note that we only extract info of 'Contract award notice' documents
    corpus_raw_list = corpus.split('I.II.')
    contracts_json = [make_json(c) for c in corpus_raw_list if re.search('Contract award notice', c)]
    write_json(contracts_json, configs)

    # From JSON to dataFrame
    df = json_to_df(contracts_json)
    logger.info('Writing CSV file.')

    # Clean DataFrame
    df_clean = clean_df(df)
    df_clean.to_csv('..' + configs['DATA_PATH'] + '/etendering_contracts_' + configs['AGENCY'] + '.csv', index=False)

    # Some contracts are duplicated because they got more than one contractor.
    # We will create another dataset with 1 contract per contractor.
    df_contractors = create_df_contractors(contracts_json, df_clean)
    df_contractors_clean = clean_df_contractors(df_contractors)
    logger.info('Data successfully cleaned. Writing the clean file.')

    df_contractors_clean.to_csv('..' + configs['DATA_PATH'] + '/etendering_contractors_' + configs['AGENCY'] + '.csv', index=False)
