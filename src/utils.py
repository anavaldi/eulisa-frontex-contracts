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

# fuzz is used to compare TWO strings
from fuzzywuzzy import fuzz

# process is used to compare a string to MULTIPLE other strings
from fuzzywuzzy import process

from sklearn.preprocessing import MinMaxScaler

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


def read_corpus(config):
    """
    Read corpus of contracts.
    """
    logger.info('Reading corpus of ' + config['AGENCY'] + '.')
    f = open('..' + config['DATA_PATH'] + '/raw/corpus_etendering_' + config['AGENCY'] + '.txt', 'r')
    corpus = f.read()
    f.close()
    return corpus

# Functions to structure the document into a dictionary file
def extract_id(text):
    """
    Function that extracts the ID of contracts.
    """
    id_aux = re.search(r'/S .(.*?)\n', text)
    id_aux = re.search(r'-.(.*?)\n', id_aux.group(0))
    id_aux = re.sub('-', '', id_aux.group(0))
    id_aux = re.sub('\n', '', id_aux)
    return id_aux

def extract_year(text):
    """
    Function that extracts the year of contracts.
    """
    year_aux = re.search(r'\n.(.*?)/S ', text)
    year_aux = re.sub('/S ', '', year_aux.group(0))
    year_aux = re.sub('\n', '', year_aux)
    return year_aux

def extract_entity(text, entity_name):
    """
    Extract entities with colon.
    Like 'Contracting authority:'.
    """
    try:
        entity_aux = re.search(entity_name + ': (.*)\n', text)
        entity_aux = re.sub(entity_name + ': ', '', entity_aux.group(0))
        entity_aux = re.sub('\n', '', entity_aux)
        return entity_aux
    except:
        entity_aux = 'NA'
        return entity_aux

def extract_entity_2(text, entity_name):
    """
    Entities without colon.
    Like 'Contracting authority'
    """
    try:
        entity_aux = re.search(entity_name + '\n(.*)\n', text)
        entity_aux = re.sub(entity_name, '', entity_aux.group(0))
        entity_aux = re.sub('\n', '', entity_aux)
        return entity_aux
    except:
        entity_aux = 'NA'
        return entity_aux

def extract_subentities(text, entity_name, sub_entity_name):
    """
    Extract subentities of an entity.
    """
    try:
        entities = []
        entities_aux = re.split(entity_name, text)
        for ent in range(1, len(entities_aux)):
            ent_aux = re.search(sub_entity_name + ':(.*)\n', entities_aux[ent])
            ent_aux = re.sub(sub_entity_name + ':', '', ent_aux.group(0))
            ent_aux = re.sub('\n', '', ent_aux)
            entities.append(ent_aux)
        return entities
    except:
        entity_aux = 'NA'
        return entity_aux

def make_json(text):
    """
    Function that structures a corpus into a dictionary (JSON file).
    """
    d = {}
    # ID & YEAR
    d['id'] = extract_id(text) + '-' + extract_year(text)
    d['year'] = extract_year(text)

    # Section I
    d['contracting_authority'] = {
        'official_name': extract_entity(text, 'Official name'),
        'postal_address': extract_entity(text, 'Postal address'),
        'town': extract_entity(text, 'Town'),
        'postal_code': extract_entity(text, 'Postal code'),
        'nuts': extract_entity(text, 'NUTS code'),
        'country': extract_entity(text, 'Country'),
        'email': extract_entity(text, 'E-mail'),
        'type_contracting_authority': extract_entity_2(text, 'Type of the contracting authority'),
        'main_activity': extract_entity_2(text, 'Main activity')
    }

    text = re.sub('NUTS Code', 'NUTS code', text)

    if(int(d['year']) > 2015):
        # Section II
        d['object'] = {
            'title': extract_entity_2(text, 'Title:\n'),
            'cpv': extract_entity_2(text, 'Main CPV code'),
            'type': extract_entity_2(text, 'Type of contract'),
            'description': extract_entity_2(text, 'Short description:'),
            'total_value': extract_entity(text, 'Value excluding VAT'),
            'lots': extract_entity_2(text, 'Information about lots'),
            'cpv_2': extract_entity_2(text, 'Additional CPV code\(s\)'),
            'award_criteria': extract_entity_2(text, 'Award criteria'),
            'duration': extract_entity_2(text, 'Duration of the contract, framework agreement or dynamic purchasing system')
        }
        # Section IV
        d['procedure'] = {
            'type': extract_entity_2(text, 'Type of procedure')
        }
        if(extract_entity_2(text, 'Information about lots') == 'This contract is divided into lots: yes'):
            contracts_text = re.split('Section V: Award of contract', text)
            contracts_list = []
            d_aux = {}
            for c in range(1, len(contracts_text)):
                # Section V
                d_aux['award_of_contract'] = {
                    'contract_no': extract_entity(contracts_text[c], 'Contract No'),
                    'number_tenders_received': extract_entity(contracts_text[c], 'Number of tenders received'),
                    'group_economic_operators': extract_entity(contracts_text[c], 'The contract has been awarded to a group of economic operators'),
                    'subcontracting': extract_entity_2(contracts_text[c], 'Information about subcontracting'),
                    'contractors': extract_subentities(contracts_text[c], 'Name and address of the contractor', 'Official name'),
                    'contractors_postal_address': extract_subentities(contracts_text[c], 'Name and address of the contractor', 'Postal address'),
                    'contractors_town': extract_subentities(contracts_text[c], 'Name and address of the contractor', 'Town'),
                    'contractors_nuts': extract_subentities(contracts_text[c], 'Name and address of the contractor', 'NUTS code'),
                    'contractors_postal_code': extract_subentities(contracts_text[c], 'Name and address of the contractor', 'Postal code'),
                    'contractors_country': extract_subentities(contracts_text[c], 'Name and address of the contractor', 'Country'),
                    'contractors_sme': extract_subentities(contracts_text[c], 'Name and address of the contractor', 'SME'),
                    'total_value': extract_entity(contracts_text[c], 'Total value of the contract/lot')

                }
                contracts_list.append(d_aux['award_of_contract'])

            d['award_of_contracts'] = contracts_list


        else:
            # Section V
            contracts_list = []
            d_aux = {}
            d_aux['award_of_contract'] = {
                'contract_no': extract_entity(text, 'Contract No'),
                'number_tenders_received': extract_entity(text, 'Number of tenders received'),
                'group_economic_operators': extract_entity(text, 'The contract has been awarded to a group of economic operators'),
                'subcontracting': extract_entity_2(text, 'Information about subcontracting'),
                'contractors': extract_subentities(text, 'Name and address of the contractor', 'Official name'),
                'contractors_postal_address': extract_subentities(text, 'Name and address of the contractor', 'Postal address'),
                'contractors_town': extract_subentities(text, 'Name and address of the contractor', 'Town'),
                'contractors_nuts': extract_subentities(text, 'Name and address of the contractor', 'NUTS code'),
                'contractors_postal_code': extract_subentities(text, 'Name and address of the contractor', 'Postal code'),
                'contractors_country': extract_subentities(text, 'Name and address of the contractor', 'Country'),
                'contractors_sme': extract_subentities(text, 'Name and address of the contractor', 'SME'),
                'total_value': extract_entity(text, 'Total value of the contract/lot')
            }
            contracts_list.append(d_aux['award_of_contract'])
            d['award_of_contracts'] = contracts_list

    else:
        # Section II
        d['object'] = {
            'title': extract_entity_2(text, 'Title attributed to the contract'),
            'cpv': extract_entity_2(text, 'Common procurement vocabulary \(CPV\)'),
            'description': extract_entity_2(text, 'Short description of the contract of purchase\(s\)'),
            'type': extract_entity_2(text, 'Type of contract and location of works, place of delivery or of performance'),
            'total_value': extract_entity(text, 'Value')
        }

        # Section IV
        d['procedure'] = {
            'type': extract_entity_2(text, 'IV.1.1\)Type of procedure'),
            'award_criteria': extract_entity_2(text, 'IV.2.1\)Award criteria')
        }

        # Section V
        if( (extract_entity_2(text, 'Information about lots') == 'This contract is divided into lots: yes') or (d['id'] == '221695-2015')):
            contracts_text = re.split('Contract No:', text)
            contracts_list = []
            d_aux = {}
            for c in range(1, len(contracts_text)):
                # Section V
                d_aux['award_of_contract'] = {
                    'contract_no': extract_entity(contracts_text[c], 'LISA/'),
                    'number_tenders_received': extract_entity(contracts_text[c], 'Number of offers received'),
                    'group_economic_operators': extract_entity(contracts_text[c], 'The contract has been awarded to a group of economic operators'),
                    'subcontracting': extract_entity_2(contracts_text[c], 'Information about subcontracting'),
                    'contractors': extract_subentities(contracts_text[c], 'Name and address of economic operator in favour of whom the contract award decision has been taken', 'Official name'),
                    'contractors_postal_address': extract_subentities(contracts_text[c], 'Name and address of economic operator in favour of whom the contract award decision has been taken', 'Postal address'),
                    'contractors_town': extract_subentities(contracts_text[c], 'Name and address of economic operator in favour of whom the contract award decision has been taken', 'Town'),
                    'contractors_postal_code': extract_subentities(contracts_text[c], 'Name and address of economic operator in favour of whom the contract award decision has been taken', 'Postal code'),
                    'contractors_country': extract_subentities(contracts_text[c], 'Name and address of economic operator in favour of whom the contract award decision has been taken', 'Country'),
                    'total_value': extract_entity(contracts_text[c], 'Total final value of the contract:\nValue')
                }
                contracts_list.append(d_aux['award_of_contract'])

            d['award_of_contracts'] = contracts_list


        else:
            # Section V
            contracts_list = []
            d_aux = {}
            d_aux['award_of_contract'] = {
                'contract_no': extract_entity(text, 'Contract No'),
                'number_tenders_received': extract_entity(text, 'Number of offers received'),
                'group_economic_operators': extract_entity(text, 'The contract has been awarded to a group of economic operators'),
                'subcontracting': extract_entity_2(text, 'Information about subcontracting'),
                'contractors': extract_subentities(text, 'Name and address of economic operator in favour of whom the contract award decision has been taken', 'Official name'),
                'contractors_postal_address': extract_subentities(text, 'Name and address of economic operator in favour of whom the contract award decision has been taken', 'Postal address'),
                'contractors_town': extract_subentities(text, 'Name and address of economic operator in favour of whom the contract award decision has been taken', 'Town'),
                'contractors_postal_code': extract_subentities(text, 'Name and address of economic operator in favour of whom the contract award decision has been taken', 'Postal code'),
                'contractors_country': extract_subentities(text, 'Name and address of economic operator in favour of whom the contract award decision has been taken', 'Country'),
                'total_value': extract_entity(text, '\nTotal final value of the contract:\nValue')
            }
            contracts_list.append(d_aux['award_of_contract'])
            d['award_of_contracts'] = contracts_list
    return d

def write_json(json_file, config):
    """
    Write the JSON file.
    """
    logger.info("Corpus succesfully structured. Writing JSON file.")
    with open('..' + config['DATA_PATH'] + '/etendering_' + config['AGENCY'] + '.json', 'w', encoding='utf-8') as f:
        json.dump(json_file, f, ensure_ascii=False, indent=4)

def json_to_df(json_file):
    """
    Structure the JSON file as a dataFrame.
    """
    ids = []
    years = []

    contracting_authority_official_names = []
    contracting_authority_countries = []
    contracting_authority_nuts = []
    contracting_authority_main_activity = []

    object_title = []
    object_type = []
    object_description = []
    object_total_value = []
    object_cpv = []
    #duration only available if year > 2015

    procedure_type = []

    award_criteria = []

    for contract in json_file:
        ids.append(contract[u'id'])
        years.append(contract[u'year'])

        # Section I (Contracting authority)
        contracting_authority_official_names.append(contract[u'contracting_authority'][u'official_name'])
        contracting_authority_countries.append(contract[u'contracting_authority'][u'country'])
        contracting_authority_nuts.append(contract[u'contracting_authority'][u'nuts'])
        contracting_authority_main_activity.append(contract[u'contracting_authority'][u'main_activity'])

        # Section II (Object)
        object_title.append(contract[u'object'][u'title'])
        object_type.append(contract[u'object'][u'type'])
        object_description.append(contract[u'object'][u'description'])
        object_total_value.append(contract[u'object'][u'total_value'])
        object_cpv.append(contract[u'object'][u'cpv'])
        if(int(contract[u'year']) > 2015):
            award_criteria.append(contract[u'object'][u'award_criteria'])
        else:
            award_criteria.append(contract[u'procedure'][u'award_criteria'])

        # Section IV (Procedure)
        procedure_type.append(contract[u'procedure']['type'])

    df = pd.DataFrame([ids,years,
                       contracting_authority_official_names, contracting_authority_countries,
                       contracting_authority_nuts, contracting_authority_main_activity,
                      object_title, object_type, object_description, object_total_value, object_cpv,
                      award_criteria, procedure_type]).T

    df.columns = ['id', 'year', 'contracting_authority_official_name', 'contracting_authority_country',
                  'contracting_authority_nut', 'contracting_authority_main_activity',
                  'object_title', 'object_type', 'object_description', 'object_total_value', 'cpv',
                  'award_criteria', 'procedure_type']

    #Create URL column
    df['url'] = 'https://ted.europa.eu/udl?uri=TED:NOTICE:' + df['id'] + ':TEXT:EN:HTML&src=0'
    return df

def clean_df(df):
    # Clean price
    df['object_total_value_clean'] = df['object_total_value'].str.replace('EUR', '')
    df['object_total_value_clean'] = df['object_total_value_clean'].str.replace(' ', '')
    df['object_total_value_clean'] = df['object_total_value_clean'].str.replace(',', '.')

    # Frontex: a budget is in PLN (zloty)
    # On 07/01/2021 1 EUR is 0,22 PLN
    for index, row in df.iterrows():
        if 'PLN' in row['object_total_value']:
            df.at[index, 'object_total_value_clean'] = df.at[index, 'object_total_value'].replace('PLN', '')
            df.at[index, 'object_total_value_clean'] = df.at[index, 'object_total_value_clean'].replace(' ', '')
            df.at[index, 'object_total_value_clean'] = df.at[index, 'object_total_value_clean'].replace(',', '.')
            df.at[index, 'object_total_value_clean'] = float(df.at[index, 'object_total_value_clean'])*0.22

    df_clean = df[df['object_total_value_clean'] != 'NA']
    df_clean['object_total_value_clean'] = df_clean['object_total_value_clean'].astype(float)
    return df_clean

def create_df_contractors(json_file, df_clean):
    # Create dataset for Section V
    id_contracts = []

    award_of_contracts_number_tenders = []
    award_of_contracts_group_economic_operators = []
    award_of_contracts_subcontracting = []
    award_of_contracts_contractors = []
    award_of_contracts_contractors_countries = []
    award_of_contracts_total_value = []

    for contract in json_file:
        contractors_list = contract[u'award_of_contracts']
        for contractors in contractors_list:
            id_contracts.append(contract[u'id'])
            award_of_contracts_number_tenders.append(contractors[u'number_tenders_received'])
            award_of_contracts_group_economic_operators.append(contractors[u'group_economic_operators'])
            award_of_contracts_subcontracting.append(contractors[u'subcontracting'])
            award_of_contracts_contractors.append(contractors[u'contractors'])
            award_of_contracts_contractors_countries.append(contractors[u'contractors_country'])
            award_of_contracts_total_value.append(contractors[u'total_value'])

    df_contractors = pd.DataFrame([id_contracts, award_of_contracts_number_tenders,
                           award_of_contracts_group_economic_operators, award_of_contracts_subcontracting,
                           award_of_contracts_contractors, award_of_contracts_contractors_countries,
                           award_of_contracts_total_value]).T

    df_contractors.columns = ['id', 'tenders', 'group_economic_operator', 'subcontracting',
                  'contractors', 'contractors_countries', 'contractors_total_value']
    df_contractors = df_clean.merge(df_contractors, how='right', on='id', validate='one_to_many')
    return df_contractors

def clean_df_contractors(df_contractors):
    # Remove euro string and spaces
    df_contractors['contractors_total_value_clean'] = df_contractors['contractors_total_value'].str.replace('EUR', '')
    df_contractors['contractors_total_value_clean'] = df_contractors['contractors_total_value_clean'].str.replace(' ', '')
    df_contractors['contractors_total_value_clean'] = df_contractors['contractors_total_value_clean'].str.replace(',', '.')

    # Frontex: a budget is in PLN (zloty)
    # On 07/01/2021 1 EUR is 0,22 PLN
    for index, row in df_contractors.iterrows():
        if 'PLN' in row['contractors_total_value']:
            df_contractors.at[index, 'contractors_total_value_clean'] = df_contractors.at[index, 'contractors_total_value'].replace('PLN', '')
            df_contractors.at[index, 'contractors_total_value_clean'] = df_contractors.at[index, 'contractors_total_value_clean'].replace(' ', '')
            df_contractors.at[index, 'contractors_total_value_clean'] = df_contractors.at[index, 'contractors_total_value_clean'].replace(',', '.')
            df_contractors.at[index, 'contractors_total_value_clean'] = float(df_contractors.at[index, 'contractors_total_value_clean'])*0.22

    # Remove empty rows
    df_contractors = df_contractors.loc[df_contractors['contractors_total_value_clean'] != 'NA']
    # List to string
    df_contractors['contractors_clean'] = [','.join(i) if isinstance(i, list) else i for i in df_contractors['contractors']]
    # Clean names
    # Bridge 3 Consortium
    df_contractors.loc[df_contractors['contractors_clean'] == ' Consortium Bridge³, represented by the Group Leader Accenture NV/SA, Consortium Bridge: consortium member Atos Belgium NV/SA, Consortium Bridge³, consortium member Morpho SAS', 'contractors_clean'] = 'Bridge3 Consortium (Accenture NV/SA, HP Belgium and Morpho)'
    df_contractors.loc[df_contractors['contractors_clean'] == ' Bridge3 Consortium (leader: Accenture NV/SA)', 'contractors_clean'] = 'Bridge3 Consortium (Accenture NV/SA, HP Belgium and Morpho)'
    df_contractors.loc[df_contractors['contractors_clean'] == ' Bridge3 consortium (leader: Accenture NV/SA), Bridge3 consortium (leader: Accenture NV/SA)', 'contractors_clean'] = 'Bridge3 Consortium (Accenture NV/SA, HP Belgium and Morpho)'
    df_contractors.loc[df_contractors['contractors_clean'] == ' Bridge3 consortium (leader: Accenture NV/SA)' , 'contractors_clean'] = 'Bridge3 Consortium (Accenture NV/SA, HP Belgium and Morpho)'

    # S3B Consortium
    df_contractors.loc[df_contractors['contractors_clean'] == ' Consortium S3B, consisting of Steria Benelux SA/NV (group leader), 3M Belgium BVBA/SPRL, Bull SAS', 'contractors_clean'] = 'S3B Consortium (Steria BE, Bull, Gemalto Cogent)'
    df_contractors.loc[df_contractors['contractors_clean'] == ' Consortium S3B, represented by the group leader Sopra Steria Benelux SA, Consortium S3B, consortium member: Bull SAS, Consortium S3B, consortium member: 3M Belgium BVBA', 'contractors_clean'] = 'S3B Consortium (Steria BE, Bull, Gemalto Cogent)'
    df_contractors.loc[df_contractors['contractors_clean'] == 'S3B Consortium (Steria BE, Bull, Gemalto Cogent)', 'contractors_clean'] = 'S3B Consortium (Steria BE, Bull, Gemalto Cogent)'

    # Infeurope
    df_contractors.loc[df_contractors['contractors_clean'] == ' Infeurope SA', 'contractors_clean'] = 'Infeurope SA'
    df_contractors.loc[df_contractors['contractors_clean'] == ' INFEUROPE S.A., imc information multimedia communication AG', 'contractors_clean'] = 'Infeurope SA'
    df_contractors.loc[df_contractors['contractors_clean'] == ' INFEUROPE S.A., imc information multimedia communication AG', 'contractors_clean'] = 'Infeurope SA'


    # Infeurope
    df_contractors.loc[df_contractors['contractors_clean'] == ' AS G4S EESTI', 'contractors_clean'] = 'AS G4S Eesti'
    df_contractors.loc[df_contractors['contractors_clean'] == ' AS G4S Eesti', 'contractors_clean'] = 'AS G4S Eesti'

    # ELIN GmbH
    df_contractors.loc[df_contractors['contractors_clean'] == ' ELIN GmbH', 'contractors_clean'] = 'ELIN GmbH'
    df_contractors.loc[df_contractors['contractors_clean'] == ' ELIN GmbH & Co KG', 'contractors_clean'] = 'ELIN GmbH'

    # Axima Concept
    df_contractors.loc[df_contractors['contractors_clean'] == ' Axima Concept', 'contractors_clean'] = 'Axima Concept SA'
    df_contractors.loc[df_contractors['contractors_clean'] == ' Axima Concept S.A.', 'contractors_clean'] = 'Axima Concept SA'

    # Consortium IBM Belgium BVBA
    df_contractors.loc[df_contractors['contractors_clean'] == ' Consortium IBM Belgium BVBA, Atos Belgium NV and Leonardo S.p.a, represented by the Group Leader IBM Belgium BVBA, Atos Belgium NV, Leonardo S.p.a', 'contractors_clean'] = 'Consortium IBM Belgium BVBA, Atos, and Leonardo'

    # European Dynamics
    df_contractors.loc[df_contractors['contractors_clean'] == ' European Dynamics Luxembourg SA (Group Leader), European Dynamics SA, European Dynamics Belgium SA', 'contractors_clean'] = 'European Dynamics'

    # Car Master
    df_contractors.loc[df_contractors['contractors_clean'] == ' Car Master 2 Sp. z o.o. Sp.k', 'contractors_clean'] = ' Car Master 2 Sp. z o.o. Sp.k'

    # Consortium  CAE Aviation, DEA Aviation Ltd, EASP Air BV
    df_contractors.loc[df_contractors['contractors_clean'] == ' CAE Aviation, DEA Aviation Ltd, EASP Air BV', 'contractors_clean'] = ' CAE Aviation, DEA Aviation, EASP Air BV'
    df_contractors.loc[df_contractors['contractors_clean'] == ' CAE Aviation, DEA Aviation, EASP Air BV', 'contractors_clean'] = ' CAE Aviation, DEA Aviation, EASP Air BV'

    # Clean first space
    df_contractors['contractors_clean'] = np.where(df_contractors['contractors_clean'].str[1:] == ' ', df_contractors['contractors_clean'].str[1:], df_contractors['contractors_clean'])

    # Create new columns that shows if the contract is related with VIS, EURODAC, or SIS.

    df_contractors['object_title'] = df_contractors['object_title'].astype(str)

    df_contractors['VIS'] = np.where(df_contractors['object_title'].str.contains(r'VIS'), True, False)
    df_contractors['Eurodac'] = np.where(df_contractors['object_title'].str.contains(r'Eurodac'), True, False)
    df_contractors['SIS I'] = np.where(df_contractors['object_title'].str.contains(r'SIS I '), True, False)
    df_contractors['SIS II'] = np.where(df_contractors['object_title'].str.contains(r'SIS II'), True, False)
    df_contractors['Entry Exit System'] = np.where(df_contractors['object_title'].str.contains(r'Entry Exit System'), True, False)

    # From string to number
    df_contractors.contractors_total_value_clean = df_contractors.contractors_total_value_clean.astype(float)
    return df_contractors

def df_to_graph(df_contractors, ids, source, target, weight):
    """
    Function that transforms df_contractors
    to a DataFrame Gephi-friendly.
    """
    df_contractors['contractors_clean'].astype(str)
    for index, row in df_contractors.iterrows():
        #print('Contractor: ' + contractor)
        contractor = row['contractors_clean']
        value = row['contractors_total_value_clean']
        contractor = re.sub(r'Business, trade', 'Business trade', contractor)
        contractor = re.sub(r'Unisys SA (group leader) and', 'Unisys SA (group leader) ,', contractor)

        # to improve code -> if consortium and and -> and is ,
        #contractor = re.sub(r' Consortium U2 — Unisys SA (group leader) and UniSystems Information Technology Systems Commercial SA', 'U2 Consortium Unisys SA (group leader) and UniSystems Information Technology Systems Commercial SA', contractor)
        if contractor == ' Consortium U2 — Unisys SA (group leader) and UniSystems Information Technology Systems Commercial SA':
            contractor = 'U2 Consortium Unisys SA (group leader) and UniSystems Information Technology Systems Commercial SA'

        #contractor = re.sub(r'Bridge3 Consortium (Accenture NV/SA, HP Belgium and Morpho)', 'Bridge3 Consortium (Accenture NV/SA, HP Belgium, Morpho)', contractor)
        #contractor = re.sub(r' Consortium ALT+ENTER, consisting of Accenture SA (group leader) and Altran Technologies SA', ' Consortium ALT+ENTER, consisting of Accenture SA (group leader), Altran Technologies SA', contractor)
        #contractor = re.sub(r' Consortium Bull–Atos–Ernst & Young, consisting of Bull SAS (group leader), Atos Integration SAS and Ernst & Young et Associés', ' Consortium Bull–Atos–Ernst & Young, consisting of Bull SAS (group leader), Atos Integration SAS, Ernst & Young et Associés', contractor)
        #contractor = re.sub(r' Consortium ACTO, consisting of Accenture SA (group leader) and Tieto Estonia AS', ' Consortium ACTO, consisting of Accenture SA (group leader), Tieto Estonia AS', contractor)
        #contractor = re.sub(r' Tarkus consortium, consisting of Everis Spain SLU succursale en Belgique (group leader), Deloitte Consulting CVBA and AS CGI Eesti', ' Tarkus consortium, consisting of Everis Spain SLU succursale en Belgique (group leader), Deloitte Consulting CVBA, AS CGI Eesti', contractor)
        #contractor = re.sub(r' Consortium Civitta, consisting of Civitta Eesti AS (group leader), Innopolis Insenerid OÜ and Civitta UAB', ' Consortium Civitta, consisting of Civitta Eesti AS (group leader), Innopolis Insenerid OÜ, Civitta UAB', contractor)
        #contractor = re.sub(r' Mostra SA, Consortium Propager SARL and North East West South (NEWS) Travel SA', ' Mostra SA, Consortium Propager SARL, North East West South (NEWS) Travel SA', contractor)


        # contractor = re.sub(r'and', ',', contractor)

        num_of_contractors = contractor.count(',')
        if num_of_contractors > 0:
            contractor = re.sub('(group leader)','', contractor)
            contractor = re.sub('(Group leader)','', contractor)
            contractor = re.sub('(Group Leader)','', contractor)
            contractor = re.sub('(leader)','', contractor)
            contractor = re.sub('(Leader)','', contractor)
            contractor = re.sub('(member)', '', contractor)
            contractor = re.sub(r'[\(\)]', '', contractor)
            contractor = re.sub(', and ', ',', contractor)
            #contractor = re.sub(' and ', ',', contractor)


            #contractor = re.sub(r', consisting of', ' consisting of', contractor)
            if 'consortium' in contractor.lower():
                contractor = re.sub(', and', ', ', contractor)
                contractor = re.sub(' and ', ', ', contractor)
                contractor = re.sub('.*\(?Consortium ', '', contractor)
                contractor = re.sub('.*\(?consortium ', '', contractor)
                contractor = re.sub(r'\(', '', contractor)
                contractor = re.sub(r'\)', '', contractor)
                contractor = re.sub('.*\(? —— ', '', contractor)
                contractor = re.sub('.*\(?consisting of', '', contractor)
                contractor = re.sub(r'[\(\)]', '', contractor)
                contractor = re.sub(' with ', ', ', contractor)



            contractor_list = contractor.split(',')
            contractor_list = [s for s in contractor_list if s != ' ']
            #contractor_list = [s for s in contractor_list if s != 'Sopra Steria Benelux SA with Bull SAS and 3M Belgium BVBA/SPRL']

            for c in range(0, len(contractor_list)-1):
                for j in range(c, len(contractor_list)-1):
                    source.append(contractor_list[c])
                    target.append(contractor_list[j+1])
                    weight.append(value)
                    ids.append(row['id'])

        else:
            source.append(contractor)
            target.append(contractor)
            weight.append(value)
            ids.append(row['id'])

    return ids, source, target, weight

def get_ratio(row, column, contractor):
    """
    Get ratio of matching.
    """
    name = row[column]
    value = fuzz.token_sort_ratio(name, contractor)
    return value

def clean_fuzzy_names(df, column, contractor, threshold):
    """
    Cleans columns.
    """
    df_aux = df[df.apply(lambda x: get_ratio(x, column, contractor), axis=1) > threshold]
    df_aux[column] = contractor
    df.update(df_aux)
    return df

def df_clean_graph_eulisa(df_graph):
    df_graph = clean_fuzzy_names(df_graph, 'source', 'Bull', 60)
    df_graph = clean_fuzzy_names(df_graph, 'target', 'Bull', 60)
    df_graph = clean_fuzzy_names(df_graph, 'source', '3M Belgium BVBA', 60)
    df_graph = clean_fuzzy_names(df_graph, 'target', '3M Belgium BVBA', 60)
    df_graph = clean_fuzzy_names(df_graph, 'source', 'Sopra Steria', 60)
    df_graph = clean_fuzzy_names(df_graph, 'target', 'Sopra Steria', 60)
    df_graph = clean_fuzzy_names(df_graph, 'source', 'Accenture', 70)
    df_graph = clean_fuzzy_names(df_graph, 'target', 'Accenture', 70)
    df_graph = clean_fuzzy_names(df_graph, 'source', 'Atos Belgium', 70)
    df_graph = clean_fuzzy_names(df_graph, 'target', 'Atos Belgium', 70)

    df_graph['source'] = np.where(df_graph['source'] == 'Atos Belgium', 'Atos', df_graph['source'])
    df_graph['source'] = np.where(df_graph['source'] == ' Atos Integration SAS', 'Atos', df_graph['source'])
    df_graph['source'] = np.where(df_graph['source'] == ' and Hewlett Packard Belgium BVBA/SPRL ', 'HP Belgium', df_graph['source'])
    df_graph['source'] = np.where(df_graph['source'] == 'Morpho', 'Idemia', df_graph['source'])
    df_graph['source'] = np.where(df_graph['source'] == ' Idemia Identity & Security SAS', 'Idemia', df_graph['source'])

    df_graph['target'] = np.where(df_graph['target'] == 'Atos Belgium', 'Atos', df_graph['target'])
    df_graph['target'] = np.where(df_graph['target'] == ' Atos Integration SAS', 'Atos', df_graph['target'])
    df_graph['target'] = np.where(df_graph['target'] == ' and Hewlett Packard Belgium BVBA/SPRL ', 'HP Belgium', df_graph['target'])
    df_graph['target'] = np.where(df_graph['target'] == 'Morpho', 'Idemia', df_graph['target'])
    df_graph['target'] = np.where(df_graph['target'] == ' Idemia Identity & Security SAS', 'Idemia', df_graph['target'])
    return df_graph

def df_clean_graph_frontext(df_graph):
    df_graph['source'] = np.where(df_graph['source'] == ' Dea Aviation Ltd', ' Dea Aviation', df_graph['source'])
    df_graph['target'] = np.where(df_graph['target'] == ' Dea Aviation Ltd', ' Dea Aviation', df_graph['target'])
    return df_graph

def scale_edge_weights(df_graph):
    scaler = MinMaxScaler(feature_range=(1, 100))
    df_graph['weight_scale'] = scaler.fit_transform(df_graph['weight'].values.reshape(-1,1))
    df_graph['weight_scale'] = df_graph['weight_scale'].round().astype(int)

    return df_graph
