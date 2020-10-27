import pandas as pd
from pathlib import Path
import json

# !! Need a way to label/order csv import

# Get round_dict
config_path = Path.cwd().joinpath('config.json')
round_dict = json.load(open(config_path))['round_dict']

# Set directory for cleaned data
datadir = Path.cwd().joinpath('etl', 'data')

# Set directory for mapping and order data
mappingdir = Path.cwd().joinpath('etl', 'cleaning', 'mapping')


### 1. ---- Create list of survey dfs
def generate_survey_df(svy_id):
    """
    Function to create survey dfs based on id
    """
    fn = svy_id + '_raw' + '.csv'
    path = datadir.joinpath(fn)
    
    df = pd.read_csv(path)
    return df

# List comp for list of survey dfs
list_of_dfs = [generate_survey_df(svy_id) for svy_id in round_dict.values()]

### 2. ---- Create list of survey mapping dicts
def generate_mapping_dict(path):
    output = pd.read_csv(path, header=0, index_col=0, squeeze=True).to_dict()
    return output

# List comp for mapping dicts
mapping_filepaths = list(mappingdir.joinpath('api').glob('*.csv'))
list_of_mappings = [generate_mapping_dict(path) for path in mapping_filepaths]

### 3. ---- Create list of survey orders
def generate_order_list(path):
    output = pd.read_csv(path, header=None)[0].tolist()
    return output

# List comp for survey orders
order_filepaths = list(mappingdir.joinpath('order').glob('*.csv'))
list_of_orders = [generate_order_list(path) for path in order_filepaths]

### 4. ---- Zip it all together
svy_values = list(zip(list_of_dfs, list_of_mappings, list_of_orders))

survey_dict = dict(zip(round_dict.keys(), svy_values))