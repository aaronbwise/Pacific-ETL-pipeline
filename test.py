import pandas as pd
from pathlib import Path
import json
import etl.loading.fiji_engine as fe

# Get config data
config_data = json.load(open('config.json'))

# Survey Round and ID dictionary
round_dict = config_data['round_dict']

# Set data directory
datadir = Path.cwd().joinpath('etl', 'data')

def generate_output_tableau(df):
    """
    Generate dictionary with svy_id and output for Tableau (ie long form)
    """
    tableau_output_dict = {}
    for svy_id in round_dict.values():
        
        # Need to add column/way to determine if data is analysed

        if svy_id == '556482':
            output = fe.fiji_r1_engine(df)
            tableau_output_dict[svy_id] = output
        else:
            output = None
            tableau_output_dict[svy_id] = output
        
        return tableau_output_dict

def generate_df(svy_id):
    fn = svy_id + '_analysed' + '.csv'
    path = datadir.joinpath(fn)

    if path.is_file():
        df = pd.read_csv(path)
    else:
        df = None
    return df

test = generate_df(round_dict['Fiji_R1'])

test1 = generate_output_tableau(test)
print(type(test1.values()))