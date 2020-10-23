import os
import numpy as np
import pandas as pd
from variable_mapping import variable_mapping

########## FOR TESTING ONLY - DELETE AFTER USE ###############
path = 'C:\\Users\\Aaron\\Google Drive\\Python_Learning\\etl_pipeline\\data\\600069_raw.csv'
## Function to generate df
def generate_input_df(path):
     df = pd.read_csv(path)
     return df

df = generate_input_df(path)

################################################################

def tonga_r1_preprocess_data(df, col_mapping_dict, col_order_list):
    """Function to preprocess Fiji R1 data"""
    df = df.rename(columns = col_mapping_dict)

    # # Add column for completed_survey
    # df['RESPAge'] = df['RESPAge'].astype('float64')
    # df['completed_svy'] = np.where((df['RESPConsent'] == 'Yes') & (df['RESPAge'] >= 18), 1, 0)

    # # Update column order
    # col_names = frozenset(df.columns.to_list())

    # # Get values in col_order that also appear in col_names
    # final_order = [x for x in col_order_list if x in col_names]

    # df = df[final_order]

    return df

test = tonga_r1_preprocess_data(df,vm.tong)