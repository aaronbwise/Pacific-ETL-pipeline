import os
import numpy as np
import pandas as pd
import config as C

########## FOR TESTING ONLY - DELETE AFTER USE ###############
def file_path(svy_id):
    fn = svy_id + '_raw' + '.csv'
    path = os.path.join(C.DATA_DIR, fn)
    return path

file_path_list = [file_path(svy) for svy in C.SVY_ID_DICT.values()]

## Function to generate df
def generate_input_df(path):
     df = pd.read_csv(path)
     return df

df = generate_input_df(file_path_list[2])



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