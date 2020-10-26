import os
from pathlib import Path
import json
import pandas as pd
# import etl.cleaning.variable_mapping as vm

# import etl.cleaning.fiji_r1_cleaning as f1cl
# import etl.cleaning.samoa_r1_cleaning as s1cl
# import etl.cleaning.tonga_r1_cleaning as t1cl

config_path = Path.cwd().parent.joinpath('config.json')
datadir = Path.cwd().joinpath('data')

svy_dict = json.load(open(config_path))['svy_dict']

def file_path(svy_id):
    fn = svy_id + '_raw' + '.csv'
    path = datadir.joinpath(fn)
    return path

file_path_list = [file_path(svy) for svy in svy_dict.values()]

# Function to generate df
def generate_input_df(path):
     df = pd.read_csv(path)
     return df

# Create dict with round, df pairs
list_of_dfs = [generate_input_df(path) for path in file_path_list]
list_of_rounds = list(svy_dict.keys())

df_dict = dict(zip(list_of_rounds, list_of_dfs))


# class CleanData:
    
#     def __init__(self, df_dict, col_mapping_dict, col_order_list):
#         self.df_dict = df_dict
#         self.col_mapping_dict = col_mapping_dict
#         self.col_order_list = col_order_list
#         self.fiji_r1_df = self.df_dict['Fiji_R1']
#         self.samoa_r1_df = self.df_dict['Samoa_R1']
#         self.samoa_r1_df = self.df_dict['Tonga_R1']

#     def clean_fiji_data(self):
#         # Round 1
#         f1cl.fiji_r1_clean_data(f1cl.fiji_r1_preprocess_data(self.fiji_r1_df, self.col_mapping_dict, self.col_order_list))
#         print('Fiji R1 cleaning complete!')
#         # Round 2

#     def clean_samoa_data(self):
#         # Round 1
#         s1cl.samoa_r1_clean_data(s1cl.samoa_r1_preprocess_data(self.samoa_r1_df, self.col_mapping_dict, self.col_order_list))
#         print('Samoa R1 cleaning complete!')

#     def clean_tonga_data(self):
#         pass
#         # Round 1
