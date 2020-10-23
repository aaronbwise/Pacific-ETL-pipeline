import os
import numpy as np
import pandas as pd
import config as C

from variable_mapping import samoa_r2_mapping_dict, samoa_r2_order_list

path = os.path.join(C.DATA_DIR, '600087_raw.csv')

## Function to generate df
def generate_input_df(path):
     df = pd.read_csv(path)
     return df

df = generate_input_df(path)