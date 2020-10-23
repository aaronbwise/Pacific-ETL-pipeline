import os
import pandas as pd
import config as C
import etl.extract as extract
import etl.transform as transform 
import variable_mapping as vm

def file_path(svy_id):
    fn = svy_id + '_raw' + '.csv'
    path = os.path.join(C.DATA_DIR, fn)
    return path

file_path_list = [file_path(svy) for svy in C.SVY_ID_DICT.values()]

## Function to generate df
def generate_input_df(path):
     df = pd.read_csv(path)
     return df

list_of_dfs = [generate_input_df(path) for path in file_path_list]
list_of_rounds = list(C.SVY_ID_DICT.keys())

# Create dict with round, df pairs
df_dict = dict(zip(list_of_rounds, list_of_dfs))


if __name__ == "__main__":
    # [extract.format_as_dataframe(extract.fetch_data(C.URL, C.USER, C.PASSWORD, svy), svy) for svy in C.SVY_ID_DICT.values()]

    samoa_r1 = transform.CleanData(df_dict, vm.samoa_r1_mapping_dict, vm.samoa_r1_order_list)
    samoa_r1.clean_samoa_data()