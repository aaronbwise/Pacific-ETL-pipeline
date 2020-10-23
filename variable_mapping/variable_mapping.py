import os
import config as C
import pandas as pd
from variable_mapping import make_dict


# def generate_mapping_dict(path):

path = os.path.join(C.MAPPING_DIR, './csv/fiji_r1_api_codebook_mapping.csv')
fiji_r1_mapping_dict = make_dict(path)

path = os.path.join(C.MAPPING_DIR, './csv/fiji_r1_col_order.csv')
fiji_r1_order_list = pd.read_csv(path, header=None)[0].tolist()

path = os.path.join(C.MAPPING_DIR, './csv/samoa_r1_api_mapping.csv')
samoa_r1_mapping_dict = make_dict(path)

path = os.path.join(C.MAPPING_DIR, './csv/samoa_r1_col_order.csv')
samoa_r1_order_list = pd.read_csv(path, header=None)[0].tolist()

path = os.path.join(C.MAPPING_DIR, './csv/tonga_r1_api_mapping.csv')
tonga_r1_mapping_dict = make_dict(path)

path = os.path.join(C.MAPPING_DIR, './csv/tonga_r1_col_order.csv')
tonga_r1_order_list = pd.read_csv(path, header=None)[0].tolist()