import os
import config as C
import pandas as pd
import make_dict as md

path = os.path.join(C.MAPPING_DIR, 'fiji_r1_api_codebook_mapping.csv')
fiji_r1_mapping_dict = make_dict(path)

path = os.path.join(C.MAPPING_DIR, 'fiji_r1_col_order.csv')
fiji_r1_order_list = pd.read_csv(path, header=None)[0].tolist()

path = os.path.join(C.MAPPING_DIR, 'samoa_r1_api_mapping.csv')
samoa_r1_mapping_dict = make_dict(path)

path = os.path.join(C.MAPPING_DIR, 'samoa_r1_col_order.csv')
samoa_r1_order_list = pd.read_csv(path, header=None)[0].tolist()