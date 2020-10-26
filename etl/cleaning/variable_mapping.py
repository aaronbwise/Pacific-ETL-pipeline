import os
import pandas as pd

def make_dict(path):
    d = pd.read_csv(path, header=0, index_col=0, squeeze=True).to_dict()
    return d

mappingdir = os.path.join(os.path.split(os.path.realpath(__file__))[0], 'mapping')

# mapping_dict_list = []  -->> Need to refactor the code below - make it DRY!!

path = os.path.join(mappingdir, 'fiji_r1_api_codebook_mapping.csv')
fiji_r1_mapping_dict = make_dict(path)

path = os.path.join(mappingdir, 'fiji_r1_col_order.csv')
fiji_r1_order_list = pd.read_csv(path, header=None)[0].tolist()

path = os.path.join(mappingdir, 'samoa_r1_api_mapping.csv')
samoa_r1_mapping_dict = make_dict(path)

path = os.path.join(mappingdir, 'samoa_r1_col_order.csv')
samoa_r1_order_list = pd.read_csv(path, header=None)[0].tolist()

path = os.path.join(mappingdir, 'tonga_r1_api_mapping.csv')
tonga_r1_mapping_dict = make_dict(path)

path = os.path.join(mappingdir, 'tonga_r1_col_order.csv')
tonga_r1_order_list = pd.read_csv(path, header=None)[0].tolist()