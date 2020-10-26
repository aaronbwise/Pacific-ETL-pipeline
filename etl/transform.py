import os
from pathlib import Path
import json
import pandas as pd

from survey_dict import survey_dict

import cleaning.fiji_r1_cleaning as f1cl
# import etl.cleaning.samoa_r1_cleaning as s1cl
# import etl.cleaning.tonga_r1_cleaning as t1cl

class CleanData:
    
    def __init__(self, survey_dict):
        self.survey_dict = survey_dict
        # Fiji components
        # Fiji_R1
        self.fiji_r1_df = self.survey_dict['Fiji_R1'][0]
        self.fiji_r1_mapping_dict = self.survey_dict['Fiji_R1'][1]
        self.fiji_r1_order_list = self.survey_dict['Fiji_R1'][2]

    def clean_fiji_data(self):
        # Round 1
        f1cl.fiji_r1_clean_data(f1cl.fiji_r1_preprocess_data(self.fiji_r1_df, self.fiji_r1_mapping_dict, self.fiji_r1_order_list))
        print('Fiji R1 cleaning complete!')
        # Round 2

#     def clean_samoa_data(self):
#         # Round 1
#         s1cl.samoa_r1_clean_data(s1cl.samoa_r1_preprocess_data(self.samoa_r1_df, self.col_mapping_dict, self.col_order_list))
#         print('Samoa R1 cleaning complete!')

#     def clean_tonga_data(self):
#         pass
#         # Round 1

test = CleanData(survey_dict)

test.clean_fiji_data()