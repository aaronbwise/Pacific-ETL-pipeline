import os
from pathlib import Path
import json
import pandas as pd
# from survey_dict import survey_dict

import etl.cleaning.fiji_r1_cleaning as f1cl
import etl.analysis.fiji_r1_analysis as f1an


class TransformData:
    
    def __init__(self, survey_dict):
        self.survey_dict = survey_dict
        self.fiji_r1_df = self.survey_dict['Fiji_R1'][0]
        self.fiji_r1_mapping_dict = self.survey_dict['Fiji_R1'][1]
        self.fiji_r1_order_list = self.survey_dict['Fiji_R1'][2]

    ## FOR LOOP OR LIST COMPREHENSION???

    def transform(self):
        self.analyse_fiji_data()
        print("Completed!")
        return
    
    def analyse_fiji_data(self):
        # Fiji_R1
        df = self.clean_fiji_data()
        f1an.fiji_r1_analyze_data(df)
        print('Fiji R1 analysis complete!')
        return

    def clean_fiji_data(self):
        # Fiji_R1
        df = f1cl.fiji_r1_clean_data(f1cl.fiji_r1_preprocess_data(self.fiji_r1_df, self.fiji_r1_mapping_dict, self.fiji_r1_order_list))
        print('Fiji R1 cleaning complete!')
        return df
        
    # def clean_samoa_data(self):
    #     # Samoa_R1
    #     self.samoa_r1_df = self.survey_dict['Samoa_R1'][0]
    #     self.samoa_r1_mapping_dict = self.survey_dict['Samoa_R1'][1]
    #     self.samoa_r1_order_list = self.survey_dict['Samoa_R1'][2]

    #     s1cl.samoa_r1_clean_data(s1cl.samoa_r1_preprocess_data(self.samoa_r1_df, self.samoa_r1_mapping_dict, self.samoa_r1_order_list))
    #     print('Samoa R1 cleaning complete!')

#     def clean_tonga_data(self):
#         pass
#         # Round 1