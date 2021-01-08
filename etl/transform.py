import pandas as pd
import json
from pathlib import Path

from etl.clean_inputs import CleaningInputs

# Fiji
import etl.cleaning.fiji_r1_cleaning as f1cl
import etl.analysis.fiji_r1_analysis as f1an

import etl.cleaning.fiji_r2_cleaning as f2cl
import etl.analysis.fiji_r2_analysis as f2an

# Tonga
import etl.cleaning.tonga_r1_cleaning as t1cl
import etl.analysis.tonga_r1_analysis as t1an

import etl.cleaning.tonga_r2_cleaning as t2cl
import etl.analysis.tonga_r2_analysis as t2an

# Samoa
import etl.cleaning.samoa_r1_cleaning as s1cl
import etl.analysis.samoa_r1_analysis as s1an

import etl.cleaning.samoa_r2_cleaning as s2cl
import etl.analysis.samoa_r2_analysis as s2an

# Kiribati
import etl.cleaning.kiribati_r1_cleaning as k1cl
import etl.analysis.kiribati_r1_analysis as k1an

# Vanuatu
import etl.cleaning.vanuatu_r1_cleaning as v1cl
import etl.analysis.vanuatu_r1_analysis as v1an


class TransformData:

    # Set data directory
    datadir = Path.cwd().joinpath('etl', 'data')

    # Set directory for mapping and order data
    mappingdir = Path.cwd().joinpath('etl', 'cleaning', 'mapping')
    
    def __init__(self, round_dict):
        self.round_dict = round_dict
        
        # Get cleaning input information
        cleanObj = CleaningInputs(self.round_dict)
        self.cleaning_inputs_dict = cleanObj.get_cleaning_inputs()


    def transform(self):
        for svy_id in self.round_dict.values(): 
            self.analyse_data(svy_id)
        return
    
    def analyse_data(self, svy_id):

        clean_df = self.clean_data(svy_id)

        if svy_id == '556482':
            analysed_df = f1an.fiji_r1_analyze_data(clean_df, svy_id)
        elif svy_id == '600069':
            analysed_df = t1an.tonga_r1_analyze_data(clean_df, svy_id)
        elif svy_id == '600087':
            analysed_df = s1an.samoa_r1_analyze_data(clean_df, svy_id)
        elif svy_id == '587333':
            analysed_df = f2an.fiji_r2_analyze_data(clean_df, svy_id)
        elif svy_id == '600072':
            analysed_df = t2an.tonga_r2_analyze_data(clean_df, svy_id)        
        elif svy_id == '600088':
            analysed_df = s2an.samoa_r2_analyze_data(clean_df, svy_id)
        elif svy_id == '658085' or svy_id == '677137':
            analysed_df = k1an.kiribati_r1_analyze_data(clean_df, svy_id)
        elif svy_id == '658102' or svy_id == '681686':
            analysed_df = v1an.vanuatu_r1_analyze_data(clean_df, svy_id)
        else:
            analysed_df = None
        return analysed_df

    def clean_data(self, svy_id):

        df = self.generate_survey_df(svy_id)

        if svy_id == '556482': # Fiji_R1
            clean_df = f1cl.fiji_r1_clean_data(f1cl.fiji_r1_preprocess_data(df, self.cleaning_inputs_dict[svy_id][0], self.cleaning_inputs_dict[svy_id][1]), svy_id)
        elif svy_id == '600069': # Tonga_R1
            clean_df = t1cl.tonga_r1_clean_data(t1cl.tonga_r1_preprocess_data(df, self.cleaning_inputs_dict[svy_id][0], self.cleaning_inputs_dict[svy_id][1]), svy_id)
        elif svy_id == '600087': # Samoa_R1
            clean_df = s1cl.samoa_r1_clean_data(s1cl.samoa_r1_preprocess_data(df, self.cleaning_inputs_dict[svy_id][0], self.cleaning_inputs_dict[svy_id][1]), svy_id)
        elif svy_id == '587333': # Fiji_R2+
            clean_df = f2cl.fiji_r2_clean_data(f2cl.fiji_r2_preprocess_data(df, self.cleaning_inputs_dict[svy_id][0], self.cleaning_inputs_dict[svy_id][1]), svy_id)
        elif svy_id == '600072': # Tonga_R2+
            clean_df = t2cl.tonga_r2_clean_data(t2cl.tonga_r2_preprocess_data(df, self.cleaning_inputs_dict[svy_id][0], self.cleaning_inputs_dict[svy_id][1]), svy_id)
        elif svy_id == '600088': # Samoa_R2+
            clean_df = s2cl.samoa_r2_clean_data(s2cl.samoa_r2_preprocess_data(df, self.cleaning_inputs_dict[svy_id][0], self.cleaning_inputs_dict[svy_id][1]), svy_id)
        elif svy_id == '658085' or svy_id == '677137': # Kiribati
            clean_df = k1cl.kiribati_r1_clean_data(k1cl.kiribati_r1_preprocess_data(df, self.cleaning_inputs_dict[svy_id][0], self.cleaning_inputs_dict[svy_id][1]), svy_id)
        elif svy_id == '658102' or svy_id == '681686': # Vanuatu
            clean_df = v1cl.vanuatu_r1_clean_data(v1cl.vanuatu_r1_preprocess_data(df, self.cleaning_inputs_dict[svy_id][0], self.cleaning_inputs_dict[svy_id][1]), svy_id)
        else:
            clean_df = None
        return clean_df

    def generate_survey_df(self, svy_id):
        fn = svy_id + '.csv'
        path = self.datadir.joinpath(fn)

        if path.is_file():
            df = pd.read_csv(path)
        else:
            df = None
        return df   
