import pandas as pd
import json
from pathlib import Path
from etl.clean_inputs import CleaningInputs

import etl.cleaning.fiji_r1_cleaning as f1cl
import etl.analysis.fiji_r1_analysis as f1an

# import etl.cleaning.tonga_r1_cleaning as t1cl
# import etl.analysis.tonga_r1_analysis as t1an

# import etl.cleaning.samoa_r1_cleaning as s1cl
# import etl.analysis.samoa_r1_analysis as s1an


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
        self.analyse_data()
        return
    
    def analyse_data(self):

        for svy_id in self.round_dict.values():
            clean_df = self.clean_data(svy_id)

            if svy_id == '556482':
                analysed_df = f1an.fiji_r1_analyze_data(clean_df, svy_id)
            # elif svy_id == '600069':
            #     analysed_df = t1an.fiji_r1_analyze_data(clean_df)
            # elif svy_id == '600087':
            #     analysed_df = s1an.fiji_r1_analyze_data(clean_df)
            else:
                analysed_df = None
            print(f'{svy_id} analysis complete!')
            return analysed_df

    def clean_data(self, svy_id):
        df = self.generate_survey_df(svy_id)

        if svy_id == '556482': # Fiji_R1
            clean_df = f1cl.fiji_r1_clean_data(f1cl.fiji_r1_preprocess_data(df, self.cleaning_inputs_dict[svy_id][0], self.cleaning_inputs_dict[svy_id][1]), svy_id)
        # elif svy_id == '600069': # Tonga_R1
        #     clean_df = t1cl.fiji_r1_clean_data(t1cl.fiji_r1_preprocess_data(df, self.cleaning_inputs_dict[svy_id][0], self.cleaning_inputs_dict[svy_id][1]))
        # elif svy_id == '600087': # Samoa_R1
        #     clean_df = s1cl.fiji_r1_clean_data(s1cl.fiji_r1_preprocess_data(df, self.cleaning_inputs_dict[svy_id][0], self.cleaning_inputs_dict[svy_id][1]))
        else:
            clean_df = None
        print(f'{svy_id} cleaning complete!')
        return clean_df

    def generate_survey_df(self, svy_id):
        fn = svy_id + '.csv'
        path = self.datadir.joinpath(fn)

        if path.is_file():
            df = pd.read_csv(path)
        else:
            df = None
        return df   
