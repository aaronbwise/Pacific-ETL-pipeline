from pathlib import Path
import numpy as np
import pandas as pd
import json
from etl.engines.aw_analytics import OutputLongFormat

class TongaEngine:

    # Set data directory
    datadir = Path.cwd().joinpath('etl', 'data')

    # Set keep column directory
    vardir = Path.cwd().joinpath('etl', 'engines', 'variables')

    # Set population directory
    popdir = Path.cwd().joinpath('etl', 'engines', 'populations')

    # Get config info
    config_path = Path.cwd().joinpath('config.json')  # --> Production
    config_data = json.load(open(config_path))


    def __init__(self, list_of_svy_ids):
        self.list_of_svy_ids = list_of_svy_ids


    def run_tonga_engine(self):
        tonga_tableau = self.generate_output_long()
        return tonga_tableau

    def generate_output_long(self):
        combined_df = self.clean_combine()

        print(f'Value Counts: \n {combined_df.Round.value_counts(dropna=False)}')
        
        wt = ['weight_scl']

        ind_vars = ['Total', 'ADM1INName', 'PrefLang', 'Rural', 'HHHSex', 'HH_04', 'HH_Disabled', 'dep_ratio_cat', 'HHHEdu',\
                    'HDwellCond', 'CARI_inc_cat', 'HH_Inc_Reduced', 'HHFarm', 'HHIll', 'Food_SRf1', 'HHRemitt_YN', 'HHBorrow']

        cat_vars = ['FoodInsecure', 'FCG', 'FG_VitA_Cat', 'FG_Protein_Cat', 'FG_HIron_Cat', 'dep_ratio_cat',\
                    'LhCSI_cat', 'Worry_DisruptLiv_Y', 'Worry_FoodShort_Y', 'Worry_FoodPrices_Y', 'Worry_MedShort_Y',\
                'Worry_DisruptMed_Y', 'Worry_DisruptEdu_Y', 'Worry_Illness_Y', 'Worry_NoWork_Y', 'Worry_TravelRestr_Y',\
                    'Worry_None_Y', 'Worry_Other_Y',  'rCARI_cat', 'MDDI_Dep_Cat', 'HH_Inc_Reduced', 'WitGenViolence', 'HHSchoolSituation',\
                    'HHIll', 'Food_SRf1', 'HHhsBedHung_YN', 'HWaterConstrYN', 'HHRemitt_YN', 'HHBorrow']

        num_vars = ['FCSStap', 'FCSPulse', 'FCSDairy', 'FCSPr', 'FCSVeg', 'FCSFruit', 'FCSFat', 'FCSSugar', 'FCS_Score', 'FCS_Score', 'rCARI', 'MDDI_Dep_Sum']

        dep_vars = cat_vars + num_vars

        outputObj = OutputLongFormat(combined_df, dep_vars, ind_vars, wt, split_col='Round')
        output = outputObj.create_output()

        return output
        

    def clean_combine(self):
        # Get list of Tonga analysed datasets
        list_of_dfs = [self.generate_df(svy_id) for svy_id in self.list_of_svy_ids]

        # Drop unnecessary columns
        list_of_small_dfs = [self.drop_cols(df) for df in list_of_dfs]
        
        # Combine (concat) R1 and R2+ dfs
        combined_df = pd.concat([list_of_small_dfs[i] for i in range(len(list_of_small_dfs))], axis=0)

        # Clean round info
        combined_df = self.clean_round(combined_df)

        # Add survey weights
        combined_df = self.add_weights(combined_df)

        return combined_df

    def add_weights(self, df):
        # Build weights on the fly
        temp = pd.DataFrame({'count': df.groupby(['Round', 'ADM1INName']).size()}).reset_index()

        # Merge in pop data
        path = self.popdir.joinpath('tonga_pop.csv')  # --> -------------  Production  -------------
        country_pop = pd.read_csv(path, dtype={'Pop': 'int32'})

        # Add pop to dataset
        temp = pd.merge(temp, country_pop, on='ADM1INName', how='left')

        # Generate weight
        count_sum = temp.groupby('Round')['count'].sum().to_frame(name = 'count_sum')
        pop_sum = temp.groupby('Round')['Pop'].sum().to_frame(name = 'Pop_sum')

        sum_df = count_sum.join(pop_sum).reset_index()
        temp = pd.merge(temp, sum_df, left_on='Round', right_on='Round', how='right')
        temp['weight_scl'] = (temp['Pop'] / temp['Pop_sum']) / (temp['count'] / temp['count_sum'])
        # Create Round+ADM column
        temp['Round_ADM'] = temp['Round'] + '_' + temp['ADM1INName']

        # Add in main dataset
        df['Round_ADM'] = df['Round'] + '_' + df['ADM1INName']
        keep_cols = ['Round_ADM', 'weight_scl']
        temp = temp[keep_cols]

        df = pd.merge(df, temp, on='Round_ADM', how='left')

        # Add total column
        df.insert(0, 'Total', 'Total')

        return df

    def clean_round(self, df):
        """
        Dynamically categorize survey round according to Digicel dates
        """
        # Convert start variable to datetime
        df.loc[:, 'start'] = pd.to_datetime(df.start, utc=True)

        # Get survey dates
        tonga_dates = self.config_data['dates_dict']['Tonga']  # --> Refactor?? Generalise?

        # Generate list of round labels
        round_label = ['R' + str(i+1) for i in range(len(tonga_dates))]

        # Convert dates to datetime
        start_dates = [pd.to_datetime(tonga_dates[label]['start']).tz_localize(tz='UTC') for label in round_label]
        end_dates = [pd.to_datetime(tonga_dates[label]['end']).tz_localize(tz='UTC') for label in round_label]
        dates = list(zip(start_dates, end_dates))

        # Dynamically generate filter categories
        conditions = [((df['start'] >= dates[i][0]) & (df['start'] < dates[i][1])) for i in range(len(round_label))]

        df['Round'] = np.select(conditions, round_label)

        # Drop rows without valid round label
        df = df[df['Round'] != '0']

        return df
   

    def drop_cols(self, df):
        keep_cols_path = self.vardir.joinpath('tonga_variables.csv')   # --> -------------  Production  -------------
        keep_cols = pd.read_csv(keep_cols_path, header=None)
        keep_cols = keep_cols[0].tolist()

        df = df.loc[:, df.columns.isin(keep_cols)]

        return df


    def generate_df(self, svy_id):
        fn = svy_id + '_analysed' + '.csv'
        path = self.datadir.joinpath(fn)  # --> Production

        if path.is_file():
            df = pd.read_csv(path)
        else:
            df = None
        return df