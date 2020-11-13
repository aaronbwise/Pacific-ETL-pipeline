from pathlib import Path
import pandas as pd
import json
# from etl.engines.aw_analytics import OutputLongFormat

class FijiEngine:

    # Set data directory
    datadir = Path.cwd().joinpath('etl', 'data')

    # Set keep column directory
    var_directory = Path.cwd().joinpath('etl', 'engines', 'variables')

    # Get config info
    # config_path = Path.cwd().joinpath('config.json')  --> Production
    # config_data = json.load(open(config_path))
    config_path = 'C:\\Users\\Aaron\\Google Drive\\01_PERSONAL\\Programming\\Python\\etl_pipeline\\config.json'
    config_data = json.load(open(config_path))


    def __init__(self, list_of_svy_ids):
        self.list_of_svy_ids = list_of_svy_ids


    def run_fiji_engine(self):
        pass

    def generate_output_long(self):
        pass  # --> OutputLongFormat
        


    def clean_combine(self):
        # Get list of Fiji analysed datasets
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

    def add_weights(self):
        pass

    def clean_round(self, df):
        # Categorize survey round according to Digicel dates
        # Convert start to datetime
        df.loc[:, 'start'] = pd.to_datetime(df.start, utc=True)

        


    
    def drop_cols(self, df):
        keep_cols_path = Path.cwd().joinpath('variables/fiji_variables.csv')  # for production change to: self.var_directory.joinpath('fiji_variables.csv')
        keep_cols = pd.read_csv(keep_cols_path, header=None)
        keep_cols = keep_cols[0].tolist()

        df = df.loc[:, df.columns.isin(keep_cols)]

        return df


    def generate_df(self, svy_id):
        fn = svy_id + '_analysed' + '.csv'
        # path = self.datadir.joinpath(fn)  --> Production
        root = Path('C:\\Users\\Aaron\\Google Drive\\01_PERSONAL\\Programming\\Python\\etl_pipeline\\etl\\data') # --> Testing
        path = root.joinpath(fn) # --> Testing

        if path.is_file():
            df = pd.read_csv(path)
        else:
            df = None
        return df


list_of_ids = ['556482', '587333']

obj = FijiEngine(list_of_ids)
test = obj.clean_combine()
print(test.Round.value_counts(dropna=False))


# def fiji_r1_engine(df):
#     # Drop FCS = np.nan
#     df = df[df['FCS_Score'].isna() == False]

#     wt = ['weight_scl']

#     ind_vars = ['Round', 'Division', 'PrefLang', 'Rural', 'HHHSex', 'HH_04', 'HH_Disabled', 'dep_ratio_cat', 'HHHEduYears',\
#                 'HDwellCond', 'HHIncFirst', 'HH_Inc_Reduced', 'HHFarm']

#     cat_vars = ['FCG', 'FCG_min', 'FG_VitA_Cat', 'FG_Protein_Cat', 'FG_HIron_Cat', 'dep_ratio_cat',\
#                 'MDDI_Dep_Cat', 'LCS_SoldAsset_Y', 'LCS_RedFoodWaste_Y', 'LCS_SoldJewelry_Y', 'LCS_RedHlthExp_Y',\
#                 'LCS_RedEduExp_Y', 'LCS_SoldProdAsset_Y', 'LCS_TradeService_Y', 'LCS_SpentSavings_Y', 'LCS_SoldLand_Y',\
#             'LCS_WithdrawEdu_Y', 'LCS_SoldFemAnm_Y', 'LCS_SoldAgProd_Y', 'LCS_Begging_Y', 'LCS_SoldMoreAnm_Y',\
#                 'LCS_Other_Y', 'Worry_DisruptLiv_Y', 'Worry_FoodShort_Y', 'Worry_FoodPrices_Y', 'Worry_MedShort_Y',\
#             'Worry_DisruptMed_Y', 'Worry_DisruptEdu_Y', 'Worry_Illness_Y', 'Worry_NoWork_Y', 'Worry_TravelRestr_Y',\
#                 'Worry_None_Y', 'Worry_Other_Y', 'HH_Inc_Reduced', 'HHSize', 'WitGenViolence', 'HHSchoolSituation',\
#                 'HHChronIll', 'HHENHealthMed', 'HHStockFoodDur', 'Food_SRf1Prev', 'Food_SRf1', 'HHhsBedHung_YN',\
#             'HWaterConstr', 'HHRemittPrev_YN', 'HHRemitt_YN', 'HHBorrow', 'HHBorrowWhy']

#     num_vars = ['FCSStap', 'FCSPulse', 'FCSDairy', 'FCSPr', 'FCSVeg', 'FCSFruit', 'FCSFat', 'FCSSugar', 'FCS_Score', 'FCS_Score_min', 'MDDI_Dep_Sum']

#     dep_vars = cat_vars + num_vars

#     keep_vars = set(wt) | set(ind_vars) | set(dep_vars)

#     df = df[keep_vars]

#     output_df = pd.concat([output_mean_tableau(df, var, ind_vars, wt='weight_scl') for var in dep_vars])

#     return output_df
