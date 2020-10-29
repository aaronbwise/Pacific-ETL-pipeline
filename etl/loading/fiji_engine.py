import pandas as pd
from etl.loading.aw_analytics import output_mean_tableau

def fiji_r1_engine(df):
    # Drop FCS = np.nan
    df = df[df['FCS_Score'].isna() == False]

    wt = ['weight_scl']

    ind_vars = ['Round', 'Division', 'PrefLang', 'Rural', 'HHHSex', 'HH_04', 'HH_Disabled', 'dep_ratio_cat', 'HHHEduYears',\
                'HDwellCond', 'HHIncFirst', 'HH_Inc_Reduced', 'HHFarm']

    cat_vars = ['FCG', 'FCG_min', 'FG_VitA_Cat', 'FG_Protein_Cat', 'FG_HIron_Cat', 'dep_ratio_cat',\
                'MDDI_Dep_Cat', 'LCS_SoldAsset_Y', 'LCS_RedFoodWaste_Y', 'LCS_SoldJewelry_Y', 'LCS_RedHlthExp_Y',\
                'LCS_RedEduExp_Y', 'LCS_SoldProdAsset_Y', 'LCS_TradeService_Y', 'LCS_SpentSavings_Y', 'LCS_SoldLand_Y',\
            'LCS_WithdrawEdu_Y', 'LCS_SoldFemAnm_Y', 'LCS_SoldAgProd_Y', 'LCS_Begging_Y', 'LCS_SoldMoreAnm_Y',\
                'LCS_Other_Y', 'Worry_DisruptLiv_Y', 'Worry_FoodShort_Y', 'Worry_FoodPrices_Y', 'Worry_MedShort_Y',\
            'Worry_DisruptMed_Y', 'Worry_DisruptEdu_Y', 'Worry_Illness_Y', 'Worry_NoWork_Y', 'Worry_TravelRestr_Y',\
                'Worry_None_Y', 'Worry_Other_Y', 'HH_Inc_Reduced', 'HHSize', 'WitGenViolence', 'HHSchoolSituation',\
                'HHChronIll', 'HHENHealthMed', 'HHStockFoodDur', 'Food_SRf1Prev', 'Food_SRf1', 'HHhsBedHung_YN',\
            'HWaterConstr', 'HHRemittPrev_YN', 'HHRemitt_YN', 'HHBorrow', 'HHBorrowWhy']

    num_vars = ['FCSStap', 'FCSPulse', 'FCSDairy', 'FCSPr', 'FCSVeg', 'FCSFruit', 'FCSFat', 'FCSSugar', 'FCS_Score', 'FCS_Score_min', 'MDDI_Dep_Sum']

    dep_vars = cat_vars + num_vars

    keep_vars = set(wt) | set(ind_vars) | set(dep_vars)

    df = df[keep_vars]

    output_df = pd.concat([output_mean_tableau(df, var, ind_vars, wt='weight_scl') for var in dep_vars])

    return output_df
