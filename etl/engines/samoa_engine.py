import pandas as pd
from etl.engines.aw_analytics import output_mean_tableau

def samoa_r1_engine(df):
    wt = ['weight_scl']

    ind_vars = ['Round', 'ADM1INName', 'PrefLang', 'Rural', 'HHHSex', 'HH_04', 'HH_Disabled', 'dep_ratio_cat', 'HHHEdu',\
                'HDwellCond', 'CARI_inc_cat', 'HH_Inc_Reduced', 'HHFarm', 'HHIll', 'Food_SRf1', 'HHRemitt_YN', 'HHBorrow']

    cat_vars = ['FoodInsecure', 'FCG', 'FG_VitA_Cat', 'FG_Protein_Cat', 'FG_HIron_Cat', 'dep_ratio_cat',\
                'LhCSI_cat', 'Worry_DisruptLiv_Y', 'Worry_FoodShort_Y', 'Worry_FoodPrices_Y', 'Worry_MedShort_Y',\
            'Worry_DisruptMed_Y', 'Worry_DisruptEdu_Y', 'Worry_Illness_Y', 'Worry_NoWork_Y', 'Worry_TravelRestr_Y',\
                'Worry_None_Y', 'Worry_Other_Y',  'rCARI_cat', 'MDDI_Dep_Cat', 'HH_Inc_Reduced', 'WitGenViolence', 'HHSchoolSituation',\
                'HHIll', 'Food_SRf1', 'HHhsBedHung_YN', 'HWaterConstrYN', 'HHRemitt_YN', 'HHBorrow']

    num_vars = ['FCSStap', 'FCSPulse', 'FCSDairy', 'FCSPr', 'FCSVeg', 'FCSFruit', 'FCSFat', 'FCSSugar', 'FCS_Score', 'FCS_Score', 'rCARI', 'MDDI_Dep_Sum']

    dep_vars = cat_vars + num_vars

    keep_vars = set(wt) | set(ind_vars) | set(dep_vars)

    df = df[keep_vars]

    output_df = pd.concat([output_mean_tableau(df, var, ind_vars, wt='weight_scl') for var in dep_vars])

    return output_df