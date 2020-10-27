from pathlib import Path
import numpy as np
import pandas as pd

# Set directory for cleaned data
datadir = Path.cwd().joinpath('etl', 'data')
weighstdir = Path.cwd().joinpath('etl', 'analysis', 'weights')

def fiji_r1_analyze_data(df):
    """Function to analyze Fiji R1 data"""
    # Load Cleaned dataset
    df = pd.read_csv(datadir.joinpath('fiji_R1_cleaned.csv'))

    # Load and merge HH weight file
    weights = pd.read_csv(weighstdir.joinpath('fiji_r1_weights.csv'), usecols=['ADM2Name', 'weight_scl'])
    df = pd.merge(df, weights, on='ADM2Name', how='left')  # -> improve

    ### Create Grouping Variables
    ## -- Divison Groups
    df.loc[(df['ADM2Name'] == 'Naitasiri') | (df['ADM2Name'] == 'Namosi') | (df['ADM2Name'] == 'Rewa') |\
        (df['ADM2Name'] == 'Serua') | (df['ADM2Name'] == 'Tailevu') | (df['ADM2Name'] == 'Kadavu') |\
                    (df['ADM2Name'] == 'Lau') | (df['ADM2Name'] == 'Lomaiviti') | (df['ADM2Name'] == 'Rotuma'), 'Division'] = 'Central/Eastern'

    df.loc[(df['ADM2Name'] == 'Bua') | (df['ADM2Name'] == 'Cakaudrove') |\
        (df['ADM2Name'] == 'Macuata'), 'Division'] = 'Northern'

    df.loc[(df['ADM2Name'] == 'Ba') | (df['ADM2Name'] == 'Nadroga-Navosa') | (df['ADM2Name'] == 'Ra'), 'Division'] = 'Western'

    ## -- Depenency Ratio Category
    dep_cols = ['HHSize04F', 'HHSize0414F', 'HHSize65aboveF', 'HHSize04M', 'HHSize0414M', 'HHSize65aboveM']
    df[dep_cols] = df[dep_cols].replace({np.nan: 0})
    df['dep_ratio'] = (df[dep_cols].apply(sum, axis=1)) / df['HHSize']
    df.loc[df['dep_ratio'] > 1] = np.nan
    # Cut into quartiles
    bin_labels_3 = ['Low', 'Moderate', 'High']
    df['dep_ratio_cat'] = pd.qcut(df['dep_ratio'],
                                q=3, labels=bin_labels_3)

    ## -- Household has Children < 5
    df.loc[(df['HHSize04F'] > 0) | (df['HHSize04M'] > 0), 'HH_04'] = 'Yes'
    df['HH_04'].fillna('No', inplace=True)

    ## -- Household has Disabled Member
    df.loc[(df['HHDisabledNb'] > 0), 'HH_Disabled'] = 'Yes'
    df['HH_Disabled'].fillna('No', inplace=True)

    ### Create Outcome Variables
    ## -- Food Consumption Score
    # Discrepancy between, e.g., FCSPr and FCSPrFish, whereby latter is greater than former.
    # Therefore create separate aggregate variables for each to determine if difference is meaningful.

    # Rename existing FCS columns 
    fcs_rename_dict = {'FCSPr': 'FCSPr_min', 'FCSVeg': 'FCSVeg_min', 'FCSFruit': 'FCSFruit_min'}
    df = df.rename(columns=fcs_rename_dict)

    # PROTEIN
    pr_cols = ['FCSPr_min', 'FCSPrMeat', 'FCSPrMeatO', 'FCSPrFish', 'FCSPrEgg']
    df['FCSPr'] = df[pr_cols].apply(max, axis=1)

    # VEGETABLES
    veg_cols = ['FCSVeg_min', 'FCSVegOrg', 'FCSVegGre']
    df['FCSVeg'] = df[veg_cols].apply(max, axis=1)

    # FRUIT
    fru_cols = ['FCSFruit_min', 'FCSFruitOrg']
    df['FCSFruit'] = df[fru_cols].apply(max, axis=1)

    # FCS Calculation
    df['FCS_Score_min'] = df['FCSStap']*2 + df['FCSPulse']*3 + df['FCSVeg_min']*1 + df['FCSFruit_min']*1\
    + df['FCSPr_min']*4 + df['FCSDairy']*4 + df["FCSSugar"]*0.5 + df['FCSFat']*0.5

    df.loc[df['FCS_Score_min'] == 0, 'FCS_Score_min'] = np.nan

    conditions = [
        (df['FCS_Score_min'] <= 21),
        (df['FCS_Score_min'] > 21) & (df['FCS_Score_min'] <= 35),
        (df['FCS_Score_min'] > 35)]
    choices = ['Poor', 'Borderline', 'Acceptable']
    df['FCG_min'] = np.select(conditions, choices)

    df.loc[df['FCG_min'] == 0, 'FCG_min'] = np.nan

    # FCS_max Calculation
    df['FCS_Score'] = df['FCSStap']*2 + df['FCSPulse']*3 + df['FCSVeg']*1 + df['FCSFruit']*1\
    + df['FCSPr']*4 + df['FCSDairy']*4 + df["FCSSugar"]*0.5 + df['FCSFat']*0.5

    df.loc[df['FCS_Score'] == 0, 'FCS_Score'] = np.nan

    conditions = [
        (df['FCS_Score'] <= 21),
        (df['FCS_Score'] > 21) & (df['FCS_Score'] <= 35),
        (df['FCS_Score'] > 35)]
    choices = ['Poor', 'Borderline', 'Acceptable']
    df['FCG'] = np.select(conditions, choices)

    df.loc[df['FCG'] == 0, 'FCG'] = np.nan

    ## -- Food Consumption Score - Nutrition Quality Analysis (FCS-N)
    vitA_cols = ['FCSDairy', 'FCSPrMeatO', 'FCSPrEgg', 'FCSVegOrg', 'FCSVegGre', 'FCSFruitOrg']
    df['FG_VitA'] = df[vitA_cols].apply(sum, axis=1)

    protein_cols = ['FCSPulse', 'FCSDairy', 'FCSPrMeat', 'FCSPrMeatO', 'FCSPrFish', 'FCSPrEgg']
    df['FG_Protein'] = df[protein_cols].apply(sum, axis=1)

    hiron_cols = ['FCSPrMeat', 'FCSPrMeatO', 'FCSPrFish']
    df['FG_HIron'] = df[hiron_cols].apply(sum, axis=1)

    # FCS-N Categories
    conditions = [
        (df['FG_VitA'] == 0),
        (df['FG_VitA'] > 0) & (df['FG_VitA'] <= 6),
        (df['FG_VitA'] > 6)]
    choices = ['0 days', '1-6 days', '7 days']
    df['FG_VitA_Cat'] = np.select(conditions, choices)

    conditions = [
        (df['FG_Protein'] == 0),
        (df['FG_Protein'] > 0) & (df['FG_Protein'] <= 6),
        (df['FG_Protein'] > 6)]
    choices = ['0 days', '1-6 days', '7 days']
    df['FG_Protein_Cat'] = np.select(conditions, choices)

    conditions = [
        (df['FG_HIron'] == 0),
        (df['FG_HIron'] > 0) & (df['FG_HIron'] <= 6),
        (df['FG_HIron'] > 6)]
    choices = ['0 days', '1-6 days', '7 days']
    df['FG_HIron_Cat'] = np.select(conditions, choices)

    ## -- Livelihood Coping Strategies
    df['LCS_SoldAsset_Y'] = df.LhCSIMoreIncome.str.contains('sold__traded_h')
    df['LCS_RedFoodWaste_Y'] = df.LhCSIMoreIncome.str.contains('reduced_food_w')
    df['LCS_SoldJewelry_Y'] = df.LhCSIMoreIncome.str.contains('sold_smaller_h')
    df['LCS_RedHlthExp_Y'] = df.LhCSIMoreIncome.str.contains('reduced_non_fo')
    df['LCS_RedEduExp_Y'] = df.LhCSIMoreIncome.str.contains('reduced_non_fo_1')
    df['LCS_SoldProdAsset_Y'] = df.LhCSIMoreIncome.str.contains('sold_productiv')
    df['LCS_TradeService_Y'] = df.LhCSIMoreIncome.str.contains('trading_a_serv')
    df['LCS_SpentSavings_Y'] = df.LhCSIMoreIncome.str.contains('spent_savings')
    df['LCS_SoldLand_Y'] = df.LhCSIMoreIncome.str.contains('sold_house_or_')
    df['LCS_WithdrawEdu_Y'] = df.LhCSIMoreIncome.str.contains('withdrew_child')
    df['LCS_SoldFemAnm_Y'] = df.LhCSIMoreIncome.str.contains('sold_traded_la')
    df['LCS_SoldAgProd_Y'] = df.LhCSIMoreIncome.str.contains('traded_anmils')
    df['LCS_Begging_Y'] = df.LhCSIMoreIncome.str.contains('pleaded_for_money__food__or_ot')
    df['LCS_SoldMoreAnm_Y'] = df.LhCSIMoreIncome.str.contains('sold_more_anim')
    df['LCS_Other_Y'] = df.LhCSIMoreIncome.str.contains('other_activiti')

    ## -- Most important concerns
    df['Worry_DisruptLiv_Y'] = df.RESPWorryRsns.str.contains('disruption_to_')
    df['Worry_FoodShort_Y'] = df.RESPWorryRsns.str.contains('shortage_of_fo')
    df['Worry_FoodPrices_Y'] = df.RESPWorryRsns.str.contains('increase_in_fo')
    df['Worry_MedShort_Y'] = df.RESPWorryRsns.str.contains('shortage_of_me')
    df['Worry_DisruptMed_Y'] = df.RESPWorryRsns.str.contains('disruption_of_')
    df['Worry_DisruptEdu_Y'] = df.RESPWorryRsns.str.contains('disruption_of__1')
    df['Worry_Illness_Y'] = df.RESPWorryRsns.str.contains('getting_sick')
    df['Worry_NoWork_Y'] = df.RESPWorryRsns.str.contains('lack_of_work')
    df['Worry_TravelRestr_Y'] = df.RESPWorryRsns.str.contains('travel_restric')
    df['Worry_None_Y'] = df.RESPWorryRsns.str.contains('no_concerns')
    df['Worry_Other_Y'] = df.RESPWorryRsns.str.contains('other')

    ## -- Multi-dimensional Deprivation Index (MDDI)
    # Create variable inputs
    # FOOD_1
    df['MDDI_Food_1_Dep'] = df['FCG'].replace({'Acceptable': 0, 'Borderline': 1, 'Poor': 1}).astype(int)

    # FOOD_2
    df.loc[(df['Food_SRf1'] == 'Gift from family_friends') |\
        (df['Food_SRf1'] == 'Food assistance_Govt'), 'MDDI_Food_2_Dep'] = 1
    df['MDDI_Food_2_Dep'].fillna(0, inplace=True)

    # FOOD_3
    df.loc[(df['HHhsBedHung_YN'] == 'Yes'), 'MDDI_Food_3_Dep'] = 1
    df['MDDI_Food_3_Dep'].fillna(0, inplace=True)

    # EDU_1
    df.loc[(df['HHSchoolSituation'] == 'Learning remotely_parent resources'), 'MDDI_Edu_1_Dep'] = 1
    df['MDDI_Edu_1_Dep'].fillna(0, inplace=True)

    # EDU_2
    df.loc[(df['HHHEduYears'] == 'Primary'), 'MDDI_Edu_2_Dep'] = 1
    df['MDDI_Edu_2_Dep'].fillna(0, inplace=True)

    # HEALTH_1
    df.loc[(df['HHChronIll'] == 'Yes') | (df['HH_Disabled'] == 'Yes'), 'MDDI_Health_1_Dep'] = 1
    df['MDDI_Health_1_Dep'].fillna(0, inplace=True)

    # SHELTER_1
    df['persroom'] = ((df['HHSize']) / df['Hroom'])
    df['persroom'].fillna(0, inplace=True)
    conditions = [
    (df['persroom'] <= 3),
        (df['persroom'] > 3)]
    choices = [0, 1]
    df['MDDI_Shelter_1_Dep'] = np.select(conditions, choices)

    # SHELTER_2
    df.loc[((df['HDwellCond'] == 'Rent') | (df['HDwellCond'] == 'Do not own but live for free')) &\
        ((df['Worry_DisruptLiv_Y'] == True) | (df['Worry_NoWork_Y'] == True)), 'MDDI_Shelter_2_Dep'] = 1
    df['MDDI_Shelter_2_Dep'].fillna(0, inplace=True)

    # WASH
    df['MDDI_WASH_Dep'] = df['HWaterConstr'].replace({'Yes': 1, 'No': 0})

    # ENVIRON_2
    df.loc[(df['HHBorrow'] == 'Yes') & ((df['HHBorrowWhy'] == 'Food') | (df['HHBorrowWhy'] == 'Healthcare')), 'MDDI_Environ_2_Dep'] = 1
    df['MDDI_Environ_2_Dep'].fillna(0, inplace=True)

    # ** MDDI Construction **
    df['MDDI_Dep'] = (1/6*1/2*100)*df['MDDI_Food_1_Dep'] + (1/6*1/2*100)*df['MDDI_Food_3_Dep'] +\
    (1/6*100)*df['MDDI_Edu_1_Dep'] + (1/6*100)*df['MDDI_Health_1_Dep'] + (1/6*1/2*100)*df['MDDI_Shelter_1_Dep'] +\
    (1/6*1/2*100)*df['MDDI_Shelter_2_Dep'] + (1/6*100)*df['MDDI_WASH_Dep'] + (1/6*100)*df['MDDI_Environ_2_Dep']

    df['MDDI_Food_Dep'] = (1/6*1/2*100)*df['MDDI_Food_1_Dep'] + (1/6*1/2*100)*df['MDDI_Food_3_Dep']
    df['MDDI_Shelter_Dep'] = (1/6*1/2*100)*df['MDDI_Shelter_1_Dep'] + (1/6*1/2*100)*df['MDDI_Shelter_2_Dep']

    df.loc[(df['MDDI_Dep'] >= 30), 'MDDI_Dep_Cat'] = 'Yes'
    df['MDDI_Dep_Cat'].fillna('No', inplace=True)

    df['MDDI_Dep_Sum'] = df['MDDI_Food_1_Dep'] + df['MDDI_Food_3_Dep'] +\
    df['MDDI_Edu_1_Dep'] + df['MDDI_Health_1_Dep'] + df['MDDI_Shelter_1_Dep'] +\
    df['MDDI_Shelter_2_Dep'] + df['MDDI_WASH_Dep'] + df['MDDI_Environ_2_Dep']

    # HH Income reduced
    df['HH_Inc_Reduced'] = np.where((df['HHIncChg'] == 'Reduced') | (df['HHIncChg'] == 'No income'), 'Yes', 'No')

    ## HH Remit update
    df['HHRemitt_YN'] = df['HHRemitt_YN'].replace({np.nan: "None before COVID"})

    # Write out file
    fn = 'fiji' + '_R1' + '_analysed' + '.csv'
    out_path = datadir.joinpath(fn)
    print(f'Analysed file being saved to: {out_path}')
    try:
        df.to_csv(out_path, index=False)
        print('Fiji R1 data analysed and SAVED!')
    except:
        print('Fiji R1 data DID NOT SAVE!')

    return df