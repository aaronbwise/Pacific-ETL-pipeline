from pathlib import Path
import numpy as np
import pandas as pd

# Set directory for cleaned data
datadir = Path.cwd().joinpath('etl', 'data')

def kiribati_r1_analyze_data(df, svy_id):
    """Function to analyze Kiribati R1 data"""

    ## -- Divison Groups
    adm_gilbert_north = ['Makin', 'Butaritari', 'Marakei', 'Abaiang', 'North Tarawa', 'South Tarawa (Teinainano)']
    adm_gilbert_central = ['Maiana', 'Abemama', 'Kuria', 'Aranuka', 'Nonouti']
    adm_gilbert_south = ['North Tabiteuea', 'South Tabiteuea', 'Beru', 'Nikunau', 'Onotoa', 'Tamana', 'Banaba (Ocean Island)', 'Arorae']
    adm_phoenix_line = ['Line + Phoenix Group']

    conditions = [
        (df['ADM1INName'].isin(adm_gilbert_north)),
        (df['ADM1INName'].isin(adm_gilbert_central)),
        (df['ADM1INName'].isin(adm_gilbert_south)),
        (df['ADM1INName'].isin(adm_phoenix_line))
    ]
    choices = ['Northern Gilberts', 'Central Gilberts', 'Southern Gilberts', 'Line + Phoenix']
    df['Island_Group'] = np.select(conditions, choices)

    ### Create Grouping Variables
    dep_cols = ['HHSize04F', 'HHSize0514F', 'HHSize65aboveF', 'HHSize04M', 'HHSize0514M', 'HHSize65aboveM']
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

    ## -- Household receives remittances
    df['HHRemitt_YN'].fillna('No', inplace=True)

    ## -- Income categories (for CARI)
    df['CARI_inc_regular'] = np.where((df['HHIncFirst'] == 'Salary_Wage_work') | (df['HHIncFirst'] == 'Farming') |\
        (df['HHIncFirst'] == 'Own_business_trade') | (df['HHIncFirst'] == 'Fishing_aquaculture') |\
                (df['HHIncFirst'] == 'Petty_trade_small_business') | (df['HHIncFirst'] == 'Livestock_production'), 'Yes', 'No')

    df['CARI_inc_informal'] = np.where((df['HHIncFirst'] == 'Government_asst_safety_nets') | (df['HHIncFirst'] == 'Daily_casual_labor') |\
        (df['HHIncFirst'] == 'Remittances') | (df['HHIncFirst'] == 'Support_from_family_friends') | (df['HHIncFirst'] == 'Other'), 'Yes', 'No')

    df['CARI_inc_none'] = np.where((df['HHIncFirst'] == 'Unemployed') | (df['HHIncFirst'] == 'Assistance_from_UN_NGO_charity') |\
        (df['HHIncFirst'] == 'savings') | (df['HHIncFirst'] == 'Reliance'), 'Yes', 'No')

    conditions = [
        (df['CARI_inc_regular'] == 'Yes'),
        (df['CARI_inc_informal'] == 'Yes'),
        (df['CARI_inc_none'] == 'Yes')
    ]
    choices = ['Regular', 'Informal', 'None']
    df['CARI_inc_cat'] = np.select(conditions, choices)

    ### Create Outcome Variables
    ## -- Food Consumption Score
    # Recode FCS columns == NaN to 0
    fcs_cols = list(df.loc[:,'FCSStap':'FCSCond'])
    df[fcs_cols] = df[fcs_cols].replace({np.nan: 0})

    # FCS Calculation
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
    vitA_cols = ['FCSDairy', 'FCSMeatO', 'FCSPrEggs', 'FCSVegOrg', 'FCSVegGre', 'FCSFruitOrg']
    df['FG_VitA'] = df[vitA_cols].apply(sum, axis=1)

    protein_cols = ['FCSPulse', 'FCSDairy', 'FCSPrMeat', 'FCSMeatO', 'FCSPrFish', 'FCSPrEggs']
    df['FG_Protein'] = df[protein_cols].apply(sum, axis=1)

    hiron_cols = ['FCSPrMeat', 'FCSMeatO', 'FCSPrFish']
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
    # Recode into single value
    df['LhCSIDomAsset_Y'] = np.where((df['LhCSIDomAsset_YN'] == 'Yes') | (df['LhCSIDomAsset'] == 'Already_depleted'), 1, 0)
    df['LhCSIHealth_Y'] = np.where((df['LhCSIHealth_YN'] == 'Yes') | (df['LhCSIHealth'] == 'Already_depleted'), 1, 0)
    df['LhCSIProdAsset_Y'] = np.where((df['LhCSIProdAsset_YN'] == 'Yes') | (df['LhCSIProdAsset'] == 'Already_depleted'), 1, 0)
    df['LhCSISaving_Y'] = np.where((df['LhCSISaving_YN'] == 'Yes') | (df['LhCSISaving'] == 'Already_depleted'), 1, 0)
    df['LhCSIBorrowCash_Y'] = np.where((df['LhCSIBorrowCash_YN'] == 'Yes') | (df['LhCSIBorrowCash'] == 'Already_depleted'), 1, 0)
    df['LhCSIResAsset_Y'] = np.where((df['LhCSIResAsset_YN'] == 'Yes') | (df['LhCSIResAsset'] == 'Already_depleted'), 1, 0)
    df['LhCSIOutSchool_Y'] = np.where((df['LhCSIOutSchool_YN'] == 'Yes') | (df['LhCSIOutSchool'] == 'Already_depleted'), 1, 0)
    df['LhCSIFemAnimal_Y'] = np.where((df['LhCSIFemAnimal_YN'] == 'Yes') | (df['LhCSIFemAnimal'] == 'Already_depleted'), 1, 0)
    df['LhCSIBegged_Y'] = np.where((df['LhCSIBegged_YN'] == 'Yes') | (df['LhCSIBegged'] == 'Already_depleted'), 1, 0)
    df['LhCSIAnimals_Y'] = np.where((df['LhCSIAnimals_YN'] == 'Yes') | (df['LhCSIAnimals'] == 'Already_depleted'), 1, 0)

    # Set levels
    df['LhCSI_Stress'] = np.where((df['LhCSISaving_Y'] == 1) | (df['LhCSIBorrowCash_Y'] == 1) | (df['LhCSIDomAsset_Y'] == 1) | (df['LhCSIDomAsset_Y'] == 1), 2, 0)
    df['LhCSI_Crisis'] = np.where((df['LhCSIProdAsset_Y'] == 1) | (df['LhCSIHealth_Y'] == 1) | (df['LhCSIOutSchool_Y'] == 1), 3, 0)
    df['LhCSI_Emergency'] = np.where((df['LhCSIBegged_Y'] == 1) | (df['LhCSIFemAnimal_Y'] == 1) | (df['LhCSIResAsset_Y'] == 1), 4, 0)

    # Get max value
    lhcsi_cols = ['LhCSI_Stress', 'LhCSI_Crisis', 'LhCSI_Emergency']
    df['Max_coping_behaviour'] = df[lhcsi_cols].apply(max, axis=1).replace({0:1})

    conditions = [
        (df['Max_coping_behaviour'] == 1),
        (df['Max_coping_behaviour'] == 2),
        (df['Max_coping_behaviour'] == 3),
        (df['Max_coping_behaviour'] == 4)
    ]
    choices = ['None', 'Stress', 'Crisis', 'Emergency']
    df['LhCSI_cat'] = np.select(conditions, choices)

    ## -- remote CARI
    # - Food consumption
    # Value for severe food coping. Do not have rCSI, so will use HHhsBedHung_YN
    conditions = [
        ((df['FCG'] == 'Acceptable') & (df['HHhsBedHung_YN'] == 'No')),
        ((df['FCG'] == 'Acceptable') & (df['HHhsBedHung_YN'] == 'Yes')),
        (df['FCG'] == 'Borderline'),
        (df['FCG'] == 'Poor')
    ]
    choices = [1,2,3,4]
    df['FCS_4pt'] = np.select(conditions, choices)

    # - Economic Vulnerability
    df['HHIncChg_decrease'] = np.where(df['HHIncChg'] == 'reduced', 'Yes', 'No')

    # Build CARI_Inc
    conditions = [
        ((df['CARI_inc_cat'] == 'Regular') & (df['HHIncChg_decrease'] == 'No')),
        (((df['CARI_inc_cat'] == 'Regular') & (df['HHIncChg_decrease'] == 'Yes')) | ((df['CARI_inc_cat'] == 'Informal') & (df['HHIncChg_decrease'] == 'No'))),
        ((df['CARI_inc_cat'] == 'Informal') & (df['HHIncChg_decrease'] == 'Yes')),
        (df['CARI_inc_cat'] == 'None')
    ]
    choices = [1,2,3,4]
    df['CARI_Inc'] = np.select(conditions, choices)

    # - rCARI
    df['rCARI'] = 0.5*df['FCS_4pt'] + 0.25*df['CARI_Inc'] + 0.25*df['Max_coping_behaviour']

    conditions = [
        (   df['rCARI'] < 1.5),
        ((df['rCARI'] >= 1.5) & (df['rCARI'] < 2.5)),
        ((df['rCARI'] >= 2.5) & (df['rCARI'] < 3.5)),
        (df['rCARI'] >= 3.5)
    ]
    choices = ['Food Secure', 'Marginally Food Secure', 'Moderately Food Insecure', 'Severely Food Insecure']
    df['rCARI_cat'] = np.select(conditions, choices)

    food_insec = ['Moderately Food Insecure', 'Severely Food Insecure']
    df['FoodInsecure'] = np.where(df['rCARI_cat'].isin(food_insec), 'Yes', 'No')

    ## -- Most important concerns
    df['Worry_DisruptLiv_Y'] = np.where(df['RESPWorryRsns'] == 'livelihood', 1, 0)
    df['Worry_FoodShort_Y'] = np.where(df['RESPWorryRsns'] == 'food_shortage', 1, 0)
    df['Worry_FoodPrices_Y'] = np.where(df['RESPWorryRsns'] == 'food_prices', 1, 0)
    df['Worry_MedShort_Y'] = np.where(df['RESPWorryRsns'] == 'medicine_shortage', 1, 0)
    df['Worry_DisruptMed_Y'] = np.where(df['RESPWorryRsns'] == 'medical_disruption', 1, 0)
    df['Worry_DisruptEdu_Y'] = np.where(df['RESPWorryRsns'] == 'education', 1, 0)
    df['Worry_Illness_Y'] = np.where(df['RESPWorryRsns'] == 'sick', 1, 0)
    df['Worry_NoWork_Y'] = np.where(df['RESPWorryRsns'] == 'work', 1, 0)
    df['Worry_TravelRestr_Y'] = np.where(df['RESPWorryRsns'] == 'travel', 1, 0)
    df['Worry_None_Y'] = np.where(df['RESPWorryRsns'] == 'none', 1, 0)
    df['Worry_Other_Y'] = np.where(df['RESPWorryRsns'] == 'other', 1, 0)

    ## -- Multi-dimensional Deprivation Index (MDDI)
    # Create variable inputs
    # FOOD_1
    df['MDDI_Food_1_Dep'] = df['FCG'].replace({'Acceptable': 0, 'Borderline': 1, 'Poor': 1}).astype(int)

    # FOOD_3
    df.loc[(df['HHhsBedHung_YN'] == 'Yes'), 'MDDI_Food_3_Dep'] = 1
    df['MDDI_Food_3_Dep'].fillna(0, inplace=True)

    # EDU_1
    df.loc[(df['HHSchoolSituation'] == 'remotely_parents'), 'MDDI_Edu_1_Dep'] = 1
    df['MDDI_Edu_1_Dep'].fillna(0, inplace=True)

    # HEALTH_1
    df.loc[(df['HHIll'] == 'Yes') | (df['HH_Disabled'] == 'Yes'), 'MDDI_Health_1_Dep'] = 1
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
    df.loc[((df['HDwellCond'] == 'rent') | (df['HDwellCond'] == 'free')) &\
        ((df['Worry_DisruptLiv_Y'] == 1) | (df['Worry_NoWork_Y'] == 1)), 'MDDI_Shelter_2_Dep'] = 1
    df['MDDI_Shelter_2_Dep'].fillna(0, inplace=True)

    # WASH
    df['MDDI_WASH_Dep'] = df['HWaterConstrYN'].replace({'Yes': 1, 'No': 0, 'dontknow':0})

    # ENVIRON_2
    df.loc[(df['HHBorrow'] == 'Yes') & ((df['HHBorrowWhy'] == 'food') | (df['HHBorrowWhy'] == 'health')), 'MDDI_Environ_2_Dep'] = 1
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
    df['HH_Inc_Reduced'] = np.where((df['HHIncChg'] == 'reduced') | (df['HHIncChg'] == 'no_income'), 'Yes', 'No')

    ## HH Remit update
    df['HHRemitt_YN'] = np.where(df['HHRemittPrev_YN'] == 'No', "None before COVID", df['HHRemitt_YN'])

    # Write out file
    fn = svy_id + '_analysed' + '.csv'
    out_path = datadir.joinpath(fn)
    print(f'\n Analysed file for {svy_id} being saved to: \n {out_path} \n')
    try:
        df.to_csv(out_path, index=False)
        print(f'{svy_id} data analysed and SAVED!')
    except:
        print(f'{svy_id} data DID NOT SAVE!')

    return df