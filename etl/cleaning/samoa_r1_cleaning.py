import os
import numpy as np
import pandas as pd
import config as C

def samoa_r1_preprocess_data(df, col_mapping_dict, col_order_list):
    """Function to preprocess Fiji R1 data"""
    df = df.rename(columns = col_mapping_dict)

    # Add column for completed_survey
    df['RESPAge'] = df['RESPAge'].astype('float64')
    df['completed_svy'] = np.where((df['RESPConsent'] == 'Yes') & (df['RESPAge'] >= 18), 1, 0)

    # Update column order
    col_names = frozenset(df.columns.to_list())

    # Get values in col_order that also appear in col_names
    final_order = [x for x in col_order_list if x in col_names]

    df = df[final_order]

    return df

def samoa_r1_clean_data(df):
    # Drop incomplete surveys
    df = df[df['completed_svy'] == 1]

    # Convert dates/times to datetime format  
    df.loc[:,'start'] = pd.to_datetime(df.start)
    df.loc[:,'end'] = pd.to_datetime(df.end)

    # Recode RESPAge == 99 as NaN
    df.loc[:,'RESPAge'] = df['RESPAge'].replace({99: np.nan})

    # Convert number variables to numeric format
    cols = ['RESPAge', 'HHSize', 'HHSizeM', 'HHSizeF', 'HHSizeOth', 'HHSize04F', 'HHBreastfedF',\
        'HHSize0514F', 'HHSize1524F', 'HHSize2554F', 'HHSize5564F', 'HHSize65aboveF', 'HHSize04M',\
        'HHBreastfedM', 'HHSize0514M', 'HHSize1524M', 'HHSize2554M', 'HHSize5564M', 'HHSize65aboveM',\
        'HHSize0514Oth', 'HHSize1524Oth', 'HHSize2554Oth', 'HHSize5564Oth', 'HHSize65aboveOth',\
        'HHDisabledNb', 'FCSStap', 'FCSPulse', 'FCSDairy', 'FCSPr', 'FCSPrMeat', 'FCSMeatO', 'FCSPrFish',\
        'FCSPrEggs', 'FCSVeg', 'FCSVegOrg', 'FCSVegGre', 'FCSFruit', 'FCSFruitOrg', 'FCSFat', 'FCSSugar',\
        'FCSCond', 'HHIllNb', 'Hroom', 'HHDebtPaidWhen', 'HHSizeCalc', 'HHSizeFCalc', 'HHSizeMCalc']

    df[cols] = df[cols].astype('float64')

    # Clean values and labels
    # Impute HHHSex from RESPSex if RESP == HHH
    df.loc[:, 'HHHSex'] = np.where(df['RESPRelationHHH'] == 'Yes', df['RESPSex'], df['HHHSex'])

    # HHInc
    df.loc[:, 'HHIncChg'] = df['HHIncChg'].replace({np.nan:'no_income'})

    # Rooms = 0 to NaN
    df.loc[: 'Hroom'] = df.replace({0:np.nan})

    # yesnodontknow
    yndk_cols = ['HHygieneYN', 'HWaterConstrYN']
    df.loc[:,yndk_cols] = df.replace({'no': 'No', 'yes': 'Yes', 'dontknow':'DK'})
    
    # Write out file
    fn = 'samoa' + '_R1' + '_cleaned' + '.csv'
    out_path = os.path.join(C.DATA_DIR, fn)
    df.to_csv(out_path, index=False)

    return df