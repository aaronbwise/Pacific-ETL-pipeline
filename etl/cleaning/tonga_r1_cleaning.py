import os 
from pathlib import Path
import numpy as np
import pandas as pd
import sys

# Set directory for cleaned data
datadir = Path.cwd().joinpath('etl', 'data')

def tonga_r1_preprocess_data(df, col_mapping_dict, col_order_list):
    """Function to preprocess Tonga R1 data"""
    df = df.rename(columns = col_mapping_dict)

    # Update column order
    col_names = frozenset(df.columns.tolist())

    # Get values in col_order that also appear in col_names
    final_order = [x for x in col_order_list if x in col_names]

    df = df[final_order]

    return df

def tonga_r1_clean_data(df, svy_id):
    # Add column for completed_survey
    df['RESPAge'] = df['RESPAge'].astype('float64')
    # Recode RESPAge == 99 as NaN
    df.loc[:,'RESPAge'] = df['RESPAge'].replace({99: np.nan})
    df['completed_svy'] = np.where((df['RESPConsent'] == 'Yes') & (df['RESPAge'] >= 18), 1, 0)

    # Drop incomplete surveys
    df = df[df['completed_svy'] == 1]

    # Convert number variables to numeric format
    cols = ['RESPAge', 'HHSize', 'HHSizeM', 'HHSizeF', 'HHSizeOth', 'HHSize04F', 'HHBreastfedF',\
            'HHSize0514F', 'HHSize1524F', 'HHSize2554F', 'HHSize5564F', 'HHSize65aboveF', 'HHSize04M',\
            'HHBreastfedM', 'HHSize0514M', 'HHSize1524M', 'HHSize2554M', 'HHSize5564M', 'HHSize65aboveM',\
            'HHSize0514Oth', 'HHSize1524Oth', 'HHSize2554Oth', 'HHSize5564Oth', 'HHSize65aboveOth',\
            'HHDisabledNb', 'FCSStap', 'FCSPulse', 'FCSDairy', 'FCSPr', 'FCSPrMeat', 'FCSMeatO', 'FCSPrFish',\
            'FCSPrEggs', 'FCSVeg', 'FCSVegOrg', 'FCSVegGre', 'FCSFruit', 'FCSFruitOrg', 'FCSFat', 'FCSSugar',\
            'FCSCond', 'HHIllNb', 'Hroom', 'HHDebtPaidWhen']

    df[cols] = df[cols].astype('float64')

    # Clean values and labels
    # Impute HHHSex from RESPSex if RESP == HHH
    df.loc[:, 'HHHSex'] = np.where(df['RESPRelationHHH'] == 'Yes', df['RESPSex'], df['HHHSex'])

    df.loc[:, 'HHygieneYN'] = df['HHygieneYN'].replace({'no': 'No', 'yes': 'Yes'})
    df.loc[:, 'HWaterConstrYN'] = df['HWaterConstrYN'].replace({'no': 'No', 'yes': 'Yes'})

    df.loc[df['Hroom'] == 0, 'Hroom'] = np.nan
    
    # Write out file
    fn = svy_id + '_cleaned' + '.csv'
    out_path = datadir.joinpath(fn)
    print(f'\n Clean file being saved to: \n {out_path} \n')
    try:
        df.to_csv(out_path, index=False)
        print(f'{svy_id} data cleaned and SAVED!')
    except:
        print(f'{svy_id} data DID NOT SAVE!')

    return df