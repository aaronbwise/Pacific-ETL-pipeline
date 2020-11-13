import os
from pathlib import Path
import numpy as np
import pandas as pd

# Set directory for cleaned data
datadir = Path.cwd().joinpath('etl', 'data')

def fiji_r2_preprocess_data(df, col_mapping_dict, col_order_list):
    """Function to preprocess Fiji_R2 data"""
    df = df.rename(columns = col_mapping_dict)

    # Update column order
    col_names = frozenset(df.columns.to_list())

    # Get values in col_order that also appear in col_names
    final_order = [x for x in col_order_list if x in col_names]

    df = df[final_order]

    # # Apply function to create separate columns for each response
    df['HHDisabledType_1'] = df.HHDisabledType.str.split(' ').str[0]
    df['HHDisabledType_2'] = df.HHDisabledType.str.split(' ').str[1]

    # # Insert Round
    # df.insert(0, 'Round', 'R2')

    return df

def fiji_r2_clean_data(df, svy_id):
    
    # Add column for completed_survey
    df['RESPAge'] = df['RESPAge'].astype('float64')
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
    df['HHHSex'] = np.where(df['RESPRelationHHH'] == 'Yes', df['RESPSex'], df['HHHSex'])

    df['HHygieneYN'] = df['HHygieneYN'].replace({'no': 'No', 'yes': 'Yes'})
    df['HWaterConstrYN'] = df['HWaterConstrYN'].replace({'no': 'No', 'yes': 'Yes'})

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