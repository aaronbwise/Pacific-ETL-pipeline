import os
from pathlib import Path
import numpy as np
import pandas as pd

# Set directory for cleaned data
datadir = Path.cwd().parent.joinpath('data')

def fiji_r1_preprocess_data(df, col_mapping_dict, col_order_list):
    """Function to preprocess Fiji R1 data"""
    df = df.rename(columns = col_mapping_dict)

    # Update column order
    col_names = frozenset(df.columns.to_list())

    # Get values in col_order that also appear in col_names
    final_order = [x for x in col_order_list if x in col_names]

    df = df[final_order]

    # Apply function to create separate columns for each response
    df['LhCSIMoreIncome_1'] = df.LhCSIMoreIncome.str.split(' ').str[0]
    df['LhCSIMoreIncome_2'] = df.LhCSIMoreIncome.str.split(' ').str[1]

    df['RESPWorryRsns_1'] = df.RESPWorryRsns.str.split(' ').str[0]
    df['RESPWorryRsns_2'] = df.RESPWorryRsns.str.split(' ').str[1]

    # Recode RESPAge == 99 as NaN
    df['RESPAge'] = df['RESPAge'].astype('float64').replace({99: np.nan})

    # Insert Round
    df.insert(0, 'Round', 'R1')

    return df

def fiji_r1_clean_data(df):
    
    # Add column for completed_survey
    df['completed_svy'] = np.where((df['RESPConsent'] == 'yes') & (df['RESPDob'] == 'yes'), 1, 0)

    # Drop incomplete surveys
    df = df[df['completed_svy'] == 1]

    # Convert number variables to numeric format
    cols = ['RESPAge', 'HHSize', 'HHSizeF', 'HHSize04F', 'HHBreastfedF', 'HHSize0414F', 'HHSize1524F', 'HHSize2554F',\
            'HHSize5564F', 'HHSize65aboveF', 'HHSizeM', 'HHSize04M', 'HHBreastfedM', 'HHSize0414M', 'HHSize1524M',\
            'HHSize2554M', 'HHSize5564M', 'HHSize65aboveM', 'HHSexOther', 'HHDisabledNb', 'FCSStap', 'FCSPulse', 'FCSDairy',\
            'FCSPr', 'FCSPrMeat', 'FCSPrMeatO', 'FCSPrFish', 'FCSPrEgg', 'FCSVeg', 'FCSVegOrg', 'FCSVegGre', 'FCSFruit', 'FCSFruitOrg',\
            'FCSFat', 'FCSSugar', 'FCSCond', 'HHChronIllNb', 'Hroom', 'HHDebtPaidWhen']
    df[cols] = df[cols].astype('float64')

    # Clean categorical variables -> values and labels
    yes_no_dict = {'yes': 'Yes', 'no': 'No'}

    df.loc[:,'RESPConsent'] = df.replace(yes_no_dict)
    df.loc[:,'RESPDob'] = df.replace(yes_no_dict)
    df.loc[:,'PrefLang'] = df.replace({'english': 'English', 'fijian': 'Fijian', 'hindi':'Hindi'})
    df.loc[:,'Rural'] = df.replace({'rural': 'Rural', 'urban': 'Urban'})
    df.loc[:,'ADM2Name'] = df.replace({'ba': 'Ba', 'naitasiri': 'Naitasiri', 'rewa': 'Rewa',\
                                            'macuata': 'Macuata', 'tailevu': 'Tailevu', 'nadroga_navosa': 'Nadroga-Navosa',\
                                            'cakaudrove': 'Cakaudrove', 'ra': 'Ra', 'serua': 'Serua', 'namosi': 'Namosi',\
                                            'bua': 'Bua', 'lomaiviti': 'Lomaiviti', 'lau': 'Lau', 'kadavu': 'Kadavu',\
                                            'rotuma': 'Rotuma'})
    df.loc[:,'RESPSex'] = df.replace({'male': 'Male', 'female': 'Female', 'other': 'Other'})
    df.loc[:,'RESPRelationHHH'] = df.replace(yes_no_dict)

    # Impute HHHSex from RESPSex if RESP == HHH
    df.loc[:,'HHHSex'] = np.where(df['RESPRelationHHH'] == 'Yes', df['RESPSex'], df['HHHSex'])
    df.loc[:,'HHHSex'] = df.replace({'male': 'Male', 'female': 'Female', 'other': 'Other'})

    df.loc[:,'HHHEduYears'] = df.replace({'male': 'No Education', 'female': 'Primary', 'secondary': 'Secondary',\
                                                'tertiary': 'Tertiary', 'vocational_tra': 'Vocational Training'})
    df.loc[:,'HHDispl'] = df.replace({'yes': 'Yes_into household', 'no': 'Yes_out of household',\
                                        'none___househo': 'No'})
    df.loc[:,'WitGenViolence'] = df.replace(yes_no_dict)
    df.loc[:,'HHIncFirst'] = df.replace({'salary_wage': 'Salary_Wage_work', 'casual_labor': 'Daily_casual_labor',\
                                                'own_business_t': 'Own_business_trade', 'petty_trade_sm': 'Petty_trade_small_business',\
                                                'farming': 'Farming', 'fishing__aquac':'Fishing_aquaculture',\
                                                'livestock_prod':'Livestock_production', 'remittances_fr':'Remittances_from_abroad',\
                                                'support_from_f':'Support_from_family_friends', 'government_ass':'Government_asst_safety_nets',\
                                                'assistance_fro': 'Assistance_from_UN_NGO_charity', 'other_activiti': 'Other'})
    df.loc[:,'HHIncChg'] = df.replace({'red_income': 'Reduced', 'no_change': 'No change', 'no_income':'No income', 'inc_income': 'Increased'})
    df.loc[:,'ShEMP'] = df.replace({'male': 'Could not work due to movement restrictions', 'female': 'Disruptions in markets (not able to sell products or buy inputs)',\
                                    'own_business_t': 'Less customers/clients', 'had_to_close_shop_business': 'Had to close shop/business',\
                                    'household_members_working_are_': 'Household members working are sick or in quarantine', 'loss_of_employment': 'Loss of employment',\
                                    'reduced_salary_wage': 'Reduced salary/wage', 'daily_labor_opportunities_redu': 'Daily labor opportunities reduced',\
                                    'support_assistance_has_been_re': 'Support/assistance has been reduced', 'loss_of_assets__e_g_equipment_': 'Loss of assets (e.g equipment, livestock) due to a natural hazard',\
                                    'other': 'Other'})
    df.loc[:,'HHNoIncWhy'] = df.replace({'male': 'Could not work due to movement restrictions', 'female': 'Disruptions in markets (not able to sell products or buy inputs)',\
                                    'own_business_t': 'Less customers/clients', 'had_to_close_shop_business': 'Had to close shop/business',\
                                    'household_members_working_are_': 'Household members working are sick or in quarantine', 'loss_of_employment': 'Loss of employment',\
                                    'reduced_salary_wage': 'Reduced salary/wage', 'daily_labor_opportunities_redu': 'Daily labor opportunities reduced',\
                                    'support_assistance_has_been_re': 'Support/assistance has been reduced', 'loss_of_assets__e_g_equipment_': 'Loss of assets (e.g equipment, livestock) due to a natural hazard',\
                                    'other': 'Other'})
    df.loc[:,'HHFarm'] = df.replace(yes_no_dict)
    df.loc[:,'HHFarmInputChall'] = df.replace({'inc_access': 'Increased access',\
                                                            'reduced_access': 'Reduced access',\
                                                            'no_change': 'No change', 'no_access': 'No access'})

    df.loc[:,'HHFarmSellChall'] = df.replace({'male': 'Increased sales', 'female': 'Reduced sales', 'no_change': 'No change'})
    df.loc[:,'LhCSIMoreIncome_1'] = df.replace({'no':'No', 'sold__traded_h':'Sold /traded household assets/goods (furniture, refrigerator, television)',\
                                                            'reduced_food_w':'Reduced food wastage', 'sold_smaller_h':'Sold smaller household/personal assets (jewelry, decoration items, antique items, other valuable possessions)',\
                                                            'reduced_non_fo':'Reduced non-food expenses on health (including drugs)', 'reduced_non_fo_1':'Reduced non-food expenses on education',\
                                                            'sold_productiv':'Sold productive assets or means of transport (e.g. boat, car, bullock cart)',\
                                                            'trading_a_serv':'Trading a service in exchange for groceries, food and necessities',\
                                                            'spent_savings':'Spent savings', 'sold_house_or_':'Sold house or land',\
                                                            'withdrew_child':'Withdrew children from school', 'sold_traded_la':'Sold/traded last female animals',\
                                                            'traded_animals':'Traded animals/vegetables/marine catch', 'pleaded_for_money__food__or_ot': 'Pleaded for money, food, or other resources',\
                                                            'sold_more_anim': 'Sold more animals (non-productive) than usual', 'other_activiti': 'Other Activities'})

    df.loc[:,'LhCSIMoreIncome_2'] = df.replace({'no':'No', 'sold__traded_h':'Sold /traded household assets/goods (furniture, refrigerator, television)',\
                                                            'reduced_food_w':'Reduced food wastage', 'sold_smaller_h':'Sold smaller household/personal assets (jewelry, decoration items, antique items, other valuable possessions)',\
                                                            'reduced_non_fo':'Reduced non-food expenses on health (including drugs)', 'reduced_non_fo_1':'Reduced non-food expenses on education',\
                                                            'sold_productiv':'Sold productive assets or means of transport (e.g. boat, car, bullock cart)',\
                                                            'trading_a_serv':'Trading a service in exchange for groceries, food and necessities',\
                                                            'spent_savings':'Spent savings', 'sold_house_or_':'Sold house or land',\
                                                            'withdrew_child':'Withdrew children from school', 'sold_traded_la':'Sold/traded last female animals',\
                                                            'traded_animals':'Traded animals/vegetables/marine catch', 'pleaded_for_money__food__or_ot': 'Pleaded for money, food, or other resources',\
                                                            'sold_more_anim': 'Sold more animals (non-productive) than usual', 'other_activiti': 'Other Activities'})

    df.loc[:,'LhCSINo'] = df.replace({'not_necessary': 'Not necessary', 'sold_assets': 'Already exhausted this activity', 'dont_know': 'Don\'t know'})
    df.loc[:,'HHSchoolSituation'] = df.replace({'no_children': 'No children 5-17 in household', 'children_going': 'Physically attending',\
                                                            'children_paren': 'Learning remotely_parent resources', 'don_t_know': 'Learning remotely_school resources',\
                                                            'children_are_n': 'No learning activities during the day', 'other': 'Other'})
    df.loc[:,'HHChronIll'] = df.replace(yes_no_dict)
    df.loc[:,'HHENHealthMed'] = df.replace({'yes': 'Yes', 'no': 'No', 'medicre_not_ne': 'Medical care not needed'})
    df.loc[:,'HHENHealthMedPrb'] = df.replace({'hlth_is_far': 'Hospital\health center is far',\
                                                            'hlth_is_clsd': 'Hospitals\Health centers closed',\
                                                            'no_visit': 'Medical personnel didn\’t visit our home',\
                                                            'lack_of_money': 'Lack of money', 'travel_restric': 'Travel restrictions',\
                                                            'all_members_of': 'Too sick to travel', 'denied_access': 'Denied access_reached capacity',\
                                                            'other': 'Other'})

    df.loc[:,'HHStockFoodDur'] = df.replace({'yes__less_than': 'Yes, 2 days', 'yes__two_weeks': 'Yes, < 1 week', 'yes__more_than': 'Yes, > 1 week', 'none': 'None'})
    df.loc[:,'Food_SRf1Prev'] = df.replace({'yes__less_than': 'Own production',\
                                                    'yes__two_weeks': 'Market_grocery', 'yes__more_than': 'Exchange labor',\
                                                    'no': 'Gift from family_friends', 'food_assistance_by_humanitaria': 'Food assistance_humanitarian',\
                                                    'food_assistance_by_religious_o': 'Food assistance_religious',\
                                                    'food_assistance_by_government': 'Food assistance_Govt', 'other': 'Other'})

    df.loc[:,'Food_SRf1'] = df.replace({'own_produce': 'Own production',\
                                                    'mrkt_store': 'Market_grocery', 'exchange': 'Exchange labor',\
                                                    'gifts': 'Gift from family_friends', 'food_assistanc': 'Food assistance_humanitarian',\
                                                    'food_assistanc_1': 'Food assistance_religious',\
                                                    'food_assistance_by_government': 'Food assistance_Govt', 'other': 'Other'})

    df.loc[:,'HHhsBedHung_YN'] = df.replace(yes_no_dict)
    df.loc[:,'FoodConstr'] = df.replace({'shortage_of_fo_1': 'Shortage of food in market_grocery',\
                                                'increase_price': 'Increased food prices',\
                                                'no_money': 'No money to buy food', 'no_food_house': 'No food in house',\
                                                'no_access': 'Cannot access the market_grocery', 'markets_groc': 'Markets_grocery closed',\
                                                'no_access_gard': 'No access to food gardens', 'other': 'Other'})
    df.loc[:,'HWaterConstr'] = df.replace({'yes': 'Yes', 'no': 'No', 'dont_know': 'Don\'t know'})
    df.loc[:,'HDwellCond'] = df.replace({'own_house': 'Own', 'rent': 'Rent', 'free': 'Do not own but live for free',\
                                                'other': 'Other'})

    df.loc[:,'RESPWorryRsns_1'] = df.replace({'disruption_to_': 'Disruption to livelihood',\
                                                        'shortage_of_fo': 'Shortage of food', 'increase_in_fo': 'Increase in food prices',\
                                                        'shortage_of_me': 'Shortage of medicine', 'disruption_of_': 'Disruption of medical service',\
                                                        'disruption_of__1': 'Disruption of educational institutes', 'getting_sick': 'Getting sick',\
                                                        'lack_of_work': 'Lack of work', 'travel_restric': 'Travel restrictions', 'no_concerns': 'No concerns',\
                                                        'other': 'Other'})
    df.loc[:,'RESPWorryRsns_2'] = df.replace({'disruption_to_': 'Disruption to livelihood',\
                                                        'shortage_of_fo': 'Shortage of food', 'increase_in_fo': 'Increase in food prices',\
                                                        'shortage_of_me': 'Shortage of medicine', 'disruption_of_': 'Disruption of medical service',\
                                                        'disruption_of__1': 'Disruption of educational institutes', 'getting_sick': 'Getting sick',\
                                                        'lack_of_work': 'Lack of work', 'travel_restric': 'Travel restrictions', 'no_concerns': 'No concerns',\
                                                        'other': 'Other'})
    df.loc[:,'HHRemittPrev_YN'] = df.replace(yes_no_dict)
    df.loc[:,'HHRemitt_YN'] = df.replace(yes_no_dict)
    df.loc[:,'HHRemittWho'] = df.replace({'spouse': 'Spouse', 'son_daughter': 'Son_daughter',\
                                                'son_daughter_i': 'Son_daughter in law', 'grandchild': 'Grandchild',\
                                                'parent': 'Parent', 'parent_in_law': 'Parent in law',\
                                                'brother_sister': 'Brother/sister', 'other_related_': 'Other related (including in laws)',\
                                                'other_unrelate': 'Other unrelated'})
    df.loc[:,'HHRemittCh'] = df.replace({'increased': 'Increased', 'decreased': 'Decreased', 'no_change': 'No change', 'dont_know': 'Don\'t know'})
    df.loc[:,'HHBorrow'] = df.replace(yes_no_dict)
    df.loc[:,'HHBorrowWhy'] = df.replace({'food': 'Food', 'health_exp': 'Healthcare',\
                                                'expenditure_for_agricultural_o': 'Agricultural_production inputs',\
                                                'expenditure_for_education': 'Education', 'for_migration': 'Migration',\
                                                'transport_costs': 'Transport', 'clothing': 'Clothing',\
                                                'ceremonies__including_weddings': 'Ceremonies', 'financing_a_business': 'Financing a business',\
                                                'construction_repair_expense': 'Construction_repair expense', 'other': 'Other'})
    
    # Remove Hroom == 0
    df.loc[df['Hroom'] == 0, 'Hroom'] = np.nan
    
    # Write out file
    fn = 'fiji' + '_R1' + '_cleaned' + '.csv'
    out_path = datadir / fn
    try:
        df.to_csv(out_path, index=False)
        print('Fiji R1 data cleaned and SAVED!')
    except:
        print('Fiji R1 data DID NOT SAVE!')

    return df

