import os
import pandas as pd
import json
from dotenv import load_dotenv
import etl.extract as extract
import etl.transform as transform
import etl.cleaning.variable_mapping as vm


# Get config info
config_data = json.load(open('config.json'))

# Get API username and password
load_dotenv()
user = os.getenv('user')
password = os.getenv('password')

url = config_data['kobo']['url']

# Survey Round and ID dictionary
svy_dict = config_data['svy_dict']

fiji_svy = config_data['svy_dict']['Fiji_R1']



if __name__ == "__main__":
    extract.format_as_dataframe(extract.fetch_data(url, user, password, fiji_svy), fiji_svy)

    # samoa_r1 = transform.CleanData(df_dict, vm.samoa_r1_mapping_dict, vm.samoa_r1_order_list)
    # samoa_r1.clean_samoa_data()