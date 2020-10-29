import os
import pandas as pd
import json
from etl.extract import ExtractData
from etl.transform import TransformData
from etl.load import LoadData

# Get config data
config_data = json.load(open('config.json'))

# Get Kobo config
url = config_data['kobo']['url']
user = config_data['kobo']['user']
password = config_data['kobo']['password']

# Survey Round and ID dictionary
round_dict = config_data['round_dict']

# Value to switch between development and production databases
ENV = 'DEV'



if __name__ == "__main__":
    # # Get raw data from API
    # extractObj = ExtractData(url, user, password, round_dict)
    # extractObj.extract()
    
    # Transform Data
    transformObj = TransformData(round_dict)
    transformObj.transform()

    # Load data
    loadObj = LoadData(round_dict, ENV)
    loadObj.load()