import os
import pandas as pd
import json
import etl.extract as extract
import etl.transform as transform
from etl.survey_dict import survey_dict


# Get config info
config_data = json.load(open('config.json'))

# Get API username and password
url = config_data['kobo']['url']
user = config_data['kobo']['user']
password = config_data['kobo']['password']

# Survey Round and ID dictionary
round_dict = config_data['round_dict']
fiji_r1_id = round_dict['Fiji_R1']


if __name__ == "__main__":
    extract.format_as_dataframe(extract.fetch_data(url, user, password, fiji_r1_id), fiji_r1_id)

    obj = transform.TransformData(survey_dict)
    obj.transform()
