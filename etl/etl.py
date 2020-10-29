# from pathlib import Path
# import pandas as pd
# import json
# from extract import ExtractData
# # import etl.transform as transform
# # from etl.survey_dict import survey_dict


# # Get config data
# config_path = Path.cwd().joinpath('config.json')
# config_data = json.load(open(config_path))

# # Get Kobo config
# url = config_data['kobo']['url']
# user = config_data['kobo']['user']
# password = config_data['kobo']['password']

# # Survey Round and ID dictionary
# round_dict = config_data['round_dict']

# class etlEngine:






# if __name__ == "__main__":
#     # Get raw data from API
#     extractObj = ExtractData(url, user, password, round_dict)
#     extractObj.extract()