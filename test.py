from pathlib import Path
import json
import numpy as np
import pandas as pd

config_path = Path.cwd().joinpath('config.json')
config_data = json.load(open(config_path))

round_dict = config_data['round_dict']

# for k in round_dict.keys():
#     if 'Fiji' in k:
#         print(round_dict[k])

# Get Fiji svy_ids
test = {'Fiji': [round_dict[k] for k in round_dict.keys() if 'Fiji' in k]}

country_name_id_list_dict = {
    'Fiji': [round_dict[k] for k in round_dict.keys() if 'Fiji' in k],
    'Samoa': [round_dict[k] for k in round_dict.keys() if 'Samoa' in k],
    'Tonga': [round_dict[k] for k in round_dict.keys() if 'Tonga' in k]
}

print(country_name_id_list_dict)