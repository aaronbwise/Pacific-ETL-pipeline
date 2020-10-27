import os
from pathlib import Path
import json
import requests
from requests.auth import HTTPBasicAuth
from pandas.io.json import json_normalize 
import gc

# Set data directory
datadir = Path.cwd().joinpath('etl', 'data')

def fetch_data(url, user, password, svy_id):
    target = url + svy_id
    result = requests.get(target, auth=HTTPBasicAuth(user, password))

    if result.status_code == 200:
        print(f'The data for {svy_id} was retrieved successfully!')

        # Get the data
        json_data = result.json()

        # Create filename with timestamp
        fn = svy_id + '.json'  
        tot_name = os.path.join(datadir, fn)
        
        # Dump data
        try: 
            with open(tot_name, 'w') as output_file:
                json.dump(json_data, output_file)
            print(f'The raw JSON data for {svy_id} was SAVED!')
        except:
            print(f'The raw JSON data for {svy_id} DID NOT SAVE!')
            
        return json_data

    else:
        print('Data was not retrieved')
        gc.collect()

def format_as_dataframe(json_data, svy_id):
    df = json_normalize(json_data)

    fn = svy_id + '_raw' + '.csv'
    path = os.path.join(datadir, fn)
    try:
        df.to_csv(path, index=False)
        print(f'The raw CSV data for {svy_id} was SAVED!')
    except:
        print(f'The raw CSV data for {svy_id} DID NOT SAVE!')

    return df




