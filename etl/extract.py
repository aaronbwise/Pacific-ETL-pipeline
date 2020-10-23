import os
import json
import requests
from requests.auth import HTTPBasicAuth
from pandas.io.json import json_normalize 
import gc

datadir = os.path.join(os.path.split(os.path.realpath(__file__))[0], 'data')

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
        except:
            print(f'The data for {svy_id} was DID NOT SAVE!')
            
        return json_data

    else:
        print('Data was not retrieved')
        gc.collect()

def format_as_dataframe(json_data, svy_id):
    df = json_normalize(json_data)

    fn = svy_id + '_raw' + '.csv'
    path = os.path.join(datadir, fn)
    df.to_csv(path, index=False)

    return df




