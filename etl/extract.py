from pathlib import Path
import json
import requests
from requests.auth import HTTPBasicAuth
import pandas as pd
import gc


class ExtractData:
    # Set data directory
    datadir = Path.cwd().joinpath('etl', 'data')

    def __init__(self, url, user, password, round_dict):
        self.url = url
        self.user = user
        self.password = password
        self.round_dict = round_dict

    def extract(self):
        self.get_raw_data()
        return

    # Extract data from API for all surveys
    def get_raw_data(self):
        [self.format_as_dataframe(svy_id) for svy_id in self.round_dict.values()]
        return

    def format_as_dataframe(self, svy_id):
        json_data = self.fetch_data(svy_id)
        df = pd.json_normalize(json_data)

        fn = svy_id + '.csv'
        path = self.datadir.joinpath(fn)

        try:
            df.to_csv(path, index=False)
            print(f'The raw CSV data for {svy_id} was SAVED!')
        except:
            print(f'The raw CSV data for {svy_id} DID NOT SAVE!')

        return

    def fetch_data(self, svy_id):
        
        target = self.url + svy_id
        result = requests.get(target, auth=HTTPBasicAuth(self.user, self.password))

        if result.status_code == 200:
            print(f'The data for {svy_id} was retrieved successfully!')

            # Get the data
            json_data = result.json()

            # Create filename
            fn = svy_id + '.json'  
            tot_name = self.datadir.joinpath(fn)
            
            # Dump data
            try: 
                with open(tot_name, 'w') as output_file:
                    json.dump(json_data, output_file)
                print(f'The raw JSON data for {svy_id} was SAVED!')
            except:
                print(f'The raw JSON data for {svy_id} DID NOT SAVE!')
                
            return json_data

        else:
            print(f'Data was not retrieved: {result.status_code}')
            gc.collect()



