# Load data into postgreSQL database
import sqlalchemy as db
import json
from pathlib import Path
from etl.stat_engine import StatEngine

# Analysed data file to test
test_file = r'C:\Users\Aaron\Google Drive\Python_Learning\etl_pipeline\etl\data\fiji_R1_analysed.csv'

# Get config info
config_path = Path.cwd().joinpath('config.json')


class LoadData:

    def __init__(self, round_dict, ENV):
    self.round_dict = round_dict

    if ENV == 'DEV':
        # Development database config
        db_config_data = json.load(open(config_path))['dev_postgres']
        conn_str = db_config_data['username'] + ':' + db_config_data['password'] + '@'\
            + 'localhost' + '/' +\
                db_config_data['db_name']
        conn_uri = f"postgresql://{conn_str}"
    else:
        # Deployment database config
        db_config_data = json.load(open(config_path))['dep_postgres']
        conn_str = db_config_data['username'] + ':' + db_config_data['password'] + '@'\
            + db_config_data['ip'] + ':' + db_config_data['port'] + '/' +\
                db_config_data['db_name']
        conn_uri = f"postgresql://{conn_str}"

    engine = db.create_engine(conn_uri)

load_df = StatEngine(test_file).statengine()
load_df.to_sql('fiji', engine, index=False, if_exists='replace')
print(f'Data was loaded to db successfully!')

