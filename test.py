from pathlib import Path
import json
import numpy as np
import pandas as pd

import sqlalchemy as db


# Get config data
config_data = json.load(open('config.json'))

fiji_output = pd.read_csv('fiji.csv')
fiji_output = fiji_output.iloc[:, 1:]

# Deployment database config
dep_config = config_data['dep_postgres']
conn_str = dep_config['username'] + ':' + dep_config['password'] + '@'\
    + dep_config['ip'] + ':' + dep_config['port'] + '/' +\
        dep_config['db_name']
conn_uri = f"postgresql+psycopg2://{conn_str}"


# # Create engine
engine = db.create_engine(conn_uri, echo=True)
# engine.connect()

print(conn_uri)

fiji_output.to_sql('fiji', con=engine, index=False, if_exists='replace', method='multi')