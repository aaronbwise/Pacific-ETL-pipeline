# Load data into postgreSQL database
import sqlalchemy as db
import json
from pathlib import Path
from etl.stat_engine import StatEngine


class LoadData:

    # Get config info
    config_path = Path.cwd().joinpath('config.json')
    db_config_data = json.load(open(config_path))

    def __init__(self, round_dict, ENV):
        self.round_dict = round_dict
        self.ENV = ENV

        # Get stat engine information
        statObj = StatEngine(self.round_dict)
        self.tableau_output_dict = statObj.stat_engine()
        print(len(self.tableau_output_dict))

    def load_data(self):
        for svy_id, output in self.tableau_output_dict.items():
            self.load_engine(svy_id, output)
        return
    
    def load_engine(self, svy_id, output):

        if self.ENV == 'DEV':
            # Development database config
            self.dev_config = self.db_config_data['dev_postgres']
            self.conn_str = self.dev_config['username'] + ':' + self.dev_config['password'] + '@'\
                + 'localhost' + '/' + self.dev_config['db_name']
            self.conn_uri = f"postgresql://{self.conn_str}"
        else:
            # Deployment database config
            self.dep_config = self.db_config_data['dep_postgres']
            self.conn_str = self.dep_config['username'] + ':' + self.dep_config['password'] + '@'\
                + self.dep_config['ip'] + ':' + self.dep_config['port'] + '/' +\
                    self.dep_config['db_name']
            self.conn_uri = f"postgresql://{self.conn_str}"

        # Create engine
        self.engine = db.create_engine(self.conn_uri)

        if output is not None:
            if svy_id == '556482' or svy_id == '587333':
                output.to_sql('fiji', self.engine, index=False, if_exists='replace')
                print(f'Data for {svy_id} was loaded to db successfully!')

            elif svy_id == '600069' or svy_id == '600072':
                output.to_sql('tonga', self.engine, index=False, if_exists='replace')
                print(f'Data for {svy_id} was loaded to db successfully!')

            elif svy_id == '600087' or svy_id == '600088':
                output.to_sql('samoa', self.engine, index=False, if_exists='replace')
                print(f'Data for {svy_id} was loaded to db successfully!')
            else:
                print(f'Uh oh! Something went wrong - check tableau_output_dict')
        else:
            print(f'There is no tableau data for {svy_id}')