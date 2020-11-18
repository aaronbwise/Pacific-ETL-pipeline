import sqlalchemy as db
import json
from pathlib import Path
from etl.stat_engine import StatEngine


class LoadData:

    # Get config info
    config_path = Path.cwd().joinpath('config.json')
    config_data = json.load(open(config_path))

    def __init__(self, round_dict, ENV):
        self.round_dict = round_dict
        self.ENV = ENV

        # Get stat engine information
        statObj = StatEngine(self.round_dict)
        self.tableau_output_dict = statObj.stat_engine()

    def load_data(self):
        for country_name, output in self.tableau_output_dict.items():
            self.load_engine(country_name, output)
        return
    
    def load_engine(self, country_name, output):

        if self.ENV == 'DEV':
            # Development database config
            self.dev_config = self.config_data['dev_postgres']
            self.conn_str = self.dev_config['username'] + ':' + self.dev_config['password'] + '@'\
                + 'localhost' + '/' + self.dev_config['db_name']
            self.conn_uri = f"postgresql://{self.conn_str}"
        else:
            # Deployment database config
            self.dep_config = self.config_data['dep_postgres']
            self.conn_str = self.dep_config['username'] + ':' + self.dep_config['password'] + '@'\
                + self.dep_config['ip'] + ':' + self.dep_config['port'] + '/' +\
                    self.dep_config['db_name']
            self.conn_uri = f"postgresql://{self.conn_str}"

        # Create engine
        self.engine = db.create_engine(self.conn_uri)

        if output is not None:
            if country_name == 'Fiji':
                output.to_sql('fiji', self.engine, index=False, if_exists='replace', method='multi')
                print(f'Data for {country_name} was loaded to db successfully!')

            elif country_name == 'Tonga':
                output.to_sql('tonga', self.engine, index=False, if_exists='replace', method='multi')
                print(f'Data for {country_name} was loaded to db successfully!')

            elif country_name == 'Samoa':
                output.to_sql('samoa', self.engine, index=False, if_exists='replace', method='multi')
                print(f'Data for {country_name} was loaded to db successfully!')
            else:
                print(f'Uh oh! Something went wrong - check tableau_output_dict')
        else:
            print(f'There is no tableau data for {country_name}')