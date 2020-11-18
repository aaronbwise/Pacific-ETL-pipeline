import json
from pathlib import Path
from etl.extract import ExtractData
from etl.transform import TransformData
from etl.load import LoadData


class PacificEngine:

    # Get config info
    config_path = Path.cwd().joinpath('config.json')
    config_data = json.load(open(config_path))

    def __init__(self, ENV):
        self.ENV = ENV

    def execute(self):
        self.etl()
        return

    def etl(self):
        self.round_dict = self.config_data['round_dict']

        # Get raw data from API
        extractObj = ExtractData(self.round_dict)
        extractObj.extract()

        # Transform Data
        transformObj = TransformData(self.round_dict)
        transformObj.transform()

        # Load data
        loadObj = LoadData(self.round_dict, self.ENV)
        loadObj.load_data()