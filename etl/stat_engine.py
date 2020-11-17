import pandas as pd
from pathlib import Path
from etl.engines.fiji_engine import FijiEngine
from etl.engines.samoa_engine import SamoaEngine
# from etl.engines.tonga_engine import TongaEngine


class StatEngine:

    # Set data directory
    datadir = Path.cwd().joinpath('etl', 'data')

    def __init__(self, round_dict):
        self.round_dict = round_dict

        self.country_name_id_list_dict = {
            'Fiji': [self.round_dict[k] for k in self.round_dict.keys() if 'Fiji' in k],
            'Samoa': [self.round_dict[k] for k in self.round_dict.keys() if 'Samoa' in k],
            'Tonga': [self.round_dict[k] for k in self.round_dict.keys() if 'Tonga' in k]
        }

    def stat_engine(self):
        tableau_output_dict = self.generate_tableau_dict()
        return tableau_output_dict
        
    def generate_tableau_dict(self):
        tableau_output_dict = {}
        for country_name in self.country_name_id_list_dict.keys():
            tableau_output_dict[country_name] = self.generate_output_tableau(country_name)
        return tableau_output_dict

    def generate_output_tableau(self, country_name):
        if country_name == 'Fiji':
            obj = FijiEngine(self.country_name_id_list_dict[country_name])
            output_tableau = obj.run_fiji_engine()
            return output_tableau
        elif country_name == 'Samoa':
            obj = SamoaEngine(self.country_name_id_list_dict[country_name])
            output_tableau = obj.run_samoa_engine()
            return output_tableau
        # elif country_name == 'Tonga':
        #     obj = TongaEngine(self.country_name_id_list_dict[country_name])
        #     output_tableau = obj.run_tonga_engine()
        #     return output_tableau
        else:
            return None
