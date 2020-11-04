import pandas as pd
from pathlib import Path
import etl.loading.fiji_engine as fe
import etl.loading.samoa_engine as se

class StatEngine:

    # Set data directory
    datadir = Path.cwd().joinpath('etl', 'data')

    def __init__(self, round_dict):
        self.round_dict = round_dict

    def stat_engine(self):
        tableau_output_dict = self.generate_tableau_dict()
        return tableau_output_dict
        
    def generate_tableau_dict(self):
        tableau_output_dict = {}
        for svy_id in self.round_dict.values():
            tableau_output_dict[svy_id] = self.generate_output_tableau(svy_id)
        return tableau_output_dict

    def generate_output_tableau(self, svy_id):
        """
        Generate dictionary with svy_id and output for Tableau (ie long form)
        """
        df = self.generate_df(svy_id)

        if svy_id == '556482':
            output = fe.fiji_r1_engine(df)
        elif svy_id == '600087':
            output = se.samoa_r1_engine(df)
        else:
            output = None
        
        return output

    def generate_df(self, svy_id):
        fn = svy_id + '_analysed' + '.csv'
        path = self.datadir.joinpath(fn)

        if path.is_file():
            df = pd.read_csv(path)
        else:
            df = None
        return df