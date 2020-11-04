import pandas as pd
from pathlib import Path
import etl.loading.fiji_engine as fe

class StatEngine:

    # Set data directory
    datadir = Path.cwd().joinpath('etl', 'data')

    def __init__(self, round_dict):
        self.round_dict = round_dict

    def stat_engine(self):
        tableau_output_dict = self.generate_output_tableau()
        return tableau_output_dict

    def generate_output_tableau(self):
        """
        Generate dictionary with svy_id and output for Tableau (ie long form)
        """
        tableau_output_dict = {}
        for svy_id in self.round_dict.values():
            
            df = self.generate_df(svy_id)

            # Need to add column/way to determine if data is analysed

            if svy_id == '556482':
                output = fe.fiji_r1_engine(df)
                tableau_output_dict[svy_id] = output
            else:
                output = None
                tableau_output_dict[svy_id] = output
            
            return tableau_output_dict

    def generate_df(self, svy_id):
        fn = svy_id + '_analysed' + '.csv'
        path = self.datadir.joinpath(fn)

        if path.is_file():
            df = pd.read_csv(path)
        else:
            df = None
        return df