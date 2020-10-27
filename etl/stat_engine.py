import os
import numpy as np
import pandas as pd
from pathlib import Path
from loading import fiji_engine

# Set directory for cleaned data
# datadir = Path.cwd().joinpath('etl', 'data')

test_file = r'C:\Users\Aaron\Google Drive\Python_Learning\etl_pipeline\etl\data\fiji_R1_analysed.csv'

class StatEngine:

    def __init__(self, path):
        self.path = path

    ## FOR LOOP OR LIST COMPREHENSION???
    def statengine(self):
        self.fiji_engine()
        return

    def fiji_engine(self):
        df = self.generate_df()
        output = fiji_engine(df)
        return output
    
    def generate_df(self):
        # Fiji_R1
        df = pd.read_csv(self.path)
        return df

statObj = StatEngine(test_file)
statObj.statengine()