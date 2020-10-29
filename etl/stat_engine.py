import pandas as pd
from etl.loading.fiji_engine import fiji_r1_engine

class StatEngine:

    def __init__(self, path):
        self.path = path

    ## FOR LOOP OR LIST COMPREHENSION???
    def statengine(self):
        df = self.fiji_engine()
        return df

    def fiji_engine(self):
        try:
            df = self.generate_df()
            output_df = fiji_r1_engine(df)
            print('Output created!')
        except:
            print('Output not created')
        return output_df
    
    def generate_df(self):
        # Fiji_R1
        try:
            df = pd.read_csv(self.path)
            print('File loaded, df created!')
        except:
            print('File did not load')
        return df