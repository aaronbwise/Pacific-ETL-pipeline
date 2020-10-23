import pandas as pd

def make_dict(path):
    d = pd.read_csv(path, header=0, index_col=0, squeeze=True).to_dict()
    return d
