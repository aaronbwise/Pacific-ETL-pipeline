import os
from pathlib import Path

datadir = Path.cwd().parents[1] / ('data')
fn = 'samoa' + '_R1' + '_cleaned' + '.csv'

outfile = datadir / fn

print(outfile)


# datadir = os.path.join(os.path.expanduser('~'), 
# print(f'The data path is: {datadir}')