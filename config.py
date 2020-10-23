import os
from dotenv import load_dotenv

URL = "https://kc.humanitarianresponse.info/api/v1/data/"

load_dotenv()
USER = os.getenv('user')
PASSWORD = os.getenv('password')
SVY_ID = os.getenv('svy_id')

DATA_DIR = os.path.join(os.getcwd(), 'data')
MAPPING_DIR = os.path.join(os.getcwd(), 'variable_mapping')

SVY_ID_DICT = {'FIJI_R1': '556482', 'FIJI_R2+': '587333', 'TONGA_R1':'600069',
'TONGA_R2+':'600072', 'SAMOA_R1':'600087', 'SAMOA_R2+':'600088'}