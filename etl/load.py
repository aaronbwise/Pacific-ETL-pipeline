# Load data into postgreSQL database
import sqlalchemy as db
from models import Base, LoadFiji
# from sqlalchemy.orm import sessionmaker
from stat_engine import StatEngine


# Value to switch between development and production
ENV = 'dev'

if ENV == 'dev':
    # development database
    DATABASE_URI = 'postgresql://postgres:P@ssw1rd@localhost/mvam'
else:
    # deployment database
    DATABASE_URI = 'postgresql://mvam:VAMdb@2020@10.99.87.10:5432/pacific_mvam'

engine = db.create_engine(DATABASE_URI)

def recreate_database():
    Base.metadata.drop_all(engine)
    Base.metadata.create_all(engine)

recreate_database()
test_file = r'C:\Users\Aaron\Google Drive\Python_Learning\etl_pipeline\etl\data\fiji_R1_analysed.csv'

load_df = StatEngine(test_file).statengine()
load_df.to_sql('fiji', engine, index=False, if_exists='replace')

